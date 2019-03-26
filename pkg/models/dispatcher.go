package models

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"

	"github.com/amyangfei/data-dam/pkg/log"
	"github.com/amyangfei/data-dam/pkg/utils"
)

var (
	flushInterval = 1 * time.Minute

	// DefaultOpWeiht is default weight for SQL operations
	DefaultOpWeiht = []int{5, 4, 1, 0}
)

// OpType is database operation type
type OpType byte

const (
	// Insert stmt
	Insert OpType = iota

	// Update stmt
	Update

	// Delete stmt
	Delete

	// Ddl stmt
	Ddl

	// Flush is internal command
	Flush
)

// RealOpType excludes internal command type
var RealOpType = []OpType{
	Insert,
	Update,
	Delete,
	Ddl,
}

type sqlJob struct {
	tp     OpType
	schema string
	table  string
	key    string
	keys   map[string]interface{}
	values map[string]interface{}
	ddl    string
}

// JobDispatcher manages and dispatches statements to databases
type JobDispatcher struct {
	sync.Mutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	DBs         []DB
	BatchSize   int
	WorkerCount int

	jobs         []chan *sqlJob
	jobsChanLock sync.Mutex
	jobsClosed   sync2.AtomicBool

	jobWg sync.WaitGroup
}

// NewJobDispatcher returns a new JobDispatcher
func NewJobDispatcher(ctx context.Context) *JobDispatcher {
	d := &JobDispatcher{}
	d.jobsClosed.Set(true)
	d.ctx, d.cancel = context.WithCancel(ctx)
	d.createJobChans()
	return d
}

func (d *JobDispatcher) closeJobChans() {
	d.jobsChanLock.Lock()
	defer d.jobsChanLock.Unlock()
	if d.jobsClosed.Get() {
		return
	}
	for _, ch := range d.jobs {
		close(ch)
	}
	d.jobsClosed.Set(true)
}

func (d *JobDispatcher) createJobChans() {
	d.closeJobChans()
	d.jobs = make([]chan *sqlJob, 0, d.WorkerCount+1)
	for i := 0; i < d.WorkerCount+1; i++ {
		d.jobs = append(d.jobs, make(chan *sqlJob, d.BatchSize+1))
	}
	d.jobsClosed.Set(false)
}

// AddDML adds a DML job from DMLParams
func (d *JobDispatcher) AddDML(dml *DMLParams) {
	job := &sqlJob{
		tp:     dml.Type,
		schema: dml.Schema,
		table:  dml.Table,
		keys:   dml.Keys,
		values: dml.Values,
	}
	d.addJob(job)
}

func (d *JobDispatcher) addJob(job *sqlJob) {
	switch job.tp {
	case Flush:
		d.jobWg.Add(d.WorkerCount)
		for i := 0; i < d.WorkerCount; i++ {
			d.jobs[i] <- job
		}
		d.jobWg.Wait()
	case Ddl:
		d.jobWg.Wait()
		d.jobWg.Add(1)
		d.jobs[d.WorkerCount] <- job
	case Insert, Update, Delete:
		d.jobWg.Add(1)
		bucket := int(utils.GenHashKey(job.key)) % d.WorkerCount
		d.jobs[bucket] <- job
	}

	if job.tp == Ddl {
		d.jobWg.Wait()
	}
}

// Start starts dispatcher main loop
func (d *JobDispatcher) Start() {
	for i := 0; i < d.WorkerCount+1; i++ {
		d.wg.Add(1)
		go func(idx int) {
			ctx2, cancel := context.WithCancel(d.ctx)
			d.dispatch(ctx2, d.DBs[idx], d.jobs[idx])
			cancel()
		}(i)
	}
}

func (d *JobDispatcher) processJobs(ctx context.Context, db DB, jobs []*sqlJob) error {
	if len(jobs) == 0 {
		return nil
	}

	var err error
	for _, job := range jobs {
		switch job.tp {
		case Insert:
			err = db.Insert(ctx, job.schema, job.table, job.values)
		case Update:
			db.Update(ctx, job.schema, job.table, job.keys, job.values)
		case Delete:
			db.Delete(ctx, job.schema, job.table, job.keys)
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (d *JobDispatcher) dispatch(ctx context.Context, db DB, jobChan <-chan *sqlJob) {
	defer d.wg.Done()

	var (
		count = d.BatchSize
		jobs  = make([]*sqlJob, 0, count)
	)

	clearJobs := func(err error) {
		if err != nil {
			log.Errorf("process jobs error: %v", errors.ErrorStack(err))
		}
		jobs = jobs[:0]
	}

	var err error
	for {
		select {
		case <-ctx.Done():
			err = d.processJobs(ctx, db, jobs)
			clearJobs(err)
			return
		case <-time.After(flushInterval):
			err = d.processJobs(ctx, db, jobs)
			clearJobs(err)
		case job, ok := <-jobChan:
			if !ok {
				return
			}
			if job.tp != Flush {
				jobs = append(jobs, job)
			}
			if len(jobs) >= count || job.tp == Flush {
				err = d.processJobs(ctx, db, jobs)
				clearJobs(err)
			}
		}
	}
}
