package central

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"

	"github.com/amyangfei/data-dam/pkg/log"
	"github.com/amyangfei/data-dam/pkg/models"
)

// RunError collects errors from sub goroutine
type RunError struct {
	source string
	err    error
}

// Controller is data dam central controller
type Controller struct {
	ctx    context.Context
	cancel context.CancelFunc
	sync.Mutex

	cfg *Config

	closed       sync2.AtomicBool
	runErrorChan chan *RunError
}

// NewController returns a new central controller for data flow
func NewController(cfg *Config) *Controller {
	c := &Controller{
		cfg:          cfg,
		runErrorChan: make(chan *RunError, 2),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

// Start starts data dam controller
func (c *Controller) Start() error {
	c.closed.Set(false)

	// if c.cfg.Seconds = 0, runs forever until context is Done
	if c.cfg.Seconds > 0 {
		go func() {
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(time.Duration(c.cfg.Seconds) * time.Second):
				log.Infof("controller exceeds duration: %s", c.cfg.Duration)
				c.cancel()
				return
			}
		}()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err, ok := <-c.runErrorChan
			if !ok {
				return
			}
			log.Errorf("RunError source: %s error: %s", err.source, errors.ErrorStack(err.err))
			c.cancel()
		}
	}()

	dispatcher := models.NewJobDispatcher(c.ctx)

	wg.Add(1)
	go func() {
		defer wg.Done()
		generator, err := NewGenerator(c.cfg, dispatcher)
		if err != nil {
			c.runErrorChan <- &RunError{"create generator", errors.Trace(err)}
			return
		}
		err = generator.Run(c.ctx)
		if err != nil {
			c.runErrorChan <- &RunError{"generator run", errors.Trace(err)}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
	}()

	close(c.runErrorChan)
	wg.Wait()

	return nil
}

// Close closes the controller
func (c *Controller) Close() {
	c.Lock()
	defer c.Unlock()
	if c.closed.Get() {
		return
	}
	c.cancel()
	c.closed.Set(true)
}
