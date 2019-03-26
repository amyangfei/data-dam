package central

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/smallnest/weighted"
	"golang.org/x/time/rate"

	md "github.com/amyangfei/data-dam/db/mysql"
	"github.com/amyangfei/data-dam/pkg/models"
)

// Generator is a database operation generator
type Generator struct {
	rate       int
	db         models.DB
	weight     weighted.W
	dispatcher *models.JobDispatcher
}

// NewGenerator returns a new Generator
func NewGenerator(cfg *Config, dispatcher *models.JobDispatcher) (*Generator, error) {

	// TODO: support MySQL only now, add more database support in the future
	db, err := md.Create(cfg.DBConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	weight := &weighted.SW{}
	for idx := range cfg.OpWeight {
		weight.Add(models.RealOpType[idx], cfg.OpWeight[idx])
	}

	gen := &Generator{
		rate:       cfg.Rate,
		db:         db,
		weight:     weight,
		dispatcher: dispatcher,
	}
	return gen, nil
}

// Run starts generator's main loop
func (g *Generator) Run(ctx context.Context) error {
	var (
		err error
		rl  = rate.NewLimiter(rate.Limit(g.rate), 10)
	)
	for {
		err = rl.Wait(ctx)
		if err != nil {
			if err == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		}
		params, err := g.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		g.dispatcher.AddDML(params)
	}
}

// Next generates next database operation (DML only, DDL support is wip).
// This function is not goroutine-safe.
// You MUST use the snchronization primitive to protect it in concurrent cases.
func (g *Generator) Next(ctx context.Context) (*models.DMLParams, error) {
	val := g.weight.Next()
	opType, ok := val.(models.OpType)
	if !ok {
		return nil, errors.Errorf("get invalid optype: %v from weighted generator", val)
	}
	params, err := g.db.GenerateDML(ctx, opType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return params, nil
}
