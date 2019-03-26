package central

import (
	"context"
	"sync"
	"time"

	"github.com/siddontang/go/sync2"

	"github.com/amyangfei/data-dam/pkg/log"
)

// Controller is data dam central controller
type Controller struct {
	ctx    context.Context
	cancel context.CancelFunc
	sync.Mutex

	cfg *Config

	closed sync2.AtomicBool
}

// NewController returns a new central controller for data flow
func NewController(cfg *Config) *Controller {
	c := &Controller{
		cfg: cfg,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

// Start starts data controller
func (c *Controller) Start() error {
	c.closed.Set(false)

	log.Infof("controller config: %s", c.cfg)

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

OUTFOR:
	for {
		select {
		case <-c.ctx.Done():
			break OUTFOR
		}
	}
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
