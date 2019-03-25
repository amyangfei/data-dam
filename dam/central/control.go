package central

import (
	"sync"

	"github.com/siddontang/go/sync2"
)

// Controller is data dam central controller
type Controller struct {
	sync.Mutex

	cfg *Config

	closed sync2.AtomicBool
}

// NewController returns a new central controller for data flow
func NewController(cfg *Config) *Controller {
	ctrl := &Controller{
		cfg: cfg,
	}
	return ctrl
}

// Start starts data controller
func (c *Controller) Start() error {
	c.closed.Set(false)
	return nil
}

// Close closes the controller
func (c *Controller) Close() {
	c.Lock()
	defer c.Unlock()
	if c.closed.Get() {
		return
	}
	c.closed.Set(true)
}
