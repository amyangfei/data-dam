package central

// Controller is data dam central controller
type Controller struct {
	cfg *Config
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
	return nil
}

// Close closes the controller
func (c *Controller) Close() {
}
