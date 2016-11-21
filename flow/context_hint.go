package flow

type FlowContextOption func(c *FlowContextConfig)

type FlowContextConfig struct {
	OnDisk bool
}

func (d *FlowContext) Hint(options ...FlowContextOption) {
	var config FlowContextConfig
	for _, option := range options {
		option(&config)
	}
}
