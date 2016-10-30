package driver

type Option struct {
	Master       string
	DataCenter   string
	Rack         string
	TaskMemoryMB int
	FlowBid      float64
	Module       string
	Host         string
	Port         int
}
