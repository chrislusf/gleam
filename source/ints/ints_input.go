package ints

type IntsInput struct {
	Start     int
	Stop      int
	SplitSize int
}

func New(start, stop, splitSize int) *IntsInput {
	return &IntsInput{
		Start:     start,
		Stop:      stop,
		SplitSize: splitSize,
	}
}

func (ci *IntsInput) GetType() string {
	return "ints"
}
