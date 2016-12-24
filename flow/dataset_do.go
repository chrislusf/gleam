package flow

// Do accepts a function to transform a dataset into a new dataset.
// This allows custom complicated pre-built logic.
func (d *Dataset) Do(fn func(*Dataset) *Dataset) *Dataset {
	return fn(d)
}
