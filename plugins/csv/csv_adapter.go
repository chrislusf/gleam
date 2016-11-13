package csv

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"strings"

	"github.com/chrislusf/gleam/adapter"
)

func init() {
	gob.Register(CsvDataSplit{})

	adapter.RegisterAdapter(NewCsvAdapter())

	// assuming the connection id is the same as the adapter type
	adapter.RegisterConnection("csv", "csv")
}

func New(fileOrPattern string) *Source {
	s := &Source{}
	if strings.ContainsAny(fileOrPattern, "/\\") {
		s.folder = filepath.Dir(fileOrPattern)
		s.fileBaseName = filepath.Base(fileOrPattern)
		s.Path = fileOrPattern
	} else {
		s.folder, _ = os.Getwd()
		s.fileBaseName = fileOrPattern
		s.Path = filepath.Join(s.folder, s.fileBaseName)
	}
	if strings.ContainsAny(s.fileBaseName, "*?") {
		s.hasWildcard = true
	}

	return s
}

type Source struct {
	folder       string
	fileBaseName string
	hasWildcard  bool
	Path         string
	HasHeader    bool
	Parallel     int
}

type CsvDataSplit struct {
	Config    map[string]string
	FileName  string
	HasHeader bool
}

func (q *Source) GetParallelLimit() int {
	return q.Parallel
}

func (q *Source) SetHasHeader(hasHeader bool) *Source {
	q.HasHeader = hasHeader
	return q
}

func (q *Source) SetParallelLimit(paraLimit int) *Source {
	q.Parallel = paraLimit
	return q
}

func (q *Source) AdapterName() string {
	return "csv"
}

type CsvAdapter struct {
}

func NewCsvAdapter() *CsvAdapter {
	return &CsvAdapter{}
}

func (c *CsvAdapter) AdapterName() string {
	return "csv"
}

func (c *CsvAdapter) LoadConfiguration(config map[string]string) {
}

func (cs CsvDataSplit) GetConfiguration() map[string]string {
	return cs.Config
}
