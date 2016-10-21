package csv

import (
	"path/filepath"
)

type CsvInput struct {
	FileNames       []string
	HasHeader       bool
	FileNamePattern string
}

func New(fileNames ...string) *CsvInput {
	return &CsvInput{
		FileNames: fileNames,
		HasHeader: true,
	}
}

func (ci *CsvInput) SetHasHeader(hasHeader bool) *CsvInput {
	ci.HasHeader = hasHeader
	return ci
}

func (ci *CsvInput) SetFileNamePattern(pattern string) *CsvInput {
	ci.FileNamePattern = pattern
	return ci
}

func (ci *CsvInput) Match(fullPath string) bool {
	baseName := filepath.Base(fullPath)
	match, _ := filepath.Match(ci.FileNamePattern, baseName)
	return match
}

func (ci *CsvInput) GetType() string {
	return "csv"
}
