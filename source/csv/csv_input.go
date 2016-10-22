package csv

import (
	"path/filepath"
)

type CsvInput struct {
	FileNames       []string
	HasHeader       bool
	FileNamePattern string
}

func New(fileNameOrFolder ...string) *CsvInput {
	return &CsvInput{
		FileNames: fileNameOrFolder,
		HasHeader: false,
	}
}

// SetHasHeader sets whether header row exists or not. Default to false.
func (ci *CsvInput) SetHasHeader(hasHeader bool) *CsvInput {
	ci.HasHeader = hasHeader
	return ci
}

// SetFileNamePattern sets file name pattern, e.g., "*.csv".
// This is used when one of the fileNames is a folder.
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
