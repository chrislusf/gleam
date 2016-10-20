package csv

type CsvInput struct {
	FileNames []string
	HasHeader bool
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

func (ci *CsvInput) GetType() string {
	return "csv"
}
