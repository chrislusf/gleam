# How an input source work? 
Each source format should have two functions: List() and Open()

List() generates a list of file locations.
Open() read from one location. Each row of input is sent to next dataset.

# Understand the code

We taking csv for example.

CsvInputFormat: a managerial type to manage CsvInputSplitter, CsvInputSplit, CsvInputSplitReader
CsvInput: any type
CsvInputSplitter: takes CsvInput, and produce a list of CsvInputSplit
CsvInputSplit: must be a serializable type, will be sent to executors
CsvInputSplitReader: runs on executors, read CsvInputSplit

When import this package:
  1. register mapping from "csv" to CsvInputFormat
On Driver:
  1. User specifiies CsvInput
  2. "csv"=>CsvInputFormat, +CsvInput => CsvSplitter.Split()/SplitBySize()/SplitByLines() => []CsvInputSplit
  3. send "csv", CsvInputSplit to each executor
On Executors:
  1. "csv"=>CsvInputFormat=>CsvInputFormat, CsvInputSplit, decode from []byte
  2. "csv"=>CsvInputFormat=>CsvInputSplitReader, read out lines by CsvInputSplit


# How to add one more input source?
1. Clone the csv or ints folder, rename all struct name first.
2. Make sure everything is mostly compiled.
3. add an import such as this in dataset_input.go file
	_ "github.com/chrislusf/gleam/source/csv"
