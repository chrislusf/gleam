This is a piece of code to profile Gleam code.

It was a try-and-error approach to improve Gleam performance.

# Input Data
The data input is 1GB gensort generated text data file.
http://www.ordinal.com/gensort.html

gensort -a 10737418 record_1Gb_input.txt


# Starting Baseline
At the beginning, the performance is fairly miserable.

## Linux Sort performance
$ time sort ~/Desktop/record_1Gb_input.txt > /dev/null
real	4m52.789s
user	4m51.454s
sys	0m1.108s

## Gleam Performance
6m49s for basic "cat" in distributed mode.

# Approaches
1. Starting with standalone mode, which is easier to profile.
2. Analyzing the pprof graph, try to add buffers to most of the io.Writers.

# Final Performance
1m54s for distributed 4-parition gleam sorting. (15m34->1m54, reduced to 12.2%)
2m30s for distributed 4-parition linux sorting on gleam. (6m49s->2m30s, reduced basic linux sort time to 37%)
3m5s for distributed gleam sorting. (7m7s->3m5s, reduced to 43%)

# Notes

graph time  partition  command         mode
p1    6m49s   1           cat          distributed
p2    2m22s   1           cat          standalone
p3    7m7s    1           sort         standalone
p4    7m19s   1           sort         standalone  // add some write buffer in dataset_output.go
p5    7m32s   1           sort         standalone  // channel_util.go add write buffer
p6    1m41s   1           cat          standalone
p7    25s     1           cat          standalone  // add write buffer in dataset_source.go TextFile()
p8    5m27s   1           sort         standalone  // add write buffer in dataset_source.go TextFile()
p9    6m9s    1           sort         distributed
p10   5m50s   1           gleam.sort   distributed
p11   15m34   4           gleam.sort   distributed
p12   11m59   2           gleam.sort   distributed
p13   6m1s    1           gleam.sort   distributed
p14   4m9s    1           gleam.sort   standalone
p15   4m58    2           gleam.sort   standalone
p16   5m5s    4           gleam.sort   standalone
p17   3m3s    4           gleam.sort   standalone  // add writer buffer in runner.go RunDatasetShard()
p18   14m21   4           gleam.sort   distributed // add writer buffer in executor.go ExecuteInstruction(), not working
profiling gleam execute
exe1 12m15    4           gleam.sort   distributed
p19  2m0s     2           gleam.sort   distributed  // add BufWrites in in executor.go ExecuteInstruction()
___  1m54     4           gleam.sort   distributed
___  1m54     4           sort         distributed 
___  3m5s     4           gleam.sort   standalone 
