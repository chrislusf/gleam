# Benchmarking timsort

Tested on: Intel Core 2 Duo 2.13 GHz running OS X 10.6.8 (64 bit), Go 1.0.2

Numbers are ns/op as reported by `go -test.bench=.*`. First number is for timsort, followed by standard sort in parantheses.
`Xor100` means - sorting 100 elements generated using `Xor` method,
`Random1M` means - sorting 1 meg (1024*1024) records generated randomly. 
For more detail on data shapes see the source - [bench_test.go][bench_test.go]. 
Three columns represent three benchmark runs. 

    Sorted100:          6983(54955)            7083(55656)            7229(55295)
    RevSorted100:      10805(56500)            7557(56417)            7930(61509)
    Xor100:            36200(65847)           36309(64788)           36432(64992)
    Random100:         47786(72601)           47385(72211)           44159(74201)

    Sorted1K:	         51600(891385)          52163(882081)          52162(886594)
    RevSorted1K:       58074(883533)          63933(926461)          58346(874746)
    Xor1K:            411326(997065)         427045(1009220)        443398(1001839)
    Random1K:         652196(1124785)        669132(1131821)        656515(1175234)

    Sorted1M:       49969680(2076530000)   50287340(2081669000)   50468320(2081046000)
    RevSorted1M:    58943960(2062623000)   62392160(2102090000)   59843080(2094715000)
    Xor1M:         709156400(1127898000)  780231800(1265369000)  714170400(1182639000)
    Random1M:     1594071000(2856945000) 1553154000(2874639000) 1668860000(2812400000)

Not surprisingly, timsort is crazy fast on sorted inputs. But even for random and quasi-random (xor) inputs, timsort is nearly 2x faster than built-in sort.

### Disclaimer

The above benchmark applies only to one specific type of data element (`record` structure as defined in [bench_test.go][bench_test.go]). For other data types results may vary.

[bench_test.go]: http://github.com/psilva261/timsort/blob/master/bench_test.go
