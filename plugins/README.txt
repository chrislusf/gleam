How to add a new input plugin

1. follow an example, such as github.com/chrislusf/gleam/plugins/cassandra/*

Usually a data set consists of many data shards.
So an input plugin has 3 steps:

1) generate a list of shard info. this runs on driver.
2) send each piece of shard info to an remote executors
3) Each executor fetch external data according to the shard info.

The shard info should be serializable/deserializable.
Usually just need to use gob to serialize and deserialize it.

Since the mapper to process shard info is in Go, the call to "gio.Init()"
is required.
