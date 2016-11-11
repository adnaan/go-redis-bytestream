# go-redis-bytestream
Store and Retrieve streaming data in Redis via `io.Reader` and `io.Writer` utilities

# Building and Testing
## Pre-reqs
```
$ go get github.com/garyburd/redigo/redis
$ go get github.com/rafaeljusto/redigomock
```

## Integration Tests
If you'd like to run the integration tests against an actual Redis server,
you just set the host:port via the environment variable `RBS_TEST_REDIS`:
```
$ RBS_TEST_REDIS=localhost:6379 go test -v
```

# Usage
Look in `rbs_test.go` to see more specifics. Outlined here is the basic
intended usage.

## Writing
```
name := "example_stream_name"
tx, _ := redis.Dial(...)

writer, err := rbs.NewWriter(tx, name, rbs.WriteStdPub())
if err != nil {...}
defer writer.Close()

var src io.Reader = ...

io.Copy(writer, src)
```

## Reading
```
ctx := context.Background()
name := "example_stream_name"
rx, _ := redis.Dial(...)  // Transactional connection
ps, _ := redis.Dial(...)  // PubSub connection

reader, err := rbs.NewSyncReader(ctx, rbs.NewReader(rx, name), ps, rbs.SyncStdSub())
if err != nil {...}
defer writer.Close()

var dst io.Writer = ...

io.Copy(dst, reader)
```
