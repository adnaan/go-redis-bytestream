// Copyright 2016 Orion Labs, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rbs_test

import (
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/onbeep/go-redis-bytestream"

	"golang.org/x/net/context"
)

func TestIntegrationPackageEnd2End(t *testing.T) {
	// For localhost, run this test with:
	//     RBS_TEST_REDIS=localhost:6379
	env := "RBS_TEST_REDIS"
	svr := os.Getenv(env)
	if svr == "" {
		t.Skipf("No environment value found for %s", env)
	}

	srcHash := sha1.New()
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	src := io.TeeReader(rand.Reader, srcHash)
	src = &randomSizeReader{r: src}
	src = &randomDelayReader{r: src, max: (time.Duration(100) * time.Millisecond)}
	src = &cancelReader{r: src, ctx: ctx}

	name := fmt.Sprintf("roundtrip-package-%d", time.Now().UnixNano())
	result := make(chan string)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(svr string, name string, result chan<- string) {
		defer wg.Done()
		defer close(result)

		rx, err := redis.Dial("tcp", svr)
		if err != nil {
			return
		}
		defer rx.Close()

		subs, err := redis.Dial("tcp", svr)
		if err != nil {
			return
		}
		defer subs.Close()

		reader, err := rbs.NewSyncReader(
			context.Background(),
			rbs.NewReader(rx, name, rbs.ReadLookahead(3)),
			subs,
			rbs.SyncStdSub(),
			rbs.SyncStarve(time.Second),
		)
		if err != nil {
			return
		}
		defer reader.Close()

		dst := sha1.New()

		_, err = io.Copy(dst, reader)
		if err != nil {
			fmt.Printf("Unexpected error: %v\n", err)
			return
		}

		result <- fmt.Sprintf("%x", dst.Sum(nil))

	}(svr, name, result)

	tx, err := redis.Dial("tcp", svr)
	if err != nil {
		t.Fatalf("Could not connect: %s - %v\n", svr, err)
	}
	defer tx.Close()

	writer, err := rbs.NewWriter(
		tx,
		name,
		rbs.WriteMaxChunk(256),
		rbs.WriteMinChunk(64),
		rbs.WriteExpire(120),
		rbs.WriteStdPub(),
	)
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	defer writer.Close()

	buf := make([]byte, 512)
	_, err = io.CopyBuffer(writer, src, buf)
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	writer.Close()

	dh := ""
	select {
	case h, ok := <-result:
		if !ok {
			t.Fatalf("Result channel closed")
		}
		dh = h
	case <-time.After(time.Second):
		t.Fatalf("Timeout")
	}

	sh := fmt.Sprintf("%x", srcHash.Sum(nil))

	if sh != dh {
		t.Errorf("Mismatch hashes: %s - %s\n", sh, dh)
	}

	// t.Logf("Results: %s -> %d / %s\n", name, n, sh)
}

func TestIntegrationNoStreamEnd2End(t *testing.T) {
	// For localhost, run this test with:
	//     RBS_TEST_REDIS=localhost:6379
	env := "RBS_TEST_REDIS"
	svr := os.Getenv(env)
	if svr == "" {
		t.Skipf("No environment value found for %s", env)
	}

	// Setup the Redis connection pool
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 10 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", svr)
		},
	}

	name := fmt.Sprintf("roundtrip-no-stream-%d", time.Now().UnixNano())

	// First, gotta write out the resource
	chunkSize := uint16(256)
	maxSize := 87 + (4 * chunkSize)
	srcHash := sha1.New()
	src := io.TeeReader(io.LimitReader(rand.Reader, int64(maxSize)), srcHash)

	tx := pool.Get()
	defer tx.Close()

	writer, err := rbs.NewWriter(
		tx,
		name,
		rbs.WriteMaxChunk(chunkSize),
		rbs.WriteExpire(10),
	)
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	defer writer.Close()

	n, err := io.Copy(writer, src)
	if err != nil {
		t.Fatalf("Unexpected error: %v\n", err)
	}
	if n != int64(maxSize) {
		t.Errorf("Unexpected size: %d\n", n)
	}
	writer.Close()
	srcSum := fmt.Sprintf("%x", srcHash.Sum(nil))

	// Now, try to read the file
	rx := pool.Get()
	// defer rx.Close()

	// First, we'll go for the non-sync reader
	r1 := rbs.NewReader(rx, name, rbs.ReadLookahead(3))
	defer r1.Close()

	d1Hash := sha1.New()
	n, err = io.Copy(d1Hash, r1)
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if n != int64(maxSize) {
		t.Errorf("Unexpected size: %d\n", n)
	}
	d1Sum := fmt.Sprintf("%x", d1Hash.Sum(nil))
	if d1Sum != srcSum {
		t.Errorf("Mismatch contents: %s != %s\n", src, d1Sum)
	}

	// Lastly, check the sync reader can handle this scenario
	subs := pool.Get()
	// defer subs.Close()

	r2, err := rbs.NewSyncReader(
		context.Background(),
		rbs.NewReader(rx, name, rbs.ReadLookahead(3)),
		subs,
		rbs.SyncStdSub(),
		rbs.SyncStarve(time.Second),
	)
	if err != nil {
		return
	}
	defer r2.Close()

	d2Hash := sha1.New()
	n, err = io.Copy(d2Hash, r2)
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if n != int64(maxSize) {
		t.Errorf("Unexpected size: %d\n", n)
	}
	d2Sum := fmt.Sprintf("%x", d2Hash.Sum(nil))
	if d2Sum != srcSum {
		t.Errorf("Mismatch contents: %s != %s\n", src, d2Sum)
	}

}

type cancelReader struct {
	r   io.Reader
	ctx context.Context
}

func (cr *cancelReader) Read(p []byte) (int, error) {
	if cr.ctx.Err() != nil {
		return 0, io.EOF
	}
	return cr.r.Read(p)
}

type randomSizeReader struct {
	r io.Reader
}

func (rs *randomSizeReader) Read(p []byte) (int, error) {
	mb, err := rand.Int(rand.Reader, big.NewInt(int64(len(p))))
	if err != nil {
		return 0, err
	}
	size := int(1 + mb.Int64())
	// fmt.Printf("%d -> %d\n", len(p), size)
	return rs.r.Read(p[:size])
}

type randomDelayReader struct {
	r   io.Reader
	max time.Duration
}

func (rd *randomDelayReader) Read(p []byte) (int, error) {
	mb, err := rand.Int(rand.Reader, big.NewInt(int64(rd.max)))
	if err != nil {
		return 0, err
	}
	dur := time.Duration(mb.Int64())
	// fmt.Printf("%f -> %f\n", rd.max.Seconds()*1000, dur.Seconds()*1000)
	time.Sleep(dur)
	return rd.r.Read(p)
}
