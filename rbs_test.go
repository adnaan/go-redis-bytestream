package rbs_test

import (
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"math/big"
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
	// svr := os.Getenv(env)
	svr := "localhost:6379"
	if svr == "" {
		t.Skipf("No config found for %s", env)
	}

	srcHash := sha1.New()
	// dstHash := sha1.New()
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
	n, err := io.CopyBuffer(writer, src, buf)
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

	t.Logf("Results: %s -> %d / %s\n", name, n, sh)
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
