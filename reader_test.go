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

package rbs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/garyburd/redigo/redis"
	"github.com/rafaeljusto/redigomock"
)

var bodies = []string{"alpha", "bravo", "charlie", "delta", "echo"}

func buildConn(name string, skipLast bool) *redigomock.Conn {
	c := redigomock.NewConn()
	for ix, body := range bodies {
		c.Command("HGET", name, fmt.Sprintf(chunkNumber, ix)).Expect([]byte(body))
	}
	c.GenericCommand("HGET").ExpectError(redis.ErrNil)

	if !skipLast {
		c.Command("HGET", name, chunkLast).Expect(int64(len(bodies) - 1))
	}
	return c
}

func TestLookaheadZeroReaderHappy(t *testing.T) {
	name := fmt.Sprintf("boundary-%d", time.Now().Unix())
	c := buildConn(name, false)
	defer c.Clear()

	// This is the chunk-by-chunk option
	r := NewReader(c, name)

	dst := make([]byte, 16)
	for ix, body := range bodies {
		n, err := r.Read(dst)
		if err != nil {
			t.Errorf("Unexpected error: %d, %v\n", ix, err)
		}
		if n != len(body) {
			t.Errorf("Unexpected size: %d - %s", n, body)
		}
		if string(dst[0:n]) != body {
			t.Errorf("Unexpected content: %s - %s", string(dst), body)
		}
	}

	n, err := r.Read(dst)
	if err != io.EOF {
		t.Errorf("Expected EOF: %d - %v - %v\n", n, err, r)
	}

	// Make sure post-close reads get the correct error
	r.Close()
	n, err = r.Read(dst)
	if err != io.ErrClosedPipe {
		t.Errorf("Expected EOF: %d - %v\n", n, err)
	}
}

func TestReaderLookaheadHappy(t *testing.T) {
	name := fmt.Sprintf("lookahead-%d", time.Now().Unix())
	c := buildConn(name, false)
	defer c.Clear()

	// Peek option
	r := NewReader(c, name, ReadLookahead(1))

	dst := make([]byte, 16)
	for ix := 0; ix < len(bodies); ix += 2 {
		body := bodies[ix]
		if ix < len(bodies)-1 {
			body = strings.Join(bodies[ix:ix+2], "")
		}

		n, err := r.Read(dst)
		if err != nil {
			t.Errorf("Unexpected error: %d, %v\n", ix, err)
		}
		if n != len(body) {
			t.Errorf("Unexpected size: %d - %s", n, body)
		}
		if string(dst[0:n]) != body {
			t.Errorf("Unexpected content: %s - %s", string(dst), body)
		}
	}

	n, err := r.Read(dst)
	if err != io.EOF {
		t.Errorf("Expected EOF: %d - %v - %v\n", n, err, r)
	}

	// Make sure post-close reads get the correct error
	r.Close()
	n, err = r.Read(dst)
	if err != io.ErrClosedPipe {
		t.Errorf("Expected EOF: %d - %v\n", n, err)
	}
}

func TestReaderRedisErrorLast(t *testing.T) {
	name := fmt.Sprintf("failures-last-%d", time.Now().Unix())
	c := buildConn(name, true)
	defer c.Clear()
	c.Command("HGET", name, chunkLast).ExpectError(fmt.Errorf("Forced"))
	r := NewReader(c, name, ReadLookahead(2))
	defer r.Close()

	n, err := io.Copy(ioutil.Discard, r)
	if err == nil {
		t.Errorf("Expected error!")
	}
	if n != 0 {
		t.Errorf("Unexpected length: %d\n", n)
	}
}

func TestReaderRedisErrorFlush(t *testing.T) {
	name := fmt.Sprintf("failures-flush-%d", time.Now().Unix())
	c := buildConn(name, true)
	defer c.Clear()

	c.FlushMock = func() error {
		return fmt.Errorf("forced")
	}

	r := NewReader(c, name, ReadLookahead(3))
	defer r.Close()

	n, err := io.Copy(ioutil.Discard, r)
	if err == nil {
		t.Errorf("Expected error!")
	}
	if n != 0 {
		t.Errorf("Unexpected length: %d\n", n)
	}
}

func TestReaderRedisErrorSend(t *testing.T) {
	name := fmt.Sprintf("failures-hget-%d", time.Now().Unix())
	c := redigomock.NewConn()
	defer c.Clear()

	c.GenericCommand("HGET").ExpectError(fmt.Errorf("forced"))

	r := NewReader(c, name, ReadLookahead(3))
	defer r.Close()

	n, err := io.Copy(ioutil.Discard, r)
	if err == nil {
		t.Errorf("Expected error!")
	}
	if n != 0 {
		t.Errorf("Unexpected length: %d\n", n)
	}
}

func TestReaderShortErrorAndRepair(t *testing.T) {
	name := fmt.Sprintf("short-%d", time.Now().Unix())

	c := redigomock.NewConn()

	// This is an artifact of HOW `redigomock` is implemented re: multi-calls
	c.Command("HGET", name, fmt.Sprintf(chunkNumber, 0)).Expect([]byte("alpha"))
	c.Command("HGET", name, fmt.Sprintf(chunkNumber, 1)).
		Expect([]byte("bravo")).
		Expect([]byte("bravo"))
	c.Command("HGET", name, fmt.Sprintf(chunkNumber, 2)).Expect([]byte("charlie"))
	c.Command("HGET", name, chunkLast).ExpectError(redis.ErrNil)

	defer c.Clear()

	r := NewReader(c, name, ReadLookahead(1))
	defer r.Close()

	dst := make([]byte, 7)

	// First call should get "alpha", and `ErrShortBuffer` on "beta"
	n, err := r.Read(dst)
	if err != io.ErrShortBuffer {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if n != len(bodies[0]) {
		t.Errorf("Unexpected size: %d - %s", n, bodies[0])
	}
	if string(dst[0:n]) != bodies[0] {
		t.Errorf("Unexpected content: %s - %s", string(dst), bodies[0])
	}

	// Second call should get "beta", and `ErrShortBuffer` on "charlie"
	n, err = r.Read(dst)
	if err != io.ErrShortBuffer {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if n != len(bodies[1]) {
		t.Errorf("Unexpected size: %d - %s", n, bodies[1])
	}
	if string(dst[0:n]) != bodies[1] {
		t.Errorf("Unexpected content: %s - %s", string(dst[0:n]), bodies[1])
	}
}

func TestReaderUnending(t *testing.T) {
	name := fmt.Sprintf("unending-%d", time.Now().Unix())
	c := buildConn(name, true)
	defer c.Clear()

	// Peek option
	r := NewReader(c, name, ReadLookahead(2))

	dst := make([]byte, 32)
	for ix := 0; ix < 2; ix++ {
		n, err := r.Read(dst)
		if err != nil {
			t.Errorf("Unexpected error: %d, %v\n", ix, err)
		}
		if n == 0 {
			t.Errorf("Unexpected size: %d - %s\n", n, string(dst[0:n]))
		}
	}

	// Subsequent calls should not error, but have 0 length
	for ix := 0; ix < 3; ix++ {
		n, err := r.Read(dst)
		if err != ErrStarveEOF {
			t.Errorf("Unexpected error: %d, %v\n", ix, err)
		}
		if n != 0 {
			t.Errorf("Unexpected size: %d - %s\n", n, string(dst[0:n]))
		}
	}

	// Make sure post-close reads get the correct error
	r.Close()
	n, err := r.Read(dst)
	if err != io.ErrClosedPipe {
		t.Errorf("Expected EOF: %d - %v\n", n, err)
	}
}

type starveReader struct {
	count  int
	closed bool
}

func (sr *starveReader) Close() error {
	sr.closed = true
	return nil
}

func (sr *starveReader) Read(p []byte) (int, error) {
	sr.count++
	return 0, ErrStarveEOF
}

func TestSyncReaderStarveAfter(t *testing.T) {
	r := &SyncReader{}
	ch := r.starveAfter()
	if ch != nil {
		t.Errorf("Expected nil starver: %v\n", ch)
	}

	r.starveDur = time.Millisecond
	ch = r.starveAfter()
	if ch == nil {
		t.Errorf("Expectec non-nil starver: %v\n", ch)
	}
}

func TestSyncReaderStarvation(t *testing.T) {
	dur := time.Duration(250) * time.Millisecond
	r := &SyncReader{
		r:         &starveReader{},
		starveDur: dur,
		ctx:       context.Background(),
	}

	start := time.Now()

	n, err := r.Read(make([]byte, 10))
	if n != 0 {
		t.Errorf("Unexpected length: %d\n", n)
	}
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Unexpected error: %v\n", err)
	}

	diff := time.Since(start)
	if diff < dur {
		t.Errorf("Unexpected duration: %v\n", diff)
	}
	// t.Logf("Diff: %v\n", diff)
}

func TestSyncReaderClose(t *testing.T) {
	ctx, cxl := context.WithCancel(context.Background())
	under := &starveReader{}
	r := &SyncReader{
		r:       under,
		ctx:     ctx,
		cxl:     cxl,
		closers: []io.Closer{under},
	}

	// Wait a bit, before calling the `Close()`
	dur := time.Duration(250) * time.Millisecond
	go func() {
		time.Sleep(dur)
		r.Close()
	}()

	// This should block until `Close()` is called
	start := time.Now()
	n, err := r.Read(make([]byte, 10))
	if n != 0 {
		t.Errorf("Unexpected length: %d\n", n)
	}
	if err != io.ErrClosedPipe {
		t.Errorf("Unexpected error: %v\n", err)
	}

	diff := time.Since(start)
	if diff < dur {
		t.Errorf("Unexpected duration: %v\n", diff)
	}

	// Verify that the close got called
	if !under.closed {
		t.Errorf("Did not call `Close()`")
	}

	// A read after close should be a VERY quick return
	start = time.Now()
	n, err = r.Read(make([]byte, 10))
	if n != 0 {
		t.Errorf("Unexpected length: %d\n", n)
	}
	if err != io.ErrClosedPipe {
		t.Errorf("Unexpected error: %v\n", err)
	}

	diff = time.Since(start)
	if diff > (time.Duration(10) * time.Millisecond) {
		t.Errorf("Unexpected duration: %v\n", diff)
	}
}

func TestSyncReaderStimulusNoDrain(t *testing.T) {
	count := int(2 + (time.Now().Unix() % 10))
	ch := make(chan struct{}, count)
	for ix := 0; ix < count; ix++ {
		ch <- struct{}{}
	}
	close(ch)

	ctr := &starveReader{}
	r := &SyncReader{
		r:         ctr,
		ctx:       context.Background(),
		stim:      ch,
		skipDrain: true,
	}

	// After the stimulus is closed, should get `io.ErrClosedPipe`
	n, err := r.Read(make([]byte, 10))
	if n != 0 {
		t.Errorf("Unexpected length: %d\n", n)
	}
	if err != io.ErrClosedPipe {
		t.Errorf("Unexpected error: %v\n", err)
	}

	// Underneath, the `starveReader` should've been
	// called `count+1` times
	if ctr.count != count+1 {
		t.Errorf("Unexpected number of invocations: %d\n", ctr.count)
	}
}

func TestSyncReaderStimulusDrain(t *testing.T) {
	count := int(2 + (time.Now().Unix() % 10))
	ch := make(chan struct{}, count)
	iter := int(2 + (time.Now().Unix() % 5))

	ctr := &starveReader{}
	r := &SyncReader{
		r:    ctr,
		ctx:  context.Background(),
		stim: ch,
	}

	go func() {
		for jx := 0; jx < iter; jx++ {
			time.Sleep(100 * time.Millisecond)
			for ix := 0; ix < count; ix++ {
				ch <- struct{}{}
			}
		}
		time.Sleep(100 * time.Millisecond)
		close(ch)
	}()

	// After the stimulus is closed, should get `io.ErrClosedPipe`
	n, err := r.Read(make([]byte, 10))
	if n != 0 {
		t.Errorf("Unexpected length: %d\n", n)
	}
	if err != io.ErrClosedPipe {
		t.Errorf("Unexpected error: %v\n", err)
	}

	// Underneath, the `starveReader` should've been
	// called (at least) `iter+1` times
	// ... because we're dealing with synchronous code, the
	// timey-wimey stuff is a little bit wobbly.
	if ctr.count < iter+1 || ctr.count > iter+3 {
		t.Errorf("Unexpected number of invocations: %d != %d\n", ctr.count, iter)
	}
}

func TestSyncReaderHappyPath(t *testing.T) {
	dst := bytes.NewBufferString("")
	r := &SyncReader{
		r:   strings.NewReader(alphabet),
		ctx: context.Background(),
	}

	n, err := io.Copy(dst, r)
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if int(n) != len(alphabet) {
		t.Errorf("Unexpected size: %d\n", n)
	}
	if dst.String() != alphabet {
		t.Errorf("Unexpected content: %s\n", dst.String())
	}
}

type mockReceiver struct {
	ctx      context.Context
	delay    time.Duration
	idx      int
	contents []interface{}
}

func (mock *mockReceiver) Receive() interface{} {
	select {
	case <-mock.ctx.Done():
		return mock.ctx.Err()
	case <-time.After(mock.delay):
		if mock.idx >= len(mock.contents) {
			return fmt.Errorf("eof")
		}
	}
	r := mock.contents[mock.idx]
	mock.idx++
	return r
}

func TestSyncReaderPubSubHappyPath(t *testing.T) {
	s := &SyncReader{ctx: context.Background()}

	mock := &mockReceiver{
		ctx:   context.Background(),
		delay: time.Duration(100) * time.Millisecond,
		contents: []interface{}{
			redis.Subscription{},
			redis.Message{},
			redis.Subscription{},
			redis.Message{},
		},
	}
	stim := make(chan struct{})

	s.wg.Add(1)
	go s.pubSubStimulus(stim, mock)

	count := 0
	for _ = range stim {
		count++
	}

	s.wg.Wait()

	if count != 2 {
		t.Errorf("Unexpected count: %d\n", count)
	}
}

func TestSyncReaderPubSubBlockedClose(t *testing.T) {
	ctx, cxl := context.WithCancel(context.Background())
	s := &SyncReader{ctx: ctx, cxl: cxl}

	mock := &mockReceiver{
		ctx:   context.Background(),
		delay: time.Duration(10) * time.Millisecond,
		contents: []interface{}{
			redis.Message{},
			redis.Message{},
			redis.Message{},
			redis.Message{},
		},
	}
	stim := make(chan struct{})

	s.wg.Add(1)
	go s.pubSubStimulus(stim, mock)

	// If we never pull off the stim, it could block, until
	// the context is closed.
	time.Sleep(time.Duration(150) * time.Millisecond)
	s.Close()

	s.wg.Wait()

	if mock.idx > 1 {
		t.Errorf("Unexpected progress delivering content: %d\n", mock.idx)
	}
}

func TestSyncReaderInitDefaultFail(t *testing.T) {
	r, err := NewSyncReader(
		context.Background(),
		strings.NewReader(""),
		redigomock.NewConn(),
	)
	if err == nil {
		t.Errorf("Expected error!\n")
	}
	if r != nil {
		t.Errorf("Expected nil: %v\n", r)
	}
}

func TestSyncReaderInitStdFail(t *testing.T) {
	r, err := NewSyncReader(
		context.Background(),
		ioutil.NopCloser(strings.NewReader("")),
		redigomock.NewConn(),
		SyncStdSub(),
	)
	if err == nil {
		t.Errorf("Expected error!\n")
	}
	if r != nil {
		t.Errorf("Expected nil: %v\n", r)
	}
}

// // TODO: Not sure why can't get `redigomock` to fail at Subscribe time
// func TestSyncReaderInitSubscribeFail(t *testing.T) {
// 	c := redigomock.NewConn()
// 	defer c.Clear()
//
// 	name := "gonna-really-fail"
// 	cmd := c.Command("SUBSCRIBE", name).ExpectError(fmt.Errorf("forced"))
//
// 	r, err := SyncReader(
// 		context.Background(),
// 		ioutil.NopCloser(strings.NewReader("")),
// 		c,
// 		Subscribe(name),
// 	)
// 	if err == nil {
// 		t.Errorf("Expected error!\n")
// 	}
// 	if r != nil {
// 		t.Errorf("Expected nil: %v\n", r)
// 	}
// }

func TestSyncReaderInitStdSuccess(t *testing.T) {
	name := fmt.Sprintf("std-success-%d", time.Now().UnixNano())
	expected := fmt.Sprintf(syncChannelPattern, name)

	b := NewReader(redigomock.NewConn(), name)
	r, err := NewSyncReader(
		context.Background(),
		b,
		redigomock.NewConn(),
		SyncStdSub(),
	)
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	defer r.Close()

	bare, ok := r.(*SyncReader)
	if !ok {
		t.Errorf("Unexpected type: %T\n", r)
	}
	if len(bare.channels) != 1 {
		t.Errorf("Unexpected channels: %v\n", bare.channels)
	}
	if bare.channels[0] != expected {
		t.Errorf("Unexpected channel name: %s\n", bare.channels[0])
	}

	// Can also test default value for `starve`
	if bare.starveDur != time.Duration(0) {
		t.Errorf("Unexpected starve value: %v\n", bare.starveDur)
	}
}

func TestSyncReaderInitSusbscribeSuccess(t *testing.T) {
	now := time.Now().UnixNano()
	n1 := fmt.Sprintf("alternate-1-%d", now)
	n2 := fmt.Sprintf("alternate-2-%d", now)
	n3 := fmt.Sprintf("alternate-3-%d", now)

	starve := time.Duration(100+(now%100)) * time.Millisecond
	r, err := NewSyncReader(
		context.Background(),
		strings.NewReader(""),
		redigomock.NewConn(),
		SyncSub(n1),
		SyncSub(n2),
		SyncSub(n3),
		SyncStarve(starve),
	)

	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	defer r.Close()

	bare, ok := r.(*SyncReader)
	if !ok {
		t.Errorf("Unexpected type: %T\n", r)
	}
	if len(bare.channels) != 3 {
		t.Errorf("Unexpected channels: %v\n", bare.channels)
	}
	for ix, name := range bare.channels {
		if name != n1 && name != n2 && name != n3 {
			t.Errorf("Unexpected subscription: #%d - %s\n", ix, name)
		}
	}

	// Also check on starve value
	if bare.starveDur != starve {
		t.Errorf("Unexpected starve duration: %v\n", bare.starveDur)
	}
}

// func TestIntegrationSyncReaderPubSubClient(t *testing.T) {
// 	// For localhost, run this test with:
// 	//     RBS_TEST_REDIS=localhost:6379
// 	env := "RBS_TEST_REDIS"
// 	// svr := os.Getenv(env)
// 	svr := "localhost:6379"
// 	if svr == "" {
// 		t.Skipf("No config found for %s", env)
// 	}
//
// 	tx, err := redis.Dial("tcp", svr)
// 	if err != nil {
// 		t.Fatalf("Could not connect: %s - %v\n", svr, err)
// 	}
// 	defer tx.Close()
//
// 	rx, err := redis.Dial("tcp", svr)
// 	if err != nil {
// 		t.Fatalf("Could not connect: %s - %v\n", svr, err)
// 	}
// 	defer rx.Close()
//
// 	name := fmt.Sprintf("pub-sub-trial-%d", time.Now().UnixNano())
// 	psc := redis.PubSubConn{Conn: rx}
//
// }
