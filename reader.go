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
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/garyburd/redigo/redis"
)

const chunkNumber = "c:%d"
const chunkLast = "c:last"

// ErrStarveEOF marks when we are returning a zero size buffer,
// in the cases where either we do not yet know the full size of the
// stream, or we do, and have not received that data yet.
var ErrStarveEOF = errors.New("data not yet available")

// RedReader is the implementation that retrieves a variable-length stream
// of bytes from a Redis hash, as written by its RedWriter counterpart
type RedReader struct {
	quit bool
	conn redis.Conn
	name string
	next uint16
	last uint16
	ends bool
	peek uint8
}

// NewReader creates an io.ReadCloser that assembles a byte stream as
// stored in complete chunks on a Redis hash.
// The underlying implementation is a *RedReader, and is the intended
// counterpart to the *RedWriter.
//
// The name parameter is used as the key for the Redis hash.
//
// Configuration is by functional options, as inspired by this blog post:
// http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
//
// See: ReadLookahead
func NewReader(conn redis.Conn, name string, options ...func(*RedReader)) io.ReadCloser {
	r := &RedReader{conn: conn, name: name}
	for _, option := range options {
		option(r)
	}
	return r
}

// ReadLookahead sets the number of chunks to optimistically look for when
// hitting the network doing a single fetch from Redis
func ReadLookahead(peek uint8) func(*RedReader) {
	return func(r *RedReader) {
		r.peek = peek
	}
}

// Close conforms to the io.Closer interface
//
// This will also close the underlying Redis connection
func (rr *RedReader) Close() error {
	rr.conn.Close()
	rr.quit = true
	return nil
}

// Read conforms to the io.Reader interface
//
// It is recommended that the buffer p is at least as big as
// ((lookahead + 1) X maxChunkSize), otherwise you run the risk of
// receiving an io.ErrShortBuffer error.
//
// (Receiving a io.ErrShortBuffer is not necessarily a fatal condition:
// the Read will try to "keep its place" in the stream and delivery
// what bytes it can per invocation.)
//
// This implementation has one very specific behavior: if the end-of-stream
// marker has not yet been written, but there are currently no bytes to
// deliver, the error ErrStarveEOF is returned.
//
// If the byte stream is already fully written, or would be by the Read's
// get to the end-of-stream marker, this acts just like any other io.Reader
func (rr *RedReader) Read(p []byte) (int, error) {
	if rr.quit {
		return 0, io.ErrClosedPipe
	}

	if rr.ends && rr.next > rr.last {
		return 0, io.EOF
	}

	// Figure out how many chunks to ask for
	max := rr.next + uint16(rr.peek)
	if rr.ends && max > rr.last {
		max = rr.last
	}

	// Register the chunk requests
	for idx := rr.next; idx <= max; idx++ {
		key := fmt.Sprintf(chunkNumber, idx)
		// fmt.Printf("Key: %s\n", key)
		err := rr.conn.Send("HGET", rr.name, key)
		if err != nil {
			return 0, err
		}
	}

	// If we don't already know the max chunk, ask for it
	if !rr.ends {
		err := rr.conn.Send("HGET", rr.name, chunkLast)
		if err != nil {
			return 0, err
		}
	}

	// Send the requests to redis
	err := rr.conn.Flush()
	if err != nil {
		return 0, err
	}

	// Pull the bytes off of each successive chunk
	clearBuffer := false
	size := 0
	short := false
	for idx := rr.next; idx <= max; idx++ {
		b, err := redis.Bytes(rr.conn.Receive())
		if err != nil {
			if err != redis.ErrNil {
				return 0, err
			}
			clearBuffer = true
		}
		if clearBuffer {
			continue
		}
		// Too much data fetched for the buffer sent in
		// (This may not be fatal, if peek is non-zero.)
		if size+len(b) > len(p) {
			short = true
			clearBuffer = true
			continue
		}

		// Actually move the bytes over
		copy(p[size:(size+len(b))], b)
		size += len(b)
		rr.next++
	}

	// Don't forget to check for the max chunck info
	// If we don't already know the max chunk, ask for it
	if !rr.ends {
		end, err := redis.Uint64(rr.conn.Receive())
		if err != nil && err != redis.ErrNil {
			if err != redis.ErrNil {
				return 0, err
			}
		}
		if err != redis.ErrNil {
			rr.last = uint16(end)
			rr.ends = true
		}
	}

	if short {
		return size, io.ErrShortBuffer
	}

	if size == 0 {
		// At the exhortation of the io.Reader interface, we should
		// NOT send `0, nil`... We should return some signature of WHY we
		// are returning with size zero.
		return size, ErrStarveEOF
	}

	return size, nil
}

// A SyncReader is the implementation that reads from an underlying io.Reader,
// and will block then retry upon stimulus if the underlying Reader returned
// ErrStarveEOF.
type SyncReader struct {
	r         io.Reader
	ctx       context.Context
	cxl       context.CancelFunc
	wg        sync.WaitGroup
	stim      <-chan struct{}
	skipDrain bool
	starveDur time.Duration
	closers   []io.Closer
	channels  []interface{}
}

// NewSyncReader creates an io.ReadCloser that reads bytes from an underlying
// io.Reader. However, if the underlying io.Reader returned an ErrStarveEOF,
// this implementation will block until receiving a Redis PubSub stimulus,
// whereupon it will attempt to read from the underlying io.Reader again.
//
// The intendend usage for SyncReader is to be used with a RedReader (or any
// io.Reader that sends the same ErrStarveEOF), but
// it would function as a simple pass through for any other io.Reader.
//
// This accepts a Context object, so a client can set a timeout, or cancel
// reading mid-stream.
//
// The passed-in Redis connection *MUST* be different the one used in the
// underlying RedReader, as this pub/sub subscription cannot also
// handle concurrent transactional needs.
//
// The client is expected to provide (at least) one pub/sub channel upon which
// this implementation will listen for syncronization events.
//
// Configuration is by functional options, as inspired by this blog post:
// http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
//
// See: SyncSub, SyncStdSub, SyncStarve
func NewSyncReader(
	parent context.Context,
	r io.Reader,
	subs redis.Conn,
	options ...func(*SyncReader) error,
) (io.ReadCloser, error) {
	ctx, cxl := context.WithCancel(parent)
	stim := make(chan struct{})
	result := &SyncReader{
		r:       r,
		ctx:     ctx,
		cxl:     cxl,
		stim:    stim,
		closers: []io.Closer{subs},
	}
	if c, ok := r.(io.Closer); ok {
		result.closers = append(result.closers, c)
	}

	for _, option := range options {
		err := option(result)
		if err != nil {
			result.Close()
			return nil, err
		}
	}

	if len(result.channels) < 1 {
		result.Close()
		return nil, fmt.Errorf("no channels specified")
	}

	psc := redis.PubSubConn{Conn: subs}
	err := psc.Subscribe(result.channels...)
	if err != nil {
		result.Close()
		return nil, err
	}

	result.wg.Add(1)
	go result.pubSubStimulus(stim, psc)

	return result, nil
}

// SyncSub allows a client to add an arbitrary Redis PubSub channel
// to listen to for stimulus
func SyncSub(pubSubChan interface{}) func(*SyncReader) error {
	return func(sr *SyncReader) error {
		sr.channels = append(sr.channels, pubSubChan)
		return nil
	}
}

// SyncStdSub sets up the "standard" channel subscription. This is the
// Read-side counterpart to WriteStdPub
func SyncStdSub() func(*SyncReader) error {
	return func(sr *SyncReader) error {
		r, ok := sr.r.(*RedReader)
		if !ok {
			return fmt.Errorf("Expected `*rbs.NewReader`: %T", sr.r)
		}
		sr.channels = append(sr.channels, fmt.Sprintf(syncChannelPattern, r.name))
		return nil
	}
}

// SyncStarve sets the maximum amount of time to wait between stimulus/Read
// attempts. If this timeout is tripped, the reader will
// return io.ErrUnexpectedEOF
func SyncStarve(dur time.Duration) func(*SyncReader) error {
	return func(sr *SyncReader) error {
		sr.starveDur = dur
		return nil
	}
}

// Close conforms to the io.Closer interface
//
// This will close the passed in Redis connection AND the passed in
// Reader if it also implements io.Closer
func (sr *SyncReader) Close() error {
	sr.cxl()
	for _, closer := range sr.closers {
		closer.Close()
	}
	sr.wg.Wait()
	return nil
}

func (sr *SyncReader) starveAfter() <-chan time.Time {
	if sr.starveDur == 0 {
		return nil
	}
	return time.After(sr.starveDur)
}

// Read conforms to the io.Reader interface
//
// This will pass through any results from the underlying Read, unless
// we recieved an ErrStarveEOF, whereupon it will block and re-attempt
// a Read when it receives pub/sub stimulus.
func (sr *SyncReader) Read(p []byte) (n int, err error) {
	if sr.ctx.Err() != nil {
		// Error if we're already closed
		return 0, io.ErrClosedPipe
	}
	for n == 0 && (err == nil || err == ErrStarveEOF) {
		n, err = sr.r.Read(p)
		if err != ErrStarveEOF {
			continue
		}

		select {
		case <-sr.starveAfter():
			return 0, io.ErrUnexpectedEOF
		case <-sr.ctx.Done():
			return 0, io.ErrClosedPipe
		case _, ok := <-sr.stim:
			if !ok {
				return 0, io.ErrClosedPipe
			}

			if !sr.skipDrain {
				// It's possible for stimulus items to pile up, and
				// in order to reduce the number of ineffective network
				// hits, we want to drain the queue.
				empty := false
				for !empty {
					select {
					case _, ok := <-sr.stim:
						if !ok {
							return 0, io.ErrClosedPipe
						}
					default:
						// There's no further stimulus waiting
						empty = true
					}
				}
			}
			// A positive stimululs tells us to pass through the loop again
			continue
		}
	}
	return n, err
}

type receiver interface {
	Receive() interface{}
}

func (sr *SyncReader) pubSubStimulus(stim chan<- struct{}, rec receiver) {
	defer close(stim)
	defer sr.wg.Done()
	for sr.ctx.Err() == nil {
		switch rec.Receive().(type) {
		case redis.Message:
			select {
			case stim <- struct{}{}:
				// pass
			case <-sr.ctx.Done():
				return
			}
		// case redis.Subscription:
		// 	// I don't think we care about this
		//     if n.Count == 0 {
		//         return
		//     }
		case error:
			return
		}
	}
}
