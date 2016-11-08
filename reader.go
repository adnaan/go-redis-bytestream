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

type rbsReader struct {
	quit bool
	conn redis.Conn
	name string
	next uint16
	last uint16
	ends bool
	peek uint8
}

// // LookaheadReader creates a reader that will attempt to reduce number of
// // `Read`s (and therefore network hits & Redis utilization) by
// // optimistically fetching multiple chunks
// // Receiving a `io.ErrShortBuffer` is not necessarily a fatal condition: if
// // it was caused by the cumulative size of peeks, a subsequent `Read` might
// // succeed; if any single chunk is greater in size than the buffer, then the
// // only hope is to use a larger buffer to succeed or progress along
// // the stream.
// func LookaheadReader(conn redis.Conn, name string, peek uint8) io.ReadCloser {
// 	return &rbsReader{conn: conn, name: name, peek: peek}
// }

// NewReader TODO: Needs description!
func NewReader(conn redis.Conn, name string, options ...func(*rbsReader)) io.ReadCloser {
	r := &rbsReader{conn: conn, name: name}
	for _, option := range options {
		option(r)
	}
	return r
}

// Lookahead sets the number of chunks to optimistically look for when
// hitting the network doing a single fetch from Redis
func Lookahead(peek uint8) func(*rbsReader) {
	return func(r *rbsReader) {
		r.peek = peek
	}
}

func (rr *rbsReader) Close() error {
	rr.conn.Close()
	rr.quit = true
	return nil
}

func (rr *rbsReader) Read(p []byte) (int, error) {
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
		// (This may not be fatal, if `peek` is non-zero.)
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
		// At the exhortation of the `io.Reader` interface, we should
		// NOT send `0, nil`... We should return some signature of WHY we
		// are returning with size zero.
		return size, ErrStarveEOF
	}

	return size, nil
}

type syncReader struct {
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

// SyncReader TODO: more
func SyncReader(
	parent context.Context,
	r io.Reader,
	subs redis.Conn,
	options ...func(*syncReader) error,
) (io.ReadCloser, error) {
	ctx, cxl := context.WithCancel(parent)
	stim := make(chan struct{})
	result := &syncReader{
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

// Subscribe to an arbitrary Redis PubSub channel for `Read(...)` stimulus
func Subscribe(pubSubChan interface{}) func(*syncReader) error {
	return func(sr *syncReader) error {
		sr.channels = append(sr.channels, pubSubChan)
		return nil
	}
}

// StdSync sets up the subscription for on the package "standard" channel
func StdSync() func(*syncReader) error {
	return func(sr *syncReader) error {
		r, ok := sr.r.(*rbsReader)
		if !ok {
			return fmt.Errorf("Expected `*rbs.NewReader`: %T", sr.r)
		}
		sr.channels = append(sr.channels, fmt.Sprintf(syncChannelPattern, r.name))
		return nil
	}
}

// Starve sets the maximum amount of time to wait between stimulus, and
// if tripped, the reader will return `io.ErrUnexpectedEOF`
func Starve(dur time.Duration) func(*syncReader) error {
	return func(sr *syncReader) error {
		sr.starveDur = dur
		return nil
	}
}

func (sr *syncReader) Close() error {
	sr.cxl()
	for _, closer := range sr.closers {
		closer.Close()
	}
	sr.wg.Wait()
	return nil
}

func (sr *syncReader) starveAfter() <-chan time.Time {
	if sr.starveDur == 0 {
		return nil
	}
	return time.After(sr.starveDur)
}

func (sr *syncReader) Read(p []byte) (n int, err error) {
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

func (sr *syncReader) pubSubStimulus(stim chan<- struct{}, rec receiver) {
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
