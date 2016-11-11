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
	"time"

	"github.com/garyburd/redigo/redis"
)

// DefaultMaxChunkSize is 1kb
const DefaultMaxChunkSize uint16 = 2 ^ 10 // 1024

const syncChannelPattern = "rbs:sync:%s"

// An RedWriter is the implementation that stores a variable-length stream
// of bytes into a Redis hash.
type RedWriter struct {
	quit     bool
	conn     redis.Conn
	name     string
	next     uint16
	min      uint16
	max      uint16
	carry    *bytes.Buffer
	trailers []CmdBuilder
}

// NewWriter creates an io.WriteCloser that will write byte chunks to Redis.
// The underlying implementation is a *RedWriter.
//
// The intendend usage for this is a growing stream of bytes where the full
// length is not known until the stream is closed. (Though, this does not
// preclude the ability to be used for streams of known/fixed length.)
//
// A single Redis hash is the sole underlying data structure. The name parameter
// is used as the Redis key, and each chunk of bytes is stored as a
// field+value pair on the hash. This implementation reserves all field names
// starting with "c:" (for chunk), but is open to clients using any arbitrary
// other field names on this hash.
//
// Network hits are minimized by using Redis pipelining. Per Write
// invocation, the buffer is apportioned into configurable-sized chunks,
// any trailers (arbitrary Redis command configured by the client) are
// assembled, and all of it is written out to Redis at once.
//
// Upon Close, if >0 bytes had been written, a last pipeline is
// constructed, writing out any buffered data, an end-of-stream marker, and
// any configured trailers.
//
// The default configuration has a 1024-byte maximum chunk size, and a 0-byte
// minimum chunk size, and no trailers. Clients will want to adjust these
// parameters to their use cases. (Some factors that may be considered in
// this tuning: desired ingress/egress data rates; Redis tuning; etc.)
//
// Configuration is by functional options, as inspired by this blog post:
// http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
//
// See: WriteExpire, WriteMaxChunk, WriteMinChunk, WriteStdPub, WriteTrailer
func NewWriter(conn redis.Conn, name string, options ...func(*RedWriter) error) (io.WriteCloser, error) {
	w := &RedWriter{
		conn:  conn,
		name:  name,
		max:   DefaultMaxChunkSize,
		carry: bytes.NewBuffer(make([]byte, 0, DefaultMaxChunkSize)),
	}
	for _, option := range options {
		err := option(w)
		if err != nil {
			w.Close()
			return nil, err
		}
	}
	return w, nil
}

// WriteMaxChunk overrides the default maximum size per chunk written to Redis
func WriteMaxChunk(max uint16) func(*RedWriter) error {
	return func(rw *RedWriter) error {
		if rw.min > max {
			return fmt.Errorf("size conflict: %d > %d", rw.min, max)
		}
		rw.max = max
		rw.carry = bytes.NewBuffer(make([]byte, 0, max))
		return nil
	}
}

// WriteMinChunk sets the minimums size per chunk written to Redis
func WriteMinChunk(min uint16) func(*RedWriter) error {
	return func(rw *RedWriter) error {
		if min > rw.max {
			return fmt.Errorf("size conflict: %d > %d", min, rw.max)
		}
		rw.min = min
		return nil
	}
}

// A CmdBuilder spits out the parameters that are used for invocation
// of a Redis command
type CmdBuilder interface {
	Build() (string, []interface{}, error)
}

// CmdBuilderFunc is a function adapter for the CmdBuilder interface
type CmdBuilderFunc func() (string, []interface{}, error)

// Build conforms to the CmdBuilder interface
func (f CmdBuilderFunc) Build() (string, []interface{}, error) {
	return f()
}

// WriteTrailer provides clients a way to configure arbitrary Redis commands
// to be included per pipeline flush
func WriteTrailer(c CmdBuilder) func(*RedWriter) error {
	return func(rw *RedWriter) error {
		rw.trailers = append(rw.trailers, c)
		return nil
	}
}

// WriteExpire sends a Redis EXPIRE command for the underlying hash upon
// each pipeline flush
func WriteExpire(sec uint16) func(*RedWriter) error {
	return func(rw *RedWriter) error {
		rw.trailers = append(
			rw.trailers,
			CmdBuilderFunc(func() (string, []interface{}, error) {
				return "EXPIRE", []interface{}{rw.name, sec}, nil
			}),
		)
		return nil
	}
}

// WriteStdPub sends a standard Redis PUBLISH message for this hash on
// every pipeline flush. This is inteneded to be used in concert with
// a standard read-side subscription for synchronizing when there are
// new bytes to be read.
func WriteStdPub() func(*RedWriter) error {
	return func(rw *RedWriter) error {
		channel := fmt.Sprintf(syncChannelPattern, rw.name)
		rw.trailers = append(
			rw.trailers,
			CmdBuilderFunc(func() (string, []interface{}, error) {
				return "PUBLISH", []interface{}{channel, time.Now().Unix()}, nil
			}),
		)
		return nil
	}
}

func (rw *RedWriter) writeChunk(b []byte) error {
	chunk := fmt.Sprintf(chunkNumber, rw.next)
	err := rw.conn.Send("HSET", rw.name, chunk, b)
	if err != nil {
		return err
	}
	rw.next++
	return nil
}

func (rw *RedWriter) flush() error {
	for _, builder := range rw.trailers {
		cmd, parts, err := builder.Build()
		if err != nil {
			return err
		}
		err = rw.conn.Send(cmd, parts...)
		if err != nil {
			return err
		}
	}
	return rw.conn.Flush()
}

// Close conforms to the io.Closer interface.
//
// It is necessary to write out any buffered (leftover) bytes, and to
// add the end-of-stream marker
//
// This finishes by calling Close on the underlyling Redis connection.
func (rw *RedWriter) Close() error {
	if rw.quit {
		return io.ErrClosedPipe
	}
	rw.quit = true
	defer rw.conn.Close()

	flush := false
	if rw.carry.Len() > 0 {
		err := rw.writeChunk(rw.carry.Bytes())
		if err != nil {
			return err
		}
		flush = true
	}
	if rw.next > 0 {
		body := rw.next - 1
		err := rw.conn.Send("HSET", rw.name, chunkLast, body)
		if err != nil {
			return err
		}
		flush = true
	}
	if flush {
		return rw.flush()
	}

	return nil
}

// Write conforms to the io.Writer interface.
//
// This is where the incoming buffer is apportioned into chunks, and
// written out to Redis via a pipeline.
//
// There should be at most 1 hit to Redis per Write invocation.
// (Only fewer if a chunk is less than the configured minimum chunk size.)
func (rw *RedWriter) Write(p []byte) (int, error) {
	if rw.quit {
		return 0, io.ErrClosedPipe
	}

	offset := 0
	n := len(p)

	flush := false
	for offset < n {
		remains := n - offset
		if remains+rw.carry.Len() < int(rw.min) {
			rw.carry.Write(p[offset:])
			break
		}
		grab := int(rw.max) - rw.carry.Len()
		if grab > remains {
			grab = remains
		}

		c, err := rw.carry.Write(p[offset:(offset + grab)])
		if err != nil {
			return 0, err
		}
		offset += c

		err = rw.writeChunk(rw.carry.Bytes())
		if err != nil {
			return 0, err
		}
		flush = true
		rw.carry.Reset()
	}

	if flush {
		ferr := rw.flush()
		if ferr != nil {
			return 0, ferr
		}
	}

	return n, nil
}
