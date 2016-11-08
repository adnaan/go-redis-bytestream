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

// A CmdBuilder spits out the parameters that are used for invocation
// of a Redis command
type CmdBuilder interface {
	Build() (string, []interface{}, error)
}

// CmdBuilderFunc is a function adapter for the `CmdBuilder` interface
type CmdBuilderFunc func() (string, []interface{}, error)

// Build conforms to the `CmdBuilder` interface
func (f CmdBuilderFunc) Build() (string, []interface{}, error) {
	return f()
}

type rbsWriter struct {
	quit     bool
	conn     redis.Conn
	name     string
	next     uint16
	min      uint16
	max      uint16
	carry    *bytes.Buffer
	trailers []CmdBuilder
}

// NewWriter creates the object that will write byte chunks to Redis
func NewWriter(conn redis.Conn, name string, options ...func(*rbsWriter) error) (io.WriteCloser, error) {
	w := &rbsWriter{
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

// MaxChunkSize overrides the default maximum size per chunk written to Redis
func MaxChunkSize(max uint16) func(*rbsWriter) error {
	return func(rw *rbsWriter) error {
		if rw.min > max {
			return fmt.Errorf("size conflict: %d > %d", rw.min, max)
		}
		rw.max = max
		rw.carry = bytes.NewBuffer(make([]byte, 0, max))
		return nil
	}
}

// MinChunkSize sets the minimums size per chunk written to Redis
func MinChunkSize(min uint16) func(*rbsWriter) error {
	return func(rw *rbsWriter) error {
		if min > rw.max {
			return fmt.Errorf("size conflict: %d > %d", min, rw.max)
		}
		rw.min = min
		return nil
	}
}

// Trailer provides a way to send arbitrary Redis commands per write
func Trailer(c CmdBuilder) func(*rbsWriter) error {
	return func(rw *rbsWriter) error {
		rw.trailers = append(rw.trailers, c)
		return nil
	}
}

// Expires sets the TTL for the underlying hash
func Expires(sec uint16) func(*rbsWriter) error {
	return func(rw *rbsWriter) error {
		rw.trailers = append(
			rw.trailers,
			CmdBuilderFunc(func() (string, []interface{}, error) {
				return "EXPIRE", []interface{}{rw.name, sec}, nil
			}),
		)
		return nil
	}
}

// Publish sends a pub/sub publish every time we write out
func Publish() func(*rbsWriter) error {
	return func(rw *rbsWriter) error {
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

func (rw *rbsWriter) writeChunk(b []byte) error {
	chunk := fmt.Sprintf(chunkNumber, rw.next)
	err := rw.conn.Send("HSET", rw.name, chunk, b)
	if err != nil {
		return err
	}
	rw.next++
	return nil
}

func (rw *rbsWriter) flush() error {
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

// Close is important to write out any leftover bytes, and to add the
// marker for the `last` index
func (rw *rbsWriter) Close() error {
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

func (rw *rbsWriter) Write(p []byte) (int, error) {
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
