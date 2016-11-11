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
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/rafaeljusto/redigomock"
)

var alphabet = "abcdefghijklmnopqrstuvwxyz"

func TestWriterErrors(t *testing.T) {
	c := redigomock.NewConn()
	defer c.Clear()

	w, err := NewWriter(
		c,
		"bubba",
		WriteMaxChunk(5),
		WriteMinChunk(3),
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v\n", err)
	}

	// Prime the pump
	w.Write([]byte(alphabet))
	c.FlushMock = func() error { return fmt.Errorf("forced") }

	// Gonna get an error from the `Flush()`
	n, err := w.Write([]byte(alphabet))
	if err == nil {
		t.Errorf("Expected error")
	}
	if n != 0 {
		t.Errorf("Expected zero")
	}

	// Because we primed the carryover, expect to need to do a flush
	err = w.Close()
	if err == nil {
		t.Errorf("Expected error")
	}

	// Now that we're closed, expect a specific error response
	_, err = w.Write([]byte(alphabet))
	if err != io.ErrClosedPipe {
		t.Errorf("Expected `io.ErrClosedPipe`: %v\n", err)
	}

	// Same expectation from `Close()`
	err = w.Close()
	if err != io.ErrClosedPipe {
		t.Errorf("Expected `io.ErrClosedPipe`: %v\n", err)
	}
}

func TestWriterZeroLength(t *testing.T) {
	c := redigomock.NewConn()
	defer c.Clear()

	// Set up ANY flush (so, any write) to error out
	c.FlushMock = func() error { return fmt.Errorf("forced") }

	w, err := NewWriter(c, "zero")
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}

	// If this closes w/o an error, that means we never tried to write
	// anything out to the underlying Redis
	err = w.Close()
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
}

func TestWriterInitDefault(t *testing.T) {
	c := redigomock.NewConn()
	defer c.Clear()

	// Test default config
	w, err := NewWriter(c, "dflt")
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	defer w.Close()

	base, ok := w.(*RedWriter)
	if !ok {
		t.Errorf("Incorrect type: %T\n", w)
	}

	if base.min != 0 {
		t.Errorf("Unexpected min: %d\n", base.min)
	}
	if base.max != DefaultMaxChunkSize {
		t.Errorf("Unexpected max: %d\n", base.max)
	}
	if base.carry.Cap() != int(DefaultMaxChunkSize) {
		t.Errorf("Unexpected buffer size: %d\n", base.carry.Cap())
	}

	if len(base.trailers) != 0 {
		t.Errorf("Unexpected trailers found: %d\n", len(base.trailers))
	}
}

func TestWriterInitSizeErrs(t *testing.T) {
	c := redigomock.NewConn()
	defer c.Clear()

	// Outsized min
	w, err := NewWriter(c, "min-err", WriteMinChunk(2*DefaultMaxChunkSize))
	if err == nil {
		t.Errorf("Expected error\n")
	}
	if w != nil {
		t.Errorf("Expected nil Writer: %v\n", w)
	}

	// Undersized max
	w, err = NewWriter(
		c, "max-err",
		WriteMinChunk(DefaultMaxChunkSize),
		WriteMaxChunk(DefaultMaxChunkSize/2),
	)
	if err == nil {
		t.Errorf("Expected error\n")
	}
	if w != nil {
		t.Errorf("Expected nil Writer: %v\n", w)
	}

	// Undersized max
	w, err = NewWriter(
		c, "max-err",
		WriteMinChunk(DefaultMaxChunkSize),
		WriteMaxChunk(DefaultMaxChunkSize/2),
	)
	if err == nil {
		t.Errorf("Expected error\n")
	}
	if w != nil {
		t.Errorf("Expected nil Writer: %v\n", w)
	}

	// Happy path: matching sizes should be okay
	w, err = NewWriter(
		c, "min-max",
		WriteMinChunk(DefaultMaxChunkSize/2),
		WriteMaxChunk(DefaultMaxChunkSize/2),
	)
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	defer w.Close()
}

func TestWriterInitTrailers(t *testing.T) {
	c := redigomock.NewConn()
	defer c.Clear()

	now := time.Now().Unix()
	msg := fmt.Sprintf("forced error #%d", now)
	cfn := CmdBuilderFunc(func() (string, []interface{}, error) {
		return "", nil, fmt.Errorf(msg)
	})
	ttl := uint16(100 + (now % 100))

	// The trailer functionalities
	name := fmt.Sprintf("trailers-%d", now)
	w, err := NewWriter(
		c,
		name,
		WriteTrailer(cfn),
		WriteExpire(ttl),
		WriteStdPub(),
	)
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	defer w.Close()

	base, ok := w.(*RedWriter)
	if !ok {
		t.Errorf("Incorrect type: %T\n", w)
	}

	for _, trailer := range base.trailers {
		cmd, data, err := trailer.Build()
		switch cmd {
		case "":
			if err.Error() != msg {
				t.Errorf("Unexpected error: %v\n", err)
			}
		case "EXPIRE":
			if data[0] != name {
				t.Errorf("Unexpected EXPIRE param: %s\n", data[0])
			}
			if data[1] != ttl {
				t.Errorf("Unexpected EXPIRE param: %v\n", data[1])
			}
		case "PUBLISH":
			if data[0] != fmt.Sprintf(syncChannelPattern, name) {
				t.Errorf("Unexpected PUBLISH param: %s\n", data[0])
			}
		default:
			t.Errorf("Unexpected cmd: %s\n", cmd)
		}
	}
}

func readExpect(s string, r io.Reader, b []byte, t *testing.T) {
	n, err := r.Read(b)
	if err != nil {
		if len(s) == 0 && err == ErrStarveEOF {
			// pass
		} else {
			t.Errorf("Unexpected error (%s): %v\n", s, err)
		}
	}
	if n != len(s) {
		t.Errorf("Unexpected length (%s): %d\n", s, n)
	}
	if string(b[:n]) != s {
		t.Errorf("Unexpected content (%s): %s\n", s, b[:n])
	}
}

func TestIntegrationWriterHappyPath(t *testing.T) {
	// For localhost, run this test with:
	//     RBS_TEST_REDIS=localhost:6379
	env := "RBS_TEST_REDIS"
	svr := os.Getenv(env)
	if svr == "" {
		t.Skipf("No environment value found for %s", env)
	}

	tx, err := redis.Dial("tcp", svr)
	if err != nil {
		t.Fatalf("Could not connect: %s - %v\n", svr, err)
	}
	defer tx.Close()

	rx, err := redis.Dial("tcp", svr)
	if err != nil {
		t.Fatalf("Could not connect: %s - %v\n", svr, err)
	}
	defer rx.Close()

	// Figure out what the hash name is, and set up for cleanup
	name := fmt.Sprintf("rbs-happy-%d", time.Now().UnixNano())
	// t.Logf("Name: %s\n", name)
	defer rx.Do("DEL", name)

	buf := make([]byte, 5)
	r := NewReader(rx, name)
	defer r.Close()

	expire := uint16(10)
	w, err := NewWriter(tx, name,
		WriteMaxChunk(5), WriteMinChunk(3), WriteExpire(expire))
	if err != nil {
		t.Fatalf("Unexpected error: %v\n", err)
	}
	defer w.Close()

	// To start with... Reads before any writes just return 0 w/o blocking
	for ix := 0; ix < 3; ix++ {
		readExpect("", r, buf, t)
	}

	// Now, the real action starts!
	n, err := w.Write([]byte(alphabet))
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if n != len(alphabet) {
		t.Errorf("Unexpected length: %d\n", n)
	}

	readExpect("abcde", r, buf, t)
	readExpect("fghij", r, buf, t)
	readExpect("klmno", r, buf, t)
	readExpect("pqrst", r, buf, t)
	readExpect("uvwxy", r, buf, t)

	// With no new data nor an end-of-stream, just expect 0s
	for ix := 0; ix < 3; ix++ {
		readExpect("", r, buf, t)
	}

	// We can also check that the "EXPIRE" was set
	ttl, err := redis.Int(rx.Do("TTL", name))
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if ttl > int(expire) || ttl <= 0 {
		t.Errorf("Unexpected TTL: %d\n", ttl)
	}

	// Write it again!
	n, err = w.Write([]byte(alphabet))
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if n != len(alphabet) {
		t.Errorf("Unexpected length: %d\n", n)
	}

	readExpect("zabcd", r, buf, t)
	readExpect("efghi", r, buf, t)
	readExpect("jklmn", r, buf, t)
	readExpect("opqrs", r, buf, t)
	readExpect("tuvwx", r, buf, t)

	// Again with the 0s
	for ix := 0; ix < 3; ix++ {
		readExpect("", r, buf, t)
	}

	// One more full write... This time triggering a sub-max write
	n, err = w.Write([]byte(alphabet))
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if n != len(alphabet) {
		t.Errorf("Unexpected length: %d\n", n)
	}

	readExpect("yzabc", r, buf, t)
	readExpect("defgh", r, buf, t)
	readExpect("ijklm", r, buf, t)
	readExpect("nopqr", r, buf, t)
	readExpect("stuvw", r, buf, t)

	// This is the roll-over!
	readExpect("xyz", r, buf, t)

	// Even more 0s
	for ix := 0; ix < 3; ix++ {
		readExpect("", r, buf, t)
	}

	// Write just 2 bytes, putting it in the buffer, but not writing out
	final := "AO"
	n, err = w.Write([]byte(final))
	if err != nil {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if n != len(final) {
		t.Errorf("Unexpected length: %d\n", n)
	}
	readExpect("", r, buf, t)

	// Calling close on the writer writes out the carryover bytes,
	// AND it sets the end-of-stream marker
	w.Close()
	readExpect(final, r, buf, t)

	// All subsequent `Read(...)` should result in EOF
	for ix := 0; ix < 3; ix++ {
		n, err = r.Read(buf)
		if err != io.EOF {
			t.Errorf("Expected eof: %v\n", err)
		}
		if n != 0 {
			t.Errorf("Unexpected length: %d\n", n)
		}
	}
}
