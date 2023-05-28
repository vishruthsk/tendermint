//
// Written by Maxim Khitrov (November 2012)
//

package flowrate

import (
	"errors"
	"io"

	proto1 "github.com/cosmos/gogoproto/proto"
)

// ErrLimit is returned by the Writer when a non-blocking write is short due to
// the transfer rate limit.
var ErrLimit = errors.New("flowrate: flow rate limit exceeded")

// Limiter is implemented by the Reader and Writer to provide a consistent
// interface for monitoring and controlling data transfer.
type Limiter interface {
	Done() int64
	Status() Status
	SetTransferSize(bytes int64)
	SetLimit(new int64) (old int64)
	SetBlocking(new bool) (old bool)
}

// Reader implements io.ReadCloser with a restriction on the rate of data
// transfer.
type Reader struct {
	io.Reader // Data source
	*Monitor  // Flow control monitor

	limit int64 // Rate limit in bytes per second (unlimited when <= 0)
	block bool  // What to do when no new bytes can be read due to the limit
}

// NewReader restricts all Read operations on r to limit bytes per second.
func NewReader(r io.Reader, limit int64) *Reader {
	return &Reader{r, New(0, 0), limit, true}
}

// Read reads up to len(p) bytes into p without exceeding the current transfer
// rate limit. It returns (0, nil) immediately if r is non-blocking and no new
// bytes can be read at this time.
func (r *Reader) Read(p []byte) (n int, err error) {
	p = p[:r.Limit(len(p), r.limit, r.block)]
	if len(p) > 0 {
		n, err = r.IO(r.Reader.Read(p))
	}
	return
}

// SetLimit changes the transfer rate limit to new bytes per second and returns
// the previous setting.
func (r *Reader) SetLimit(new int64) (old int64) {
	old, r.limit = r.limit, new
	return
}

// SetBlocking changes the blocking behavior and returns the previous setting. A
// Read call on a non-blocking reader returns immediately if no additional bytes
// may be read at this time due to the rate limit.
func (r *Reader) SetBlocking(new bool) (old bool) {
	old, r.block = r.block, new
	return
}

// Close closes the underlying reader if it implements the io.Closer interface.
func (r *Reader) Close() error {
	defer r.Done()
	if c, ok := r.Reader.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Writer implements io.WriteCloser with a restriction on the rate of data
// transfer.
type Writer struct {
	io.Writer // Data destination
	*Monitor  // Flow control monitor

	limit int64 // Rate limit in bytes per second (unlimited when <= 0)
	block bool  // What to do when no new bytes can be written due to the limit
}

// NewWriter restricts all Write operations on w to limit bytes per second. The
// transfer rate and the default blocking behavior (true) can be changed
// directly on the returned *Writer.
func NewWriter(w io.Writer, limit int64) *Writer {
	return &Writer{w, New(0, 0), limit, true}
}

// Write writes len(p) bytes from p to the underlying data stream without
// exceeding the current transfer rate limit. It returns (n, ErrLimit) if w is
// non-blocking and no additional bytes can be written at this time.
func (w *Writer) Write(p []byte) (n int, err error) {
	var c int
	for len(p) > 0 && err == nil {
		s := p[:w.Limit(len(p), w.limit, w.block)]
		if len(s) > 0 {
			c, err = w.IO(w.Writer.Write(s))
		} else {
			return n, ErrLimit
		}
		p = p[c:]
		n += c
	}
	return
}

// SetLimit changes the transfer rate limit to new bytes per second and returns
// the previous setting.
func (w *Writer) SetLimit(new int64) (old int64) {
	old, w.limit = w.limit, new
	return
}

// SetBlocking changes the blocking behavior and returns the previous setting. A
// Write call on a non-blocking writer returns as soon as no additional bytes
// may be written at this time due to the rate limit.
func (w *Writer) SetBlocking(new bool) (old bool) {
	old, w.block = w.block, new
	return
}

// Close closes the underlying writer if it implements the io.Closer interface.
func (w *Writer) Close() error {
	defer w.Done()
	if c, ok := w.Writer.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

type Writer1 interface {
	WriteMsg(proto1.Message) (int, error)
}

type WriteCloser interface {
	Writer1
	io.Closer
}

type Reader1 interface {
	ReadMsg(msg proto1.Message) (int, error)
}

type ReadCloser interface {
	Reader1
	io.Closer
}

type marshaler interface {
	MarshalTo(data []byte) (n int, err error)
}

func getSize(v interface{}) (int, bool) {
	if sz, ok := v.(interface {
		Size() (n int)
	}); ok {
		return sz.Size(), true
	} else if sz, ok := v.(interface {
		ProtoSize() (n int)
	}); ok {
		return sz.ProtoSize(), true
	} else {
		return 0, false
	}
}

// byteReader wraps an io.Reader and implements io.ByteReader, required by
// binary.ReadUvarint(). Reading one byte at a time is extremely slow, but this
// is what Amino did previously anyway, and the caller can wrap the underlying
// reader in a bufio.Reader if appropriate.
type byteReader struct {
	reader    io.Reader
	buf       []byte
	bytesRead int // keeps track of bytes read via ReadByte()
}

func newByteReader(r io.Reader) *byteReader {
	return &byteReader{
		reader: r,
		buf:    make([]byte, 1),
	}
}

func (r *byteReader) ReadByte() (byte, error) {
	n, err := r.reader.Read(r.buf)
	r.bytesRead += n
	if err != nil {
		return 0x00, err
	}
	return r.buf[0], nil
}
