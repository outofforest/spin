package ringbuffer

import (
	"io"
	"math"
	"sync"

	"github.com/pkg/errors"
)

// Buffer is the ring buffer implementation.
type Buffer struct {
	buf []byte

	mu          sync.Mutex
	condData    *sync.Cond
	condSpace   *sync.Cond
	head, tail  uint16
	empty, full bool
	closed      bool
}

// New creates a new ring buffer.
func New() *Buffer {
	b := &Buffer{
		buf:   make([]byte, math.MaxUint16+1),
		empty: true,
	}
	b.condData = sync.NewCond(&b.mu)
	b.condSpace = sync.NewCond(&b.mu)
	return b
}

// Close closes the buffer.
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true
	b.condData.Signal()
	b.condSpace.Signal()
	return nil
}

// Read reads bytes from the buffer.
func (b *Buffer) Read(p []byte) (int, error) {
	b.condData.L.Lock()
	for {
		if !b.empty {
			break
		}

		if b.closed {
			b.condData.L.Unlock()
			return 0, errors.Wrap(io.EOF, "ring buffer has been closed and there is no more data to read")
		}

		b.condData.Wait()
	}

	head, tail := b.head, b.tail

	b.condData.L.Unlock()

	if len(p) == 0 {
		return 0, nil
	}

	var n int
	if head < tail {
		n = copy(p, b.buf[head:tail])
	} else {
		n = copy(p, b.buf[head:])
		if n < len(p) && tail != 0 {
			n += copy(p[n:], b.buf[:tail])
		}
	}

	b.updatePointersAfterReading(uint16(n))

	return n, nil
}

// WriteTo copies the content of the ring buffer to the provided writer.
func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	var nTotal int64
	for {
		b.condData.L.Lock()
		for {
			if !b.empty {
				break
			}

			if b.closed {
				b.condData.L.Unlock()
				return nTotal, nil
			}

			b.condData.Wait()
		}

		head, tail := b.head, b.tail

		b.condData.L.Unlock()

		var n int
		var err error
		if head < tail {
			n, err = w.Write(b.buf[head:tail])
		} else {
			n, err = w.Write(b.buf[head:])
		}

		if n > 0 {
			nTotal += int64(n)
			b.updatePointersAfterReading(uint16(n))
		}

		if err != nil {
			return nTotal, errors.WithStack(err)
		}
	}
}

// Write writes bytes to the ring buffer.
func (b *Buffer) Write(p []byte) (int, error) {
	var nTotal int
	for {
		b.condSpace.L.Lock()
		for {
			if b.closed {
				b.condSpace.L.Unlock()
				return nTotal, errors.Wrap(io.ErrClosedPipe, "writing to closed ring buffer")
			}

			if !b.full {
				break
			}

			b.condSpace.Wait()
		}

		head, tail := b.head, b.tail

		b.condSpace.L.Unlock()

		if len(p) == 0 {
			return 0, nil
		}

		var n int
		if tail >= head {
			n = copy(b.buf[tail:], p)
		} else {
			n = copy(b.buf[tail:head], p)
		}

		nTotal += n
		b.updatePointersAfterWriting(uint16(n))

		if n == len(p) {
			break
		}

		p = p[n:]
	}

	return nTotal, nil
}

// ReadFrom copies the content of the provided reader to the ring buffer.
func (b *Buffer) ReadFrom(r io.Reader) (int64, error) {
	var nTotal int64
	for {
		b.condSpace.L.Lock()
		for {
			if b.closed {
				b.condSpace.L.Unlock()
				return nTotal, errors.Wrap(io.ErrClosedPipe, "writing to closed ring buffer")
			}

			if !b.full {
				break
			}

			b.condSpace.Wait()
		}

		head, tail := b.head, b.tail

		b.condSpace.L.Unlock()

		var n int
		var err error
		if tail >= head {
			n, err = r.Read(b.buf[tail:])
		} else {
			n, err = r.Read(b.buf[tail:head])
		}

		if n > 0 {
			nTotal += int64(n)
			b.updatePointersAfterWriting(uint16(n))
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return nTotal, nil
			}
			return nTotal, errors.WithStack(err)
		}
	}
}

func (b *Buffer) updatePointersAfterReading(n uint16) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.head += n
	if b.head == b.tail {
		b.empty = true
	}

	b.full = false
	b.condSpace.Signal()
}

func (b *Buffer) updatePointersAfterWriting(n uint16) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.tail += n
	if b.head == b.tail {
		b.full = true
	}

	b.empty = false
	b.condData.Signal()
}
