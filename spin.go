package spin

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
	head, tail, closed := b.waitBeforeReading(0)
	if closed {
		return 0, errors.Wrap(io.EOF, "ring buffer has been closed and there is no more data to read")
	}

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

	b.updatePointersAfterReading(uint16(n), true)

	return n, nil
}

// WriteTo copies the content of the ring buffer to the provided writer.
func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	var nTotal int64
	var n int
	var head, tail uint16
	var closed bool
	var err error
	for {
		head, tail, closed = b.waitBeforeReading(n)
		if closed {
			return nTotal, nil
		}

		if head < tail {
			n, err = w.Write(b.buf[head:tail])
		} else {
			n, err = w.Write(b.buf[head:])
		}

		nTotal += int64(n)
		if err != nil {
			return nTotal, errors.WithStack(err)
		}
	}
}

// Write writes bytes to the ring buffer.
func (b *Buffer) Write(p []byte) (int, error) {
	var nTotal int
	var n int
	var head, tail uint16
	var closed bool
	for {
		head, tail, closed = b.waitBeforeWriting(n)
		if closed {
			return nTotal, errors.Wrap(io.ErrClosedPipe, "writing to closed ring buffer")
		}

		if len(p) == 0 {
			return 0, nil
		}

		if tail >= head {
			n = copy(b.buf[tail:], p)
		} else {
			n = copy(b.buf[tail:head], p)
		}

		nTotal += n
		if n == len(p) {
			b.updatePointersAfterWriting(uint16(n), true)
			return nTotal, nil
		}

		p = p[n:]
	}
}

// ReadFrom copies the content of the provided reader to the ring buffer.
func (b *Buffer) ReadFrom(r io.Reader) (int64, error) {
	var nTotal int64
	var n int
	var head, tail uint16
	var closed bool
	var err error
	for {
		head, tail, closed = b.waitBeforeWriting(n)
		if closed {
			return nTotal, errors.Wrap(io.ErrClosedPipe, "writing to closed ring buffer")
		}

		if tail >= head {
			n, err = r.Read(b.buf[tail:])
		} else {
			n, err = r.Read(b.buf[tail:head])
		}

		nTotal += int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nTotal, nil
			}
			return nTotal, errors.WithStack(err)
		}
	}
}

// ReadByte reads one byte from the buffer.
func (b *Buffer) ReadByte() (byte, error) {
	b.condData.L.Lock()
	defer b.condData.L.Unlock()

	for {
		if !b.empty {
			v := b.buf[b.head]
			b.updatePointersAfterReading(1, false)
			return v, nil
		}

		if b.closed {
			return 0x00, errors.Wrap(io.EOF, "ring buffer has been closed and there is no more data to read")
		}

		b.condData.Wait()
	}
}

// WriteByte writes one byte to the buffer.
func (b *Buffer) WriteByte(v byte) error {
	b.condSpace.L.Lock()
	defer b.condSpace.L.Unlock()

	for {
		if b.closed {
			return errors.Wrap(io.ErrClosedPipe, "writing to closed ring buffer")
		}

		if !b.full {
			b.buf[b.tail] = v
			b.updatePointersAfterWriting(1, false)
			return nil
		}

		b.condSpace.Wait()
	}
}

// Iterate implements iterator over available data.
// It makes all the available data to the iterator, then it should say how many bytes has been used
// and if iterator is interested in receiving more data.
func (b *Buffer) Iterate(f func(p []byte) (int, bool, error)) (int64, error) {
	var nTotal int64
	var n int
	var head, tail uint16
	var more, closed bool
	var err error
	for {
		head, tail, closed = b.waitBeforeReading(n)
		if closed {
			return nTotal, io.EOF
		}

		if head < tail {
			n, more, err = f(b.buf[head:tail])
		} else {
			n, more, err = f(b.buf[head:])
		}

		nTotal += int64(n)
		if err != nil {
			return nTotal, errors.WithStack(err)
		}
		if !more {
			if n > 0 {
				b.updatePointersAfterReading(uint16(n), true)
			}
			return nTotal, nil
		}
	}
}

func (b *Buffer) waitBeforeReading(n int) (uint16, uint16, bool) {
	b.condData.L.Lock()
	defer b.condData.L.Unlock()

	if n > 0 {
		b.updatePointersAfterReading(uint16(n), false)
	}

	for {
		if !b.empty {
			return b.head, b.tail, false
		}

		if b.closed {
			return 0, 0, true
		}

		b.condData.Wait()
	}
}

func (b *Buffer) waitBeforeWriting(n int) (uint16, uint16, bool) {
	b.condSpace.L.Lock()
	defer b.condSpace.L.Unlock()

	if n > 0 {
		b.updatePointersAfterWriting(uint16(n), false)
	}

	for {
		if b.closed {
			return 0, 0, true
		}

		if !b.full {
			return b.head, b.tail, false
		}

		b.condSpace.Wait()
	}
}

func (b *Buffer) updatePointersAfterReading(n uint16, lock bool) {
	if lock {
		b.mu.Lock()
		defer b.mu.Unlock()
	}

	b.head += n
	b.empty = b.head == b.tail

	if b.full {
		b.full = false
		b.condSpace.Signal()
	}
}

func (b *Buffer) updatePointersAfterWriting(n uint16, lock bool) {
	if lock {
		b.mu.Lock()
		defer b.mu.Unlock()
	}

	b.tail += n
	b.full = b.head == b.tail

	if b.empty {
		b.empty = false
		b.condData.Signal()
	}
}
