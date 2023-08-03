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
	for {
		head, tail, closed := b.waitBeforeReading(uint32(n))
		if closed {
			return nTotal, nil
		}

		var err error
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
	for {
		head, tail, closed := b.waitBeforeWriting(uint32(n))
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
	for {
		head, tail, closed := b.waitBeforeWriting(uint32(n))
		if closed {
			return nTotal, errors.Wrap(io.ErrClosedPipe, "writing to closed ring buffer")
		}

		var err error
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

func (b *Buffer) waitBeforeReading(n uint32) (uint16, uint16, bool) {
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

func (b *Buffer) waitBeforeWriting(n uint32) (uint16, uint16, bool) {
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
	b.full = false

	b.condSpace.Signal()
}

func (b *Buffer) updatePointersAfterWriting(n uint16, lock bool) {
	if lock {
		b.mu.Lock()
		defer b.mu.Unlock()
	}

	b.tail += n
	b.full = b.head == b.tail
	b.empty = false

	b.condData.Signal()
}
