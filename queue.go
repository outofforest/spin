package spin

import (
	"io"
	"math"
	"sync"

	"github.com/pkg/errors"
)

// NewQueue creates a new ring queue.
func NewQueue[T any]() *Queue[T] {
	b := &Queue[T]{
		buf:   make([]T, math.MaxUint16+1),
		empty: true,
	}
	b.condData = sync.NewCond(&b.mu)
	b.condSpace = sync.NewCond(&b.mu)
	return b
}

// Queue is the ring queue implementation.
type Queue[T any] struct {
	buf []T

	mu          sync.Mutex
	condData    *sync.Cond
	condSpace   *sync.Cond
	head, tail  uint16
	empty, full bool
	closed      bool
}

// Dequeue returns item from the queue.
func (q *Queue[T]) Dequeue() (T, error) {
	q.condData.L.Lock()
	defer q.condData.L.Unlock()

	for {
		if !q.empty {
			v := q.buf[q.head]
			q.updatePointersAfterReading()
			return v, nil
		}

		if q.closed {
			return *new(T), errors.Wrap(io.EOF, "ring queue has been closed and there is no more items to read")
		}

		q.condData.Wait()
	}
}

// Enqueue adds item to the queue.
func (q *Queue[T]) Enqueue(v T) error {
	q.condSpace.L.Lock()
	defer q.condSpace.L.Unlock()

	for {
		if q.closed {
			return errors.Wrap(io.ErrClosedPipe, "writing to closed ring queue")
		}

		if !q.full {
			q.buf[q.tail] = v
			q.updatePointersAfterWriting()
			return nil
		}

		q.condSpace.Wait()
	}
}

// Close closes the queue.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	q.condData.Signal()
	q.condSpace.Signal()
}

func (q *Queue[T]) updatePointersAfterReading() {
	q.head++
	q.empty = q.head == q.tail

	if q.full {
		q.full = false
		q.condSpace.Signal()
	}
}

func (q *Queue[T]) updatePointersAfterWriting() {
	q.tail++
	q.full = q.head == q.tail

	if q.empty {
		q.empty = false
		q.condData.Signal()
	}
}
