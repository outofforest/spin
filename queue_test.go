package spin

import (
	"io"
	"math"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestQueueFullEnqueueAndDequeue(t *testing.T) {
	requireT := require.New(t)

	queue := NewQueue[uint64]()

	for i := range uint64(3 * math.MaxUint16) {
		err := queue.Enqueue(i)
		requireT.NoError(err)

		item, err := queue.Dequeue()
		requireT.NoError(err)
		requireT.Equal(i, item)
	}
}

func TestQueueClosed(t *testing.T) {
	requireT := require.New(t)

	queue := NewQueue[uint64]()
	queue.Close()

	requireT.ErrorIs(queue.Enqueue(0), io.ErrClosedPipe)
	_, err := queue.Dequeue()
	requireT.ErrorIs(err, io.EOF)
}

func TestQueueSlowDequeue(t *testing.T) {
	requireT := require.New(t)
	numOfItems := uint64(100)

	queue := NewQueue[uint64]()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)

		for range numOfItems {
			time.Sleep(10 * time.Millisecond)
			_, err := queue.Dequeue()
			if err != nil {
				panic(err)
			}
		}
	}()

	for i := range numOfItems {
		requireT.NoError(queue.Enqueue(i))
	}
	queue.Close()

	<-doneCh
}

func TestQueueSlowEnqueue(t *testing.T) {
	requireT := require.New(t)
	numOfItems := uint64(100)

	queue := NewQueue[uint64]()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)

		for {
			_, err := queue.Dequeue()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				panic(err)
			}
		}
	}()

	for i := range numOfItems {
		time.Sleep(10 * time.Millisecond)
		requireT.NoError(queue.Enqueue(i))
	}
	queue.Close()

	<-doneCh
}
