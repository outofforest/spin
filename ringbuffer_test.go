package ringbuffer

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFullWriteAndRead(t *testing.T) {
	requireT := require.New(t)

	data := make([]byte, math.MaxUint16)
	n, err := rand.Read(data)
	requireT.NoError(err)
	requireT.Equal(math.MaxUint16, n)

	ring := New()
	n, err = ring.Write(data)
	requireT.NoError(err)
	requireT.Equal(math.MaxUint16, n)

	data2 := make([]byte, math.MaxUint16)
	n, err = ring.Read(data2)
	requireT.NoError(err)
	requireT.Equal(math.MaxUint16, n)

	requireT.Equal(data, data2)
}

func TestPartialWriteAndRead(t *testing.T) {
	const loops = 1000
	const batchSize = math.MaxUint16 / 3

	requireT := require.New(t)

	data := make([]byte, 0, loops*batchSize)
	data2 := make([]byte, 0, loops*batchSize)
	in := make([]byte, batchSize)
	out := make([]byte, batchSize)

	n, err := rand.Read(in)
	requireT.NoError(err)
	requireT.Equal(len(in), n)

	data = append(data, in...)

	ring := New()
	n, err = ring.Write(in)
	requireT.NoError(err)
	requireT.Equal(len(in), n)

	t.Logf("Write, head: %d, tail: %d", ring.head, ring.tail)

	for i := 1; i < loops; i++ {
		n, err := rand.Read(in)
		requireT.NoError(err)
		requireT.Equal(len(in), n)

		data = append(data, in...)

		n, err = ring.Write(in)
		requireT.NoError(err)
		requireT.Equal(len(in), n)

		t.Logf("Write, head: %d, tail: %d", ring.head, ring.tail)

		n, err = ring.Read(out)
		requireT.NoError(err)
		requireT.Equal(len(out), n)

		t.Logf("Read, head: %d, tail: %d", ring.head, ring.tail)

		data2 = append(data2, out...)
	}

	n, err = ring.Read(out)
	requireT.NoError(err)
	requireT.Equal(len(out), n)

	t.Logf("Read, head: %d, tail: %d", ring.head, ring.tail)

	data2 = append(data2, out...)

	requireT.Equal(data, data2)
}

type reader struct {
	data      []byte
	batchSize int
}

func (r *reader) Read(d []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}

	var n int
	if len(r.data) > r.batchSize {
		n = copy(d, r.data[:r.batchSize])
	} else {
		n = copy(d, r.data)
	}
	if n == len(r.data) {
		r.data = nil
	} else {
		r.data = r.data[n:]
	}
	return n, nil
}

func TestReadFrom(t *testing.T) {
	const capacity = 100 * math.MaxUint16
	const readBatchSize = math.MaxUint16 / 5
	const batchSize = math.MaxUint16 / 3

	requireT := require.New(t)

	data := make([]byte, capacity)

	_, err := rand.Read(data)
	requireT.NoError(err)

	ring := New()

	errCh := make(chan error, 1)
	data2 := make([]byte, 0, capacity)
	go func() {
		buff := make([]byte, readBatchSize)
		for {
			n, err := ring.Read(buff)
			if n > 0 {
				data2 = append(data2, buff[:n]...)
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	n, err := ring.ReadFrom(&reader{
		data:      data,
		batchSize: batchSize,
	})
	requireT.NoError(err)
	requireT.Equal(int64(len(data)), n)

	requireT.NoError(ring.Close())
	err = <-errCh

	requireT.ErrorIs(err, io.EOF)
	requireT.Equal(data, data2)
}

func TestWriteTo(t *testing.T) {
	const loops = 500
	const batchSize = math.MaxUint16 / 5

	requireT := require.New(t)

	ring := New()

	errCh := make(chan error, 1)
	data := make([]byte, 0, loops*batchSize)
	go func() {
		defer func() {
			select {
			case errCh <- ring.Close():
			default:
			}
		}()

		buff := make([]byte, batchSize)
		for i := 0; i < loops; i++ {
			_, err := rand.Read(buff)
			if err != nil {
				errCh <- err
				return
			}
			n, err := ring.Write(buff)
			if n > 0 {
				data = append(data, buff[:n]...)
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	buff := &bytes.Buffer{}
	n, err := ring.WriteTo(buff)
	requireT.NoError(err)
	requireT.Equal(int64(buff.Len()), n)

	err = <-errCh

	requireT.NoError(err)
	requireT.Equal(data, buff.Bytes())
}

func TestClosedRead(t *testing.T) {
	requireT := require.New(t)

	ring := New()
	requireT.NoError(ring.Close())
	n, err := ring.Read(make([]byte, 1))
	requireT.ErrorIs(err, io.EOF)
	requireT.Equal(0, n)

	ring = New()
	requireT.NoError(ring.Close())
	n, err = ring.Read(nil)
	requireT.ErrorIs(err, io.EOF)
	requireT.Equal(0, n)
}

func TestClosedWriteTo(t *testing.T) {
	requireT := require.New(t)

	buf := &bytes.Buffer{}

	ring := New()
	requireT.NoError(ring.Close())
	n, err := ring.WriteTo(buf)
	requireT.NoError(err)
	requireT.Equal(int64(0), n)
}

func TestClosedWrite(t *testing.T) {
	requireT := require.New(t)

	ring := New()
	requireT.NoError(ring.Close())
	n, err := ring.Write(make([]byte, 1))
	requireT.ErrorIs(err, io.ErrClosedPipe)
	requireT.Equal(0, n)

	ring = New()
	requireT.NoError(ring.Close())
	n, err = ring.Write(nil)
	requireT.ErrorIs(err, io.ErrClosedPipe)
	requireT.Equal(0, n)
}

func TestClosedReadFrom(t *testing.T) {
	requireT := require.New(t)

	buf := bytes.NewBuffer(make([]byte, 1))

	ring := New()
	requireT.NoError(ring.Close())
	n, err := ring.ReadFrom(buf)
	requireT.ErrorIs(err, io.ErrClosedPipe)
	requireT.Equal(int64(0), n)
}

func TestReadToEmpty(t *testing.T) {
	requireT := require.New(t)

	ring := New()
	n, err := ring.Write(make([]byte, 1))
	requireT.NoError(err)
	requireT.Equal(1, n)

	n, err = ring.Read(nil)
	requireT.NoError(err)
	requireT.Equal(0, n)
}

func TestWriteFromEmpty(t *testing.T) {
	requireT := require.New(t)

	ring := New()
	n, err := ring.Write(nil)
	requireT.NoError(err)
	requireT.Equal(0, n)
}

var errTest = errors.New("test")

type errReaderWriter struct{}

func (rw errReaderWriter) Read(_ []byte) (int, error) {
	return 10, errTest
}

func (rw errReaderWriter) Write(_ []byte) (int, error) {
	return 10, errTest
}

func TestReadFromWithError(t *testing.T) {
	requireT := require.New(t)

	ring := New()
	n, err := ring.ReadFrom(errReaderWriter{})
	requireT.ErrorIs(err, errTest)
	requireT.Equal(int64(10), n)
}

func TestWriteToWithError(t *testing.T) {
	requireT := require.New(t)

	ring := New()
	n, err := ring.Write(make([]byte, 20))
	requireT.NoError(err)
	requireT.Equal(20, n)

	n2, err := ring.WriteTo(errReaderWriter{})
	requireT.ErrorIs(err, errTest)
	requireT.Equal(int64(10), n2)
}
