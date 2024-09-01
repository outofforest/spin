package spin

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFullWriteAndRead(t *testing.T) {
	requireT := require.New(t)

	data := make([]byte, math.MaxUint16)
	n, err := rand.Read(data)
	requireT.NoError(err)
	requireT.Equal(math.MaxUint16, n)

	ring := NewBuffer()
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

	ring := NewBuffer()
	n, err = ring.Write(in)
	requireT.NoError(err)
	requireT.Equal(len(in), n)

	t.Logf("Write, head: %d, tail: %d", ring.head, ring.tail)

	for range loops {
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
	const capacity = 2 * 1024 * math.MaxUint16
	const readBatchSize = math.MaxUint16 / 5
	const batchSize = math.MaxUint16 / 3

	requireT := require.New(t)

	data := make([]byte, capacity)

	_, err := rand.Read(data)
	requireT.NoError(err)

	ring := NewBuffer()

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
	const loops = 8000
	const batchSize = math.MaxUint16 / 5

	requireT := require.New(t)

	ring := NewBuffer()

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
		for range loops {
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

func TestIterateFull(t *testing.T) {
	const loops = 8000
	const batchSize = math.MaxUint16 / 5

	requireT := require.New(t)

	data := make([]byte, loops*batchSize)
	_, err := rand.Read(data)
	requireT.NoError(err)

	result := make([]byte, 0, len(data))

	ring := NewBuffer()

	errCh := make(chan error, 1)
	var n int64
	go func() {
		var err error
		n, err = ring.Iterate(func(b []byte) (int, bool, error) {
			result = append(result, b...)
			return len(b), true, nil
		})
		errCh <- err
	}()

	n2, err := ring.Write(data)
	requireT.NoError(err)
	requireT.Equal(len(data), n2)
	requireT.NoError(ring.Close())

	err = <-errCh

	requireT.ErrorIs(err, io.EOF)
	requireT.Equal(int64(len(data)), n)
	requireT.Equal(data, result)
}

func TestIteratePartial(t *testing.T) {
	const loops = 8000
	const batchSize = math.MaxUint16 / 5

	requireT := require.New(t)

	data := make([]byte, 0, loops*batchSize)
	result := make([]byte, 0, loops*batchSize)

	ring := NewBuffer()
	buf := make([]byte, batchSize)
	for range loops {
		_, err := rand.Read(buf)
		requireT.NoError(err)

		_, err = ring.Write(buf)
		requireT.NoError(err)

		data = append(data, buf...)

		n, err := ring.Iterate(func(p []byte) (int, bool, error) {
			result = append(result, p[0])
			return 1, false, nil
		})
		requireT.NoError(err)
		requireT.Equal(int64(1), n)

		n, err = ring.Iterate(func(p []byte) (int, bool, error) {
			result = append(result, p...)
			return len(p), false, nil
		})
		requireT.NoError(err)
		if n < int64(len(buf)-1) {
			n2, err := ring.Iterate(func(p []byte) (int, bool, error) {
				result = append(result, p...)
				return len(p), false, nil
			})
			requireT.NoError(err)
			n += n2
		}
		requireT.Equal(int64(len(buf)-1), n)
	}

	requireT.NoError(ring.Close())
	requireT.Equal(data, result)
}

func TestReadByte(t *testing.T) {
	const loop = math.MaxUint16

	requireT := require.New(t)

	data := make([]byte, 3)
	ring := NewBuffer()
	for range loop {
		_, err := rand.Read(data)
		requireT.NoError(err)

		n, err := ring.Write(data)
		requireT.NoError(err)
		requireT.Equal(len(data), n)

		b, err := ring.ReadByte()
		requireT.NoError(err)
		requireT.Equal(data[0], b)

		b, err = ring.ReadByte()
		requireT.NoError(err)
		requireT.Equal(data[1], b)

		b, err = ring.ReadByte()
		requireT.NoError(err)
		requireT.Equal(data[2], b)
	}
}

func TestWriteByte(t *testing.T) {
	const loop = math.MaxUint16

	requireT := require.New(t)

	data := make([]byte, 3)
	result := make([]byte, 3)
	ring := NewBuffer()
	for range loop {
		_, err := rand.Read(data)
		requireT.NoError(err)

		requireT.NoError(ring.WriteByte(data[0]))
		requireT.NoError(ring.WriteByte(data[1]))
		requireT.NoError(ring.WriteByte(data[2]))

		n, err := ring.Read(result)
		requireT.NoError(err)
		requireT.Equal(len(data), n)

		requireT.Equal(data, result)
	}
}

func TestSlowReadByte(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
	_, err := ring.Write(make([]byte, math.MaxUint16+1))
	requireT.NoError(err)

	data := make([]byte, 10)
	_, err = rand.Read(data)
	requireT.NoError(err)

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)

		for range len(data) {
			time.Sleep(10 * time.Millisecond)
			_, err := ring.ReadByte()
			if err != nil {
				panic(err)
			}
		}
	}()

	for _, b := range data {
		requireT.NoError(ring.WriteByte(b))
	}
	requireT.NoError(ring.Close())

	<-doneCh
}

func TestSlowWriteByte(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()

	data := make([]byte, 10)
	_, err := rand.Read(data)
	requireT.NoError(err)

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)

		for {
			_, err := ring.ReadByte()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				panic(err)
			}
		}
	}()

	for _, b := range data {
		time.Sleep(10 * time.Millisecond)
		requireT.NoError(ring.WriteByte(b))
	}
	requireT.NoError(ring.Close())

	<-doneCh
}

func TestClosedRead(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
	requireT.NoError(ring.Close())
	n, err := ring.Read(make([]byte, 1))
	requireT.ErrorIs(err, io.EOF)
	requireT.Equal(0, n)

	ring = NewBuffer()
	requireT.NoError(ring.Close())
	n, err = ring.Read(nil)
	requireT.ErrorIs(err, io.EOF)
	requireT.Equal(0, n)
}

func TestClosedWriteTo(t *testing.T) {
	requireT := require.New(t)

	buf := &bytes.Buffer{}

	ring := NewBuffer()
	requireT.NoError(ring.Close())
	n, err := ring.WriteTo(buf)
	requireT.NoError(err)
	requireT.Equal(int64(0), n)
}

func TestClosedWrite(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
	requireT.NoError(ring.Close())
	n, err := ring.Write(make([]byte, 1))
	requireT.ErrorIs(err, io.ErrClosedPipe)
	requireT.Equal(0, n)

	ring = NewBuffer()
	requireT.NoError(ring.Close())
	n, err = ring.Write(nil)
	requireT.ErrorIs(err, io.ErrClosedPipe)
	requireT.Equal(0, n)
}

func TestClosedReadFrom(t *testing.T) {
	requireT := require.New(t)

	buf := bytes.NewBuffer(make([]byte, 1))

	ring := NewBuffer()
	requireT.NoError(ring.Close())
	n, err := ring.ReadFrom(buf)
	requireT.ErrorIs(err, io.ErrClosedPipe)
	requireT.Equal(int64(0), n)
}

func TestClosedReadByte(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
	requireT.NoError(ring.Close())
	v, err := ring.ReadByte()
	requireT.ErrorIs(err, io.EOF)
	requireT.Equal(byte(0x00), v)
}

func TestClosedWriteByte(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
	requireT.NoError(ring.Close())
	err := ring.WriteByte(0x00)
	requireT.ErrorIs(err, io.ErrClosedPipe)
}

func TestClosedIterate(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
	requireT.NoError(ring.Close())
	run := false
	n, err := ring.Iterate(func(b []byte) (int, bool, error) {
		run = true
		return len(b), true, nil
	})
	requireT.ErrorIs(err, io.EOF)
	requireT.False(run)
	requireT.Equal(int64(0), n)
}

func TestReadToEmpty(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
	n, err := ring.Write(make([]byte, 1))
	requireT.NoError(err)
	requireT.Equal(1, n)

	n, err = ring.Read(nil)
	requireT.NoError(err)
	requireT.Equal(0, n)
}

func TestWriteFromEmpty(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
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

	ring := NewBuffer()
	n, err := ring.ReadFrom(errReaderWriter{})
	requireT.ErrorIs(err, errTest)
	requireT.Equal(int64(10), n)
}

func TestWriteToWithError(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
	n, err := ring.Write(make([]byte, 20))
	requireT.NoError(err)
	requireT.Equal(20, n)

	n2, err := ring.WriteTo(errReaderWriter{})
	requireT.ErrorIs(err, errTest)
	requireT.Equal(int64(10), n2)
}

func TestIterateWithError(t *testing.T) {
	requireT := require.New(t)

	ring := NewBuffer()
	_, err := ring.Write([]byte{0x00})
	requireT.NoError(err)

	n, err := ring.Iterate(func(p []byte) (int, bool, error) {
		return 1, true, errTest
	})
	requireT.ErrorIs(err, errTest)
	requireT.Equal(int64(1), n)
}
