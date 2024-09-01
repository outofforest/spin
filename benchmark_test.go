package spin

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// go test -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkCopyNative(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const loops = 1000

	for range b.N {
		data := make([]byte, 10*1024) // 10 KiBs of nothing
		result := make([]byte, len(data))

		b.StartTimer()
		for range loops {
			// We copy twice because in ring buffer you copy to ring buffer and then from read buffer
			copy(result, data)
			copy(result, data)
		}
		b.StopTimer()
	}
}

func BenchmarkCopyRing(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const loops = 1000

	for range b.N {
		data := make([]byte, 10*1024) // 10 KiBs of nothing
		result := make([]byte, len(data))

		ring := NewBuffer()

		b.StartTimer()
		for range loops {
			_, _ = ring.Write(data)
			_, _ = ring.Read(result)
		}
		b.StopTimer()
	}
}

func BenchmarkPerByteNative(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const loops = 1000

	for range b.N {
		buf := bytes.NewBuffer(make([]byte, loops))
		mu := sync.Mutex{} // mutex is here to match the fact that ring is a concurrent-safe type

		b.StartTimer()
		for range loops {
			mu.Lock()
			_ = buf.WriteByte(0x00)
			mu.Unlock()

			mu.Lock()
			_, _ = buf.ReadByte()
			mu.Unlock()
		}
		b.StopTimer()
	}
}

func BenchmarkPerByteRing(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const loops = 1000

	for range b.N {
		ring := NewBuffer()

		b.StartTimer()
		for range loops {
			_ = ring.WriteByte(0x00)
			_, _ = ring.ReadByte()
		}
		b.StopTimer()
	}
}

func BenchmarkIterator(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	for range b.N {
		const loops = 1000
		const count = 3

		requireT := require.New(b)

		ring := NewBuffer()
		_, err := ring.Write(make([]byte, count*loops))
		requireT.NoError(err)
		requireT.NoError(ring.Close())

		b.StartTimer()
		_, _ = ring.Iterate(func(p []byte) (int, bool, error) {
			return count, true, nil
		})
		b.StopTimer()
	}
}

func BenchmarkTCPNative(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	requireT := require.New(b)

	for range b.N {
		data := make([]byte, 100*1024*1024) // 100 MiBs of nothing
		_, err := rand.Read(data)
		requireT.NoError(err)

		result := make([]byte, 10*1024)

		l, err := net.Listen("tcp", "localhost:")
		requireT.NoError(err)

		defer l.Close()

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)

			conn, err := l.Accept()
			if err != nil {
				panic(err)
			}

			_ = conn.(*net.TCPConn).SetNoDelay(true)
			_ = conn.(*net.TCPConn).SetReadBuffer(32 * 1024)
			_ = conn.(*net.TCPConn).SetWriteBuffer(32 * 1024)

			total := 0
			for {
				n, err := conn.Read(result)
				total += n
				if total == len(data) {
					return
				}

				if err != nil {
					panic(err)
				}
			}
		}()

		conn, err := net.Dial("tcp", l.Addr().String())
		requireT.NoError(err)
		requireT.NoError(conn.(*net.TCPConn).SetNoDelay(true))
		requireT.NoError(conn.(*net.TCPConn).SetReadBuffer(128 * 1024))
		requireT.NoError(conn.(*net.TCPConn).SetWriteBuffer(128 * 1024))

		b.StartTimer()

		_, err = conn.Write(data)
		requireT.NoError(err)
		requireT.NoError(conn.Close())

		<-doneCh

		b.StopTimer()
	}
}

func BenchmarkTCPRing(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	requireT := require.New(b)

	for range b.N {
		data := make([]byte, 100*1024*1024) // 100 MiBs of nothing
		_, err := rand.Read(data)
		requireT.NoError(err)

		ring := NewBuffer()

		l, err := net.Listen("tcp", "localhost:")
		requireT.NoError(err)

		defer l.Close()

		done2Ch := make(chan struct{})
		go func() {
			defer close(done2Ch)

			_, _ = ring.WriteTo(io.Discard)
		}()

		done1Ch := make(chan struct{})
		go func() {
			defer close(done1Ch)

			conn, err := l.Accept()
			if err != nil {
				panic(err)
			}

			_ = conn.(*net.TCPConn).SetNoDelay(true)
			_ = conn.(*net.TCPConn).SetReadBuffer(32 * 1024)
			_ = conn.(*net.TCPConn).SetWriteBuffer(32 * 1024)

			_, err = ring.ReadFrom(conn)
			_ = ring.Close()
			if err != nil {
				panic(err)
			}
		}()

		conn, err := net.Dial("tcp", l.Addr().String())
		requireT.NoError(err)
		requireT.NoError(conn.(*net.TCPConn).SetNoDelay(true))
		requireT.NoError(conn.(*net.TCPConn).SetReadBuffer(128 * 1024))
		requireT.NoError(conn.(*net.TCPConn).SetWriteBuffer(128 * 1024))

		b.StartTimer()

		_, err = conn.Write(data)

		requireT.NoError(err)
		requireT.NoError(conn.Close())

		<-done1Ch

		b.StopTimer()

		<-done2Ch
	}
}
