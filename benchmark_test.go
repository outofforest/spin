package ringbuffer

import (
	"crypto/rand"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkCopyNative(b *testing.B) {
	b.StopTimer()

	const loops = 1000

	data := make([]byte, 10*1024) // 10 KiBs of nothing
	result := make([]byte, len(data))

	for i := 0; i < loops; i++ {
		b.StartTimer()

		// We copy twice because in ring buffer you copy to ring buffer and then from read buffer
		copy(result, data)
		copy(result, data)

		b.StopTimer()
	}
}

func BenchmarkCopyRing(b *testing.B) {
	b.StopTimer()

	const loops = 1000

	data := make([]byte, 10*1024) // 10 KiBs of nothing
	result := make([]byte, len(data))

	ring := New()

	for i := 0; i < loops; i++ {
		b.StartTimer()

		_, _ = ring.Write(data)
		_, _ = ring.Read(result)

		b.StopTimer()
	}
}

func BenchmarkTCPNative(b *testing.B) {
	b.StopTimer()

	requireT := require.New(b)

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
		_ = conn.(*net.TCPConn).SetReadBuffer(128 * 1024)
		_ = conn.(*net.TCPConn).SetWriteBuffer(128 * 1024)

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

func BenchmarkTCPRing(b *testing.B) {
	b.StopTimer()

	requireT := require.New(b)

	data := make([]byte, 100*1024*1024) // 100 MiBs of nothing
	_, err := rand.Read(data)
	requireT.NoError(err)

	ring := New()

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
		_ = conn.(*net.TCPConn).SetReadBuffer(128 * 1024)
		_ = conn.(*net.TCPConn).SetWriteBuffer(128 * 1024)

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
