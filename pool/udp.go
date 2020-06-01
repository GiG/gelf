package pool

import (
	"fmt"
	"net"
	"os"
)

const (
	_net = "udp4" //
)

func createUDPConnection(address string) (*net.UDPConn, error) {
	addr, err := net.ResolveUDPAddr(_net, address)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP(_net, nil, addr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func worker(id int, address string, buffers <-chan []byte) {

	conn, err := createUDPConnection(address)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create a connection to addr, err: %s", err.Error())
		return
	}

	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to close connection, err: %s", err.Error())
		}
	}()

	for buffer := range buffers {
		_, err := conn.Write(buffer)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to send message to addr, err: %s", err.Error())
		}
	}
}

type UDPPool struct {
	buffers      chan []byte
	workerNumber int
}

//NewUDPPool create new UDP workers pool
func NewUDPPool(address string, workerNumber int) *UDPPool {
	buffers := make(chan []byte, workerNumber)

	for wid := 1; wid < workerNumber; wid++ {
		go worker(wid, address, buffers)
	}

	return &UDPPool{buffers, workerNumber}
}

func (p *UDPPool) Fire(buffer []byte) {
	if len(p.buffers) < p.workerNumber {
		p.buffers <- buffer
	} else {
		fmt.Fprintf(os.Stderr, "Gelf buffer channel is full")
	}
}

func (p *UDPPool) Close() {
	close(p.buffers)
}
