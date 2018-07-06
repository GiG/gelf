package pool

import (
	"fmt"
	"net"
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
		fmt.Printf("Unable to create a connection to addr, err: %s", err.Error())
		return
	}

	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	for buffer := range buffers {
		_, err := conn.Write(buffer)
		if err != nil {
			fmt.Printf("Unable to send message to addr, err: %s", err.Error())
		}
	}
}

type UDPPool struct {
	buffers chan []byte
}

//NewUDPPool create new UDP workers pool
func NewUDPPool(address string, workerNumber int) *UDPPool {
	buffers := make(chan []byte, workerNumber)

	for wid := 1; wid < workerNumber; wid++ {
		go worker(wid, address, buffers)
	}

	return &UDPPool{buffers}
}

func (p *UDPPool) Fire(buffer []byte) {
	p.buffers <- buffer
}

func (p *UDPPool) Close() {
	close(p.buffers)
}
