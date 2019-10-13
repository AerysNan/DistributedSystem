package mapreduce

import (
	"log"
	"net"
	"net/rpc"
	"os"

	"github.com/sirupsen/logrus"
)

// Shutdown is an RPC method that shuts down the Master's RPC server.
func (mr *Master) Shutdown(_, _ *struct{}) error {
	logrus.Debug("Shutdown registration server")
	close(mr.shutdown)
	mr.l.Close() // causes the Accept to fail
	return nil
}

// startRPCServer starts the Master's RPC server. It continues accepting RPC
// calls (Register in particular) for as long as the worker is alive.
func (mr *Master) startRPCServer() {
	rpcs := rpc.NewServer()
	_ = rpcs.Register(mr)
	os.Remove(mr.address) // only needed for "unix"
	l, e := net.Listen("unix", mr.address)
	if e != nil {
		log.Fatal("RegstrationServer", mr.address, " error: ", e)
	}
	mr.l = l

	// now that we are listening on the master address, can fork off
	// accepting connections to another thread.
	go func() {
	loop:
		for {
			select {
			case <-mr.shutdown:
				break loop
			default:
			}
			conn, err := mr.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				logrus.WithError(err).Error("RegistrationServer accept error")
				break
			}
		}
		logrus.Debug("RegistrationServer done")
	}()
}

// stopRPCServer stops the master RPC server.
// This must be done through an RPC to avoid race conditions between the RPC
// server thread and the current thread.
func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if !ok {
		logrus.WithField("address", mr.address).Debug("Cleanup RPC error")
	}
	logrus.Debug("Cleanup Registration done")
}
