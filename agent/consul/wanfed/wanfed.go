package wanfed

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/consul/agent/pool"
	"github.com/hashicorp/consul/tlsutil"
	"github.com/hashicorp/memberlist"
)

const (
	// GossipPacketMaxIdleTime controls how long we keep an idle connection
	// open to a server.
	//
	// Conceptually similar to: agent/consul/server.go:serverRPCCache
	//
	// TODO(rb): should this actually be dynamically derived from the size of the wan pool?
	GossipPacketMaxIdleTime = 2 * time.Minute
)

const (
	// serverRPCCache controls how long we keep an idle connection
	// open to a server
	//
	// TODO: share with agent/consul/server.go:serverRPCCache
	serverRPCCache = 2 * time.Minute
)

type MeshGatewayResolver func(datacenter string) string

func NewTransport(
	logger *log.Logger,
	tlsConfigurator *tlsutil.Configurator,
	transport memberlist.NodeAwareTransport,
	datacenter string,
	gwResolver MeshGatewayResolver, // TODO: move this to the router? FindGatewayRoute?
) (*Transport, error) {
	// TODO(rb)
	if logger == nil {
		return nil, errors.New("wanfed: logger is nil")
	}
	// TODO(rb)
	if tlsConfigurator == nil {
		return nil, errors.New("wanfed: tlsConfigurator is nil")
	}
	// TODO(rb)
	if gwResolver == nil {
		return nil, errors.New("wanfed: gwResolver is nil")
	}

	cp, err := newConnPool(logger, GossipPacketMaxIdleTime)
	if err != nil {
		return nil, err
	}

	t := &Transport{
		NodeAwareTransport: transport,
		logger:             logger,
		tlsConfigurator:    tlsConfigurator,
		datacenter:         datacenter,
		gwResolver:         gwResolver,
		pool:               cp,
	}
	return t, nil
}

type Transport struct {
	memberlist.NodeAwareTransport

	logger          *log.Logger
	tlsConfigurator *tlsutil.Configurator
	datacenter      string
	gwResolver      MeshGatewayResolver
	pool            *connPool
}

var _ memberlist.NodeAwareTransport = (*Transport)(nil)

func (t *Transport) Shutdown() error {
	err1 := t.pool.Close()
	err2 := t.NodeAwareTransport.Shutdown()
	if err2 != nil {
		// the more important error is err2
		return err2
	}
	if err1 != nil {
		return err1
	}
	return nil
}

func (t *Transport) WriteToAddress(b []byte, addr memberlist.Address) (time.Time, error) {
	node, dc, err := splitNodeName(addr.Name)
	if err != nil {
		return time.Time{}, err
	}

	if dc != t.datacenter {
		t.logger.Printf("[DEBUG] wanfed.packet: dest.dc=%q in src.dc=%q", dc, t.datacenter)

		gwAddr := t.gwResolver(dc)
		if gwAddr == "" {
			return time.Time{}, fmt.Errorf("could not find suitable mesh gateway to dial dc=%q", dc)
			// TODO: return structs.ErrDCNotAvailable
		}

		dialFunc := func() (net.Conn, error) {
			return t.dial(dc, node, pool.ALPN_WANGossipPacket, gwAddr)
		}
		conn, err := t.pool.AcquireOrDial(addr.Name, dialFunc)
		if err != nil {
			return time.Time{}, err
		}
		defer conn.ReturnOrClose()

		// Send the length first.
		if err := binary.Write(conn, binary.BigEndian, uint32(len(b))); err != nil {
			conn.MarkFailed()
			return time.Time{}, err
		}

		if _, err = conn.Write(b); err != nil {
			conn.MarkFailed()
			return time.Time{}, err
		}

		return time.Now(), nil
	}

	return t.NodeAwareTransport.WriteToAddress(b, addr)
}

func (t *Transport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	node, dc, err := splitNodeName(addr.Name)
	if err != nil {
		return nil, err
	}

	if dc != t.datacenter {
		t.logger.Printf("[DEBUG] wanfed.stream: dest.dc=%q in src.dc=%q", dc, t.datacenter)

		gwAddr := t.gwResolver(dc)
		if gwAddr == "" {
			return nil, fmt.Errorf("could not find suitable mesh gateway to dial dc=%q", dc)
			// TODO: return structs.ErrDCNotAvailable
		}

		return t.dial(dc, node, pool.ALPN_WANGossipStream, gwAddr)
	}

	return t.NodeAwareTransport.DialAddressTimeout(addr, timeout)
}

// NOTE: There is a close mirror of this method in agent/pool/pool.go:DialTimeoutWithRPCType
func (t *Transport) dial(dc, nodeName, nextProto, addr string) (net.Conn, error) {
	t.logger.Printf("[DEBUG] wanfed: dialing dc=%q node=%q proto=%q via mgw-addr=%q",
		dc, nodeName, nextProto, addr)

	wrapper := t.tlsConfigurator.OutgoingALPNRPCWrapper()
	if wrapper == nil {
		return nil, fmt.Errorf("wanfed: cannot dial via a mesh gateway when outgoing TLS is disabled")
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}

	rawConn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	if tcp, ok := rawConn.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetNoDelay(true)
	}

	tlsConn, err := wrapper(dc, nodeName, nextProto, rawConn)
	if err != nil {
		return nil, err
	}

	return tlsConn, nil
}

func splitNodeName(fullName string) (nodeName, dc string, err error) {
	parts := strings.Split(fullName, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("node name does not encode a datacenter: %s", fullName)
	}
	return parts[0], parts[1], nil
}
