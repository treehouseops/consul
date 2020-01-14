package wanfed

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// connPool pools idle negotiated ALPN_WANGossipPacket flavored connections to
// remote servers. Idle connections only remain pooled for up to maxTime after
// they were last acquired.
type connPool struct {
	logger *log.Logger // TODO

	// maxTime is the maximum time to keep a connection open.
	maxTime time.Duration

	// mu protects pool and shutdown
	mu       sync.Mutex
	pool     map[string][]*conn
	shutdown bool

	shutdownCh chan struct{}
	reapWg     sync.WaitGroup
}

func newConnPool(logger *log.Logger, maxTime time.Duration) (*connPool, error) {
	if logger == nil {
		return nil, fmt.Errorf("wanfed: conn pool needs a logger")
	}
	if maxTime == 0 {
		return nil, fmt.Errorf("wanfed: conn pool needs a max time configured")
	}

	p := &connPool{
		logger:     logger,
		maxTime:    maxTime,
		pool:       make(map[string][]*conn),
		shutdownCh: make(chan struct{}),
	}

	p.reapWg.Add(1)
	go p.reap()

	return p, nil
}

func (p *connPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.shutdown {
		return nil
	}

	for _, conns := range p.pool {
		for _, conn := range conns {
			conn.Close()
		}
	}
	p.pool = nil
	p.shutdown = true

	close(p.shutdownCh)
	p.reapWg.Wait()

	return nil
}

// AcquireOrDial either removes an idle connection from the pool or
// estabilishes a new one using the provided dialer function.
func (p *connPool) AcquireOrDial(key string, dialer func() (net.Conn, error)) (*conn, error) {
	c, err := p.maybeAcquire(key)
	if err != nil {
		return nil, err
	}
	if c != nil {
		c.markForUse()
		p.logger.Printf("[DEBUG] wanfed: reusing conn for %q", key)
		return c, nil
	}

	nc, err := dialer()
	if err != nil {
		return nil, err
	}

	c = &conn{
		key:  key,
		pool: p,
		Conn: nc,
	}
	c.markForUse()
	p.logger.Printf("[DEBUG] wanfed: new conn for %q", key)

	return c, nil
}

var errPoolClosed = fmt.Errorf("wanfed: connection pool is closed")

// maybeAcquire removes an idle connection from the pool if possible otherwise
// returns nil indicating there were no idle connections ready. It is the
// caller's responsibility to open a new connection if that is desired.
func (p *connPool) maybeAcquire(key string) (*conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.shutdown {
		return nil, errPoolClosed
	}
	conns, ok := p.pool[key]
	if !ok {
		return nil, nil
	}

	switch len(conns) {
	case 0:
		delete(p.pool, key) // stray cleanup
		return nil, nil

	case 1:
		c := conns[0]
		delete(p.pool, key)
		return c, nil

	default:
		sz := len(conns)
		remaining, last := conns[0:sz-1], conns[sz-1] // car, cdr
		p.pool[key] = remaining
		return last, nil
	}
}

// returnConn puts the connection back into the idle pool for reuse.
func (p *connPool) returnConn(c *conn) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.shutdown {
		p.logger.Printf("[DEBUG] wanfed: auto-closing returned conn for %q", c.key)
		return c.Conn.Close() // actual shutdown
	}

	p.pool[c.key] = append(p.pool[c.key], c)

	p.logger.Printf("[DEBUG] wanfed: returning conn for %q", c.key)

	return nil
}

// reap periodically scans the idle pool for connections that have not been
// used recently and closes them.
func (p *connPool) reap() {
	defer p.reapWg.Done()
	for {
		select {
		case <-p.shutdownCh:
			return
		case <-time.After(time.Second):
		}

		p.reapOnce()
	}
}

func (p *connPool) reapOnce() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.shutdown {
		return
	}

	now := time.Now()

	var removedKeys []string
	for key, conns := range p.pool {
		if len(conns) == 0 {
			removedKeys = append(removedKeys, key) // cleanup
			continue
		}

		var retain []*conn
		for _, c := range conns {
			// Skip recently used connections
			if now.Sub(c.lastUsed) < p.maxTime {
				retain = append(retain, c)
			} else {
				c.Conn.Close()
				p.logger.Printf("[DEBUG] wanfed: reaping old conn for %q", c.key)
			}
		}

		if len(retain) == len(conns) {
			continue // no change

		} else if len(retain) == 0 {
			removedKeys = append(removedKeys, key)
			continue
		}

		p.pool[key] = retain
	}

	for _, key := range removedKeys {
		delete(p.pool, key)
	}
}

type conn struct {
	key string

	mu       sync.Mutex
	lastUsed time.Time
	failed   bool
	closed   bool

	pool *connPool

	net.Conn
}

func (c *conn) ReturnOrClose() error {
	c.mu.Lock()
	closed := c.closed
	failed := c.failed
	if failed {
		c.closed = true
	}
	c.mu.Unlock()

	if closed {
		return nil
	}

	if failed {
		c.pool.logger.Printf("[DEBUG] wanfed: reaping failed conn for %q", c.key)
		return c.Conn.Close()
	}

	return c.pool.returnConn(c)
}

func (c *conn) Close() error {
	c.mu.Lock()
	closed := c.closed
	c.closed = true
	c.mu.Unlock()

	if closed {
		return nil
	}

	c.pool.logger.Printf("[DEBUG] wanfed: closing conn for %q", c.key)
	return c.Conn.Close()
}

func (c *conn) markForUse() {
	c.mu.Lock()
	c.lastUsed = time.Now()
	c.failed = false
	c.mu.Unlock()
}

func (c *conn) MarkFailed() {
	c.mu.Lock()
	c.failed = true
	c.mu.Unlock()
}
