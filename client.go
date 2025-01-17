package amqp

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
)

var (
	// ErrSessionClosed is propagated to Sender/Receivers
	// when Session.Close() is called.
	ErrSessionClosed = errors.New("amqp: session closed")

	// ErrLinkClosed is returned by send and receive operations when
	// Sender.Close() or Receiver.Close() are called.
	ErrLinkClosed = errors.New("amqp: link closed")

	// ErrLinkDetached is returned by operations when the
	// link is in a detached state.
	ErrLinkDetached = errors.New("amqp: link detached")
)

// Client is an AMQP client connection.
type Client struct {
	conn *conn
}

// Dial connects to an AMQP server.
//
// If the addr includes a scheme, it must be "amqp" or "amqps".
// If no port is provided, 5672 will be used for "amqp" and 5671 for "amqps".
//
// If username and password information is not empty it's used as SASL PLAIN
// credentials, equal to passing ConnSASLPlain option.
func Dial(addr string, opts ...ConnOption) (*Client, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "5672"
		if u.Scheme == "amqps" {
			port = "5671"
		}
	}

	// prepend SASL credentials when the user/pass segment is not empty
	if u.User != nil {
		pass, _ := u.User.Password()
		opts = append([]ConnOption{
			ConnSASLPlain(u.User.Username(), pass),
		}, opts...)
	}

	// append default options so user specified can overwrite
	opts = append([]ConnOption{
		ConnServerHostname(host),
	}, opts...)

	c, err := newConn(nil, opts...)
	if err != nil {
		return nil, err
	}

	dialer := &net.Dialer{Timeout: c.connectTimeout}
	switch u.Scheme {
	case "amqp", "":
		c.net, err = dialer.Dial("tcp", net.JoinHostPort(host, port))
	case "amqps":
		c.initTLSConfig()
		c.tlsNegotiation = false
		c.net, err = tls.DialWithDialer(dialer, "tcp", net.JoinHostPort(host, port), c.tlsConfig)
	default:
		return nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	if err != nil {
		return nil, err
	}
	err = c.start()
	return &Client{conn: c}, err
}

// New establishes an AMQP client connection over conn.
func New(conn net.Conn, opts ...ConnOption) (*Client, error) {
	c, err := newConn(conn, opts...)
	if err != nil {
		return nil, err
	}
	err = c.start()
	return &Client{conn: c}, err
}

// Close disconnects the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// NewSession opens a new AMQP session to the server.
func (c *Client) NewSession(opts ...SessionOption) (*Session, error) {
	// get a session allocated by Client.mux
	var sResp newSessionResp
	select {
	case <-c.conn.done:
		return nil, c.conn.getErr()
	case sResp = <-c.conn.newSession:
	}

	if sResp.err != nil {
		return nil, sResp.err
	}
	s := sResp.session

	for _, opt := range opts {
		err := opt(s)
		if err != nil {
			_ = s.Close(context.Background()) // deallocate session on error
			return nil, err
		}
	}

	// send Begin to server
	begin := &frames.PerformBegin{
		NextOutgoingID: 0,
		IncomingWindow: s.incomingWindow,
		OutgoingWindow: s.outgoingWindow,
		HandleMax:      s.handleMax,
	}
	debug(1, "TX: %s", begin)
	_ = s.txFrame(begin, nil)

	// wait for response
	var fr frames.Frame
	select {
	case <-c.conn.done:
		return nil, c.conn.getErr()
	case fr = <-s.rx:
	}
	debug(1, "RX: %s", fr.Body)

	begin, ok := fr.Body.(*frames.PerformBegin)
	if !ok {
		_ = s.Close(context.Background()) // deallocate session on error
		return nil, fmt.Errorf("unexpected begin response: %+v", fr.Body)
	}

	// start Session multiplexor
	go s.mux(begin)

	return s, nil
}

// Default session options
const (
	DefaultMaxLinks = 4294967296
	DefaultWindow   = 1000
)

// SessionOption is an function for configuring an AMQP session.
type SessionOption func(*Session) error

// SessionIncomingWindow sets the maximum number of unacknowledged
// transfer frames the server can send.
func SessionIncomingWindow(window uint32) SessionOption {
	return func(s *Session) error {
		s.incomingWindow = window
		return nil
	}
}

// SessionOutgoingWindow sets the maximum number of unacknowledged
// transfer frames the client can send.
func SessionOutgoingWindow(window uint32) SessionOption {
	return func(s *Session) error {
		s.outgoingWindow = window
		return nil
	}
}

// SessionMaxLinks sets the maximum number of links (Senders/Receivers)
// allowed on the session.
//
// n must be in the range 1 to 4294967296.
//
// Default: 4294967296.
func SessionMaxLinks(n int) SessionOption {
	return func(s *Session) error {
		if n < 1 {
			return errors.New("max sessions cannot be less than 1")
		}
		if int64(n) > 4294967296 {
			return errors.New("max sessions cannot be greater than 4294967296")
		}
		s.handleMax = uint32(n - 1)
		return nil
	}
}

// lockedRand provides a rand source that is safe for concurrent use.
type lockedRand struct {
	mu  sync.Mutex
	src *rand.Rand
}

func (r *lockedRand) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.src.Read(p)
}

// package scoped rand source to avoid any issues with seeding
// of the global source.
var pkgRand = &lockedRand{
	src: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// randBytes returns a base64 encoded string of n bytes.
func randString(n int) string {
	b := make([]byte, n)
	// from math/rand, cannot fail
	_, _ = pkgRand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DetachError is returned by a link (Receiver/Sender) when a detach frame is received.
//
// RemoteError will be nil if the link was detached gracefully.
type DetachError struct {
	RemoteError *Error
}

func (e *DetachError) Error() string {
	return fmt.Sprintf("link detached, reason: %+v", e.RemoteError)
}

// Default link options
const (
	DefaultLinkCredit      = 1
	DefaultLinkBatching    = false
	DefaultLinkBatchMaxAge = 5 * time.Second
)

// linkKey uniquely identifies a link on a connection by name and direction.
//
// A link can be identified uniquely by the ordered tuple
//     (source-container-id, target-container-id, name)
// On a single connection the container ID pairs can be abbreviated
// to a boolean flag indicating the direction of the link.
type linkKey struct {
	name string
	role encoding.Role // Local role: sender/receiver
}

const maxTransferFrameHeader = 66 // determined by calcMaxTransferFrameHeader
