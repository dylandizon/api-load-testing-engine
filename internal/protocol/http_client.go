package protocol

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"
)

// HTTPTiming captures granular request/response timings.
type HTTPTiming struct {
	DNS           time.Duration
	TCPConnect    time.Duration
	TLSHandshake  time.Duration
	TTFB          time.Duration
	TotalDuration time.Duration
	StatusCode    int
	BytesSent     int64
	BytesReceived int64
	Headers       http.Header
	Body          []byte
	Error         error
}

// ProtocolClient executes HTTP requests with timing hooks.
type ProtocolClient struct {
	client *http.Client
}

// NewProtocolClient creates an optimized HTTP/1.1 and HTTP/2 client.
func NewProtocolClient() *ProtocolClient {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   1000,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: false,
		},
	}

	return &ProtocolClient{
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}
}

// Do executes an HTTP request, recording network and protocol timings.
func (c *ProtocolClient) Do(ctx context.Context, method, rawURL string, headers map[string]string, body []byte) HTTPTiming {
	var timing HTTPTiming
	timing.BytesSent = int64(len(body))

	var dnsStart, connectStart, tlsStart, start time.Time
	var ttfbRecorded bool

	trace := &httptrace.ClientTrace{
		DNSStart: func(_ httptrace.DNSStartInfo) {
			dnsStart = time.Now()
		},
		DNSDone: func(_ httptrace.DNSDoneInfo) {
			if !dnsStart.IsZero() {
				timing.DNS = time.Since(dnsStart)
			}
		},
		ConnectStart: func(_, _ string) {
			connectStart = time.Now()
		},
		ConnectDone: func(_, _ string, _ error) {
			if !connectStart.IsZero() {
				timing.TCPConnect = time.Since(connectStart)
			}
		},
		TLSHandshakeStart: func() {
			tlsStart = time.Now()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			if !tlsStart.IsZero() {
				timing.TLSHandshake = time.Since(tlsStart)
			}
		},
		GotFirstResponseByte: func() {
			if !ttfbRecorded {
				timing.TTFB = time.Since(start)
				ttfbRecorded = true
			}
		},
	}

	reqCtx := httptrace.WithClientTrace(ctx, trace)
	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(reqCtx, method, rawURL, bodyReader)
	if err != nil {
		timing.Error = err
		return timing
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	start = time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		timing.TotalDuration = time.Since(start)
		timing.Error = err
		return timing
	}
	defer resp.Body.Close()

	timing.StatusCode = resp.StatusCode
	timing.Headers = resp.Header

	respBody, err := io.ReadAll(resp.Body)
	timing.TotalDuration = time.Since(start)
	if err != nil {
		timing.Error = err
		return timing
	}

	timing.Body = respBody
	timing.BytesReceived = int64(len(respBody))
	return timing
}
