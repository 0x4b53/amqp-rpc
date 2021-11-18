package amqprpc

import (
	"crypto/tls"
	"crypto/x509"
	"os"
)

// Certificates represents the certificate, the key and the CA to use
// when using RabbitMQ with TLS or the certificate and key when
// using as TLS configuration for RPC server.
// The fields should be the path to files stored on disk and will be
// passed to ioutil.ReadFile and tls.LoadX509KeyPair.
type Certificates struct {
	Cert string
	Key  string
	CA   string
}

// TLSConfig will return a *tls.Config type based on the files set
// in the Certificates type.
func (c *Certificates) TLSConfig() *tls.Config {
	// Use the system certificate pool by default but if it fails to load start
	// with an empty pool.
	certPool, err := x509.SystemCertPool()
	if err != nil {
		certPool = x509.NewCertPool()
	}

	tlsConfig := new(tls.Config)
	tlsConfig.RootCAs = certPool

	if ca, err := os.ReadFile(c.CA); err == nil {
		tlsConfig.RootCAs.AppendCertsFromPEM(ca)
	}

	if c.Cert != "" && c.Key != "" {
		if cert, err := tls.LoadX509KeyPair(c.Cert, c.Key); err == nil {
			tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
		}
	}

	return tlsConfig
}
