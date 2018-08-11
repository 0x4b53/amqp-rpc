package amqprpc

import (
	"os"
	"testing"

	. "gopkg.in/go-playground/assert.v1"
)

func TestTLS(t *testing.T) {
	createCertificateFiles()
	defer removeCertificateFiles()

	c := Certificates{
		Cert: "./temp-cert.pem",
		Key:  "./temp-key.key",
		CA:   "./temp-ca.pem",
	}

	Equal(t, c.Cert, "./temp-cert.pem")

	cfg := c.TLSConfig()

	Equal(t, cfg.RootCAs == nil, false)
	Equal(t, cfg == nil, false)
	Equal(t, len(cfg.Certificates), 1)

	c = Certificates{}

	cfg = c.TLSConfig()

	Equal(t, cfg == nil, false)
	Equal(t, len(cfg.Certificates), 0)
}

func createFile(path, content string) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	_, err = f.WriteString(content)
	if err != nil {
		panic(err)
	}
}

func removeFile(path string) {
	if err := os.Remove(path); err != nil {
		panic(err)
	}
}

var (
	key = `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCs1E61OTgl8XhQ
iLyi4ZoQOhS96G8YEetFWw7xJcznDC5mW0tJwl+TV7dK/Aujt2QcL9lqE4ofXpvS
OXBuGASmf4xB5rnuCgtbygq+FkdAeZVtWEuAOHjIdDKjwrk1VYvDfalKr0+uASQV
mTuU/SQICAuX6ueWQT8YUr6qxjC6JchIz09HMOOvJEbBaah0JtH7dBfOckPB9l9n
ujH2X9yonGbMH8RSn1R+T80U2oo7gVtRvXloCEGHcKeQF8AmGjQaYoZXKf0LACmk
0N7FxauQQEN3AEWC5Kq/nTgIjqnM4/u8VTV2nsWXolg5ZKV8PdR3M6EZepfIFXNN
IQGu9nGpAgMBAAECggEATpFhio8NkGo6mNngb2eB4ziUL1UYE+gpfWkM3OGjSbHG
8i80hb6ANnpc0BDOtclsEEhMXSWu193plmMYUmRG0O4Q8/CQu78DNOIfihSUpaHg
JUpLYGnLtszkLIAcNDvEgsWAjXwvC9pm7g6wAGYn2CLYKmLWjv7wUP1kwjvA8Q32
WO3FlfybMgFJJNimhcATEA8l9gMZ+oXplEBvPuV22Bj3eCBeZsFPF/BgxYC/ih/6
pXF15cQtsAjgtZXrBWAXWBj0RN8vcYv5e4lkz0bgzliLkwXWO0IVTAVzO2qSPaOo
c5JrLUNZ+BG+aYEDfsxMvV5UTIpJ4/fxA/7egA/b1QKBgQDgyEeurQr239Aa8nrK
JHV6/Ae9xyP2nn2tWO2Ivoj7gFk+peZoNiILMx+px5spQzV00fR0X1pzx5Vwu8I3
nGbGe668q3vC2+O4PV0mxXj0mFoAXJynqceqG2W19/b1HRuHhR+v55P3tuFnOX2F
2YP3VNzD8n34TxnuIiH+JZxvNwKBgQDE1O8hMVwFyr6qs/rTf5MEoLJFCQ7nsE40
QqoWr28ZnVRwuMf+KJK3tO7yuoqQWcivNendx+YDQzCO7PSGqYS2aArEitXuWz9w
w4x/NO6iJYGOqWa1L1IDo6+uq5aIFJ5KCFH7ji2ry8Xgxk2FmOo+hpjxqfwhJEK8
aRQuiNvWHwKBgA0malsjsHKE4W3SJbDRtTW+rM1Day0wVHXhr/PCAc2E0rPBjNsn
35H8KawLC168mdH9vmlYcrg3QtzXfyM1uSV/cteCyLi7mHTc0ihEgTD+ALXotRlx
60ZeV/LvULlCPKwO862cxKFHR6R8ToWYFgpQIqIr+4adSg0OXiK6HI69AoGAdS60
IxXIzGX095npJYtZErM9Dt6isgsDtBdQPes6AIzrHaTU5Bpxps8gRwAJyIC7epxl
XDVLWfvhZ+Xoeldn/FSavIJPdPV8In05Iu53d69On4l47Tt731DPIfVjzCZCSp8D
d/kgdZv+daB5vKTaKFlnqtBhm7WDybnhWwb9ok8CgYEAqRxyqv53qMml26tJvHXX
mOop3gOj0Og+uXemcu5VpfOXFj/OP5NaCF0p27LjydoX/Pt3OHgnElTrvBXPaVHp
TxkhUHHa5/uHgv65ANrxSgt3DumfKXsMZbvPAgQl+kKjv72Nyd7et1MDazBzK1ig
XgPQxD/mhJpu+AAnZO49/wk=
-----END PRIVATE KEY-----`

	cert = `-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKPktBIbe7kuMA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTgwNDMwMTMwMTA5WhcNMjgwNDI3MTMwMTA5WjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEArNROtTk4JfF4UIi8ouGaEDoUvehvGBHrRVsO8SXM5wwuZltLScJfk1e3
SvwLo7dkHC/ZahOKH16b0jlwbhgEpn+MQea57goLW8oKvhZHQHmVbVhLgDh4yHQy
o8K5NVWLw32pSq9PrgEkFZk7lP0kCAgLl+rnlkE/GFK+qsYwuiXISM9PRzDjryRG
wWmodCbR+3QXznJDwfZfZ7ox9l/cqJxmzB/EUp9Ufk/NFNqKO4FbUb15aAhBh3Cn
kBfAJho0GmKGVyn9CwAppNDexcWrkEBDdwBFguSqv504CI6pzOP7vFU1dp7Fl6JY
OWSlfD3UdzOhGXqXyBVzTSEBrvZxqQIDAQABo1AwTjAdBgNVHQ4EFgQUgjb4Sw0Z
ViaEWft11wutbFF0G8swHwYDVR0jBBgwFoAUgjb4Sw0ZViaEWft11wutbFF0G8sw
DAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAi7Ca4zOYq1j2AACqXPOn
slet1nfRrMFrVbEwKwqgYnCXoSYbhpfglGF3+tXhstVY0A/qiVOzNhPBaFm/KiOY
w1nm8rK/Ydxh6MAdnlkbOyiGFK/dCDpamdrYpROJfzLzYtZFrhtNeyjub11bEdkI
yGv/BTo5JPdhmAXFqZ33HZLU+QtD+jfmap2wfIWKI2rIUSYGsZiotJqKakegALHQ
enS4T57nfHezZlAFAJ9IUU2P81cysWLv8ECwT7AAgmMNlp5cE6ud5AGosiIx3lqo
0UENiAIdx3/ki0CvmZ5evKBok7+wC7Q9aXLgYBIztAQuJZebQ8Hjs6RBhdn0L53O
ug==
-----END CERTIFICATE-----`

	certificates = map[string]string{
		"./temp-ca.pem":   cert,
		"./temp-key.key":  key,
		"./temp-cert.pem": cert,
	}
)

func createCertificateFiles() {
	for file, content := range certificates {
		createFile(file, content)
	}
}

func removeCertificateFiles() {
	for file := range certificates {
		removeFile(file)
	}
}
