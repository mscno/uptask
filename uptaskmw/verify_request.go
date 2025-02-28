package uptaskmw

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// Config holds the configuration for the middleware
type Config struct {
	SigningKey string
	Issuer     string
	Logger     *slog.Logger
}

// VerifyMiddleware creates a standard http middleware function that verifies
// Upstash signatures on incoming requests with default configuration
func VerifyMiddleware(signingKey string, issuer string) func(http.Handler) http.Handler {
	return VerifyMiddlewareWithConfig(Config{
		SigningKey: signingKey,
		Issuer:     issuer,
	})
}

// VerifyMiddlewareWithConfig creates a middleware with a custom configuration
func VerifyMiddlewareWithConfig(config Config) func(http.Handler) http.Handler {
	// Use default logger if none is provided
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				logger.Error("failed to read request body", "error", err)
				http.Error(w, "Failed to read request body", http.StatusBadRequest)
				return
			}

			// Close the body and reset it for future reads
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // Reset body for re-reading

			// Verify the signature
			err = verifySignature(bodyBytes, r.Header.Get("Upstash-Signature"), config.SigningKey, config.Issuer)
			if err != nil {
				logger.Error(err.Error(), "error", err)
				httpErr, ok := err.(*httpError)
				if ok {
					http.Error(w, httpErr.message, httpErr.statusCode)
				} else {
					http.Error(w, "Failed to verify signature", http.StatusUnauthorized)
				}
				return
			}

			// Re-reset the body for the next handler
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

			// Call the next handler
			next.ServeHTTP(w, r)
		})
	}
}

// httpError is a custom error type that contains HTTP status code
type httpError struct {
	statusCode int
	message    string
	err        error
}

func (e *httpError) Error() string {
	return e.message
}

func newHTTPError(statusCode int, err error) *httpError {
	return &httpError{
		statusCode: statusCode,
		message:    err.Error(),
		err:        err,
	}
}

func verifySignature(body []byte, tokenString, signingKey, issuer string) error {
	// Parse and verify the token
	key, err := jwk.FromRaw([]byte(signingKey))
	if err != nil {
		return newHTTPError(http.StatusUnauthorized, fmt.Errorf("failed to create key: %w", err))
	}

	msg, err := jws.Verify([]byte(tokenString), jws.WithKey(jwa.HS256, key))
	if err != nil {
		return newHTTPError(http.StatusUnauthorized, fmt.Errorf("failed to verify token: %w", err))
	}

	type standardClaims struct {
		Aud  string `json:"aud"`
		Body string `json:"body"`
		Exp  int64  `json:"exp"`
		Iat  int64  `json:"iat"`
		Iss  string `json:"iss"`
		Jti  string `json:"jti"`
		Nbf  int64  `json:"nbf"`
		Sub  string `json:"sub"`
	}

	var sc standardClaims
	err = json.Unmarshal(msg, &sc)
	if err != nil {
		return newHTTPError(http.StatusUnauthorized, fmt.Errorf("failed to parse claims: %w", err))
	}

	// Verify standard claims
	if sc.Iss != issuer {
		return newHTTPError(http.StatusUnauthorized, fmt.Errorf("invalid issuer"))
	}

	if !time.Unix(sc.Exp, 0).After(time.Now()) {
		return newHTTPError(http.StatusUnauthorized, fmt.Errorf("token has expired"))
	}

	if time.Unix(sc.Nbf, 0).After(time.Now()) {
		return newHTTPError(http.StatusUnauthorized, fmt.Errorf("token is not valid yet"))
	}

	// Verify body hash
	bodyHash := sha256.Sum256(body)
	expectedBodyHash := base64.URLEncoding.EncodeToString(bodyHash[:])
	if sc.Body != expectedBodyHash {
		return newHTTPError(http.StatusUnauthorized, fmt.Errorf("body hash does not match"))
	}

	return nil
}
