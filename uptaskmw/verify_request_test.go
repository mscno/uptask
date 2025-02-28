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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	testSigningKey = "test-signing-key-must-be-at-least-32-bytes"
	testIssuer     = "upstash.com"
)

// createJWT creates a valid JWT for testing
func createJWT(body []byte, signingKey string, issuer string, expOffset time.Duration, nbfOffset time.Duration) (string, error) {
	bodyHash := sha256.Sum256(body)
	encodedBodyHash := base64.URLEncoding.EncodeToString(bodyHash[:])

	now := time.Now()
	claims := map[string]interface{}{
		"iss":  issuer,
		"body": encodedBodyHash,
		"iat":  now.Unix(),
		"exp":  now.Add(expOffset).Unix(),
		"nbf":  now.Add(nbfOffset).Unix(),
		"jti":  "test-jti",
		"sub":  "test-sub",
		"aud":  "test-aud",
	}

	key, err := jwk.FromRaw([]byte(signingKey))
	if err != nil {
		return "", err
	}

	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	signed, err := jws.Sign(payload, jws.WithKey(jwa.HS256, key))
	if err != nil {
		return "", err
	}

	return string(signed), nil
}

// Mock handler for testing
type testHandler struct {
	called bool
}

func (h *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.called = true
	// Read body to verify it's still available
	body, _ := io.ReadAll(r.Body)
	w.Write(body)
}

func TestVerifyMiddleware_ValidSignature(t *testing.T) {
	testBody := []byte(`{"test":"data"}`)
	token, err := createJWT(testBody, testSigningKey, testIssuer, 1*time.Hour, -1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create test JWT: %v", err)
	}

	// Create a request with the test body and token
	req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(testBody))
	req.Header.Set("Upstash-Signature", token)

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was called (signature verified)
	if !handler.called {
		t.Error("Handler was not called, signature verification likely failed")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	// Check that the body was preserved
	if rr.Body.String() != string(testBody) {
		t.Errorf("Body was not preserved properly")
	}
}

func TestVerifyMiddleware_InvalidSignature(t *testing.T) {
	testBody := []byte(`{"test":"data"}`)
	token, err := createJWT(testBody, "wrong-signing-key-must-be-at-least-32-bytes", testIssuer, 1*time.Hour, -1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create test JWT: %v", err)
	}

	// Create a request with the test body and token
	req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(testBody))
	req.Header.Set("Upstash-Signature", token)

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was not called (signature verification failed)
	if handler.called {
		t.Error("Handler was called despite invalid signature")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusUnauthorized)
	}
}

func TestVerifyMiddleware_ExpiredToken(t *testing.T) {
	testBody := []byte(`{"test":"data"}`)
	token, err := createJWT(testBody, testSigningKey, testIssuer, -1*time.Hour, -2*time.Hour) // Expired 1 hour ago
	if err != nil {
		t.Fatalf("Failed to create test JWT: %v", err)
	}

	// Create a request with the test body and token
	req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(testBody))
	req.Header.Set("Upstash-Signature", token)

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was not called (signature verification failed)
	if handler.called {
		t.Error("Handler was called despite expired token")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusUnauthorized)
	}
}

func TestVerifyMiddleware_FutureToken(t *testing.T) {
	testBody := []byte(`{"test":"data"}`)
	token, err := createJWT(testBody, testSigningKey, testIssuer, 2*time.Hour, 1*time.Hour) // Not valid for another hour
	if err != nil {
		t.Fatalf("Failed to create test JWT: %v", err)
	}

	// Create a request with the test body and token
	req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(testBody))
	req.Header.Set("Upstash-Signature", token)

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was not called (signature verification failed)
	if handler.called {
		t.Error("Handler was called despite future token")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusUnauthorized)
	}
}

func TestVerifyMiddleware_InvalidIssuer(t *testing.T) {
	testBody := []byte(`{"test":"data"}`)
	token, err := createJWT(testBody, testSigningKey, "wrong-issuer", 1*time.Hour, -1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create test JWT: %v", err)
	}

	// Create a request with the test body and token
	req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(testBody))
	req.Header.Set("Upstash-Signature", token)

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was not called (signature verification failed)
	if handler.called {
		t.Error("Handler was called despite invalid issuer")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusUnauthorized)
	}
}

func TestVerifyMiddleware_ModifiedBody(t *testing.T) {
	originalBody := []byte(`{"test":"data"}`)
	token, err := createJWT(originalBody, testSigningKey, testIssuer, 1*time.Hour, -1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create test JWT: %v", err)
	}

	// Create a request with a modified body but the original token
	modifiedBody := []byte(`{"test":"modified"}`)
	req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(modifiedBody))
	req.Header.Set("Upstash-Signature", token)

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was not called (signature verification failed)
	if handler.called {
		t.Error("Handler was called despite modified body")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusUnauthorized)
	}
}

func TestVerifyMiddleware_MalformedToken(t *testing.T) {
	testBody := []byte(`{"test":"data"}`)

	// Create a request with a malformed token
	req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(testBody))
	req.Header.Set("Upstash-Signature", "not-a-valid-jwt")

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was not called (signature verification failed)
	if handler.called {
		t.Error("Handler was called despite malformed token")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusUnauthorized)
	}
}

func TestVerifyMiddleware_MissingToken(t *testing.T) {
	testBody := []byte(`{"test":"data"}`)

	// Create a request with no token
	req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(testBody))

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was not called (signature verification failed)
	if handler.called {
		t.Error("Handler was called despite missing token")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusUnauthorized)
	}
}

func TestVerifyMiddleware_ReadBodyError(t *testing.T) {
	// Create a request with a body that will cause an error when read
	errorReader := &errorReader{err: fmt.Errorf("read error")}
	req := httptest.NewRequest("POST", "/test", errorReader)
	req.Header.Set("Upstash-Signature", "some-token")

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was not called
	if handler.called {
		t.Error("Handler was called despite body read error")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusBadRequest)
	}
}

// errorReader is a helper that returns an error when Read is called
type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

func BenchmarkVerifyMiddleware(b *testing.B) {
	testBody := []byte(`{"test":"data"}`)
	token, err := createJWT(testBody, testSigningKey, testIssuer, 1*time.Hour, -1*time.Minute)
	if err != nil {
		b.Fatalf("Failed to create test JWT: %v", err)
	}

	handler := &testHandler{}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)
	wrappedHandler := middleware(handler)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(testBody))
		req.Header.Set("Upstash-Signature", token)
		rr := httptest.NewRecorder()
		wrappedHandler.ServeHTTP(rr, req)
	}
}

// TestVerifyMiddleware_LargeBody tests the middleware with a large request body
func TestVerifyMiddleware_LargeBody(t *testing.T) {
	// Create a large body with 1MB of data
	largeBody := strings.Repeat("x", 1024*1024)
	testBody := []byte(largeBody)

	token, err := createJWT(testBody, testSigningKey, testIssuer, 1*time.Hour, -1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create test JWT: %v", err)
	}

	// Create a request with the large body and token
	req := httptest.NewRequest("POST", "/test", bytes.NewBuffer(testBody))
	req.Header.Set("Upstash-Signature", token)

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Create the handler and middleware
	handler := &testHandler{called: false}
	middleware := VerifyMiddleware(testSigningKey, testIssuer)

	// Call the middleware with our handler
	middleware(handler).ServeHTTP(rr, req)

	// Check that the handler was called (signature verified)
	if !handler.called {
		t.Error("Handler was not called, signature verification failed with large body")
	}

	// Check the response status code
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}
