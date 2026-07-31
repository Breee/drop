/*
Copyright (c) 2026 Breee

SPDX-License-Identifier: MIT
*/

package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestParseBearerChallenge(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		want    *bearerChallenge
		wantNil bool
	}{
		{
			name:   "gitlab challenge",
			header: `Bearer realm="https://gitlab.com/jwt/auth",service="container_registry",scope="repository:foo/bar:pull"`,
			want:   &bearerChallenge{realm: "https://gitlab.com/jwt/auth", service: "container_registry", scope: "repository:foo/bar:pull"},
		},
		{
			name:   "case-insensitive scheme and no scope",
			header: `bearer realm="https://auth.example.com/token",service="registry"`,
			want:   &bearerChallenge{realm: "https://auth.example.com/token", service: "registry"},
		},
		{
			name:    "not a bearer challenge",
			header:  `Basic realm="registry"`,
			wantNil: true,
		},
		{
			name:    "missing realm",
			header:  `Bearer service="registry"`,
			wantNil: true,
		},
		{
			name:    "empty header",
			header:  "",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseBearerChallenge(tt.header)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("expected nil, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected %+v, got nil", tt.want)
			}
			if *got != *tt.want {
				t.Fatalf("expected %+v, got %+v", tt.want, got)
			}
		})
	}
}

// TestAuthTransportFollowsBearerChallenge verifies the Docker/OCI token workflow:
// a 401 with a Bearer challenge triggers a token fetch from the realm and a retry
// with the obtained bearer token.
func TestAuthTransportFollowsBearerChallenge(t *testing.T) {
	const wantToken = "test-bearer-token"

	var tokenRequests int32
	mux := http.NewServeMux()

	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenRequests, 1)
		if r.URL.Query().Get("service") != "registry" {
			t.Errorf("token request missing service param: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"token": wantToken})
	})

	mux.HandleFunc("/v2/foo/bar/tags/list", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+wantToken {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+server.URL+`/token",service="registry",scope="repository:foo/bar:pull"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"name": "foo/bar", "tags": []string{"v1", "v2"}})
	})

	client := &http.Client{Transport: &authTransport{base: http.DefaultTransport}}

	// First request triggers the challenge + token fetch + retry.
	resp, err := client.Get(server.URL + "/v2/foo/bar/tags/list")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// A second request should reuse the cached token (no extra token fetch).
	resp2, err := client.Get(server.URL + "/v2/foo/bar/tags/list")
	if err != nil {
		t.Fatalf("unexpected error on second request: %v", err)
	}
	_ = resp2.Body.Close()

	if got := atomic.LoadInt32(&tokenRequests); got != 1 {
		t.Fatalf("expected exactly 1 token request (cached), got %d", got)
	}
}

// TestAuthTransportUsesBasicAuthForToken verifies basic credentials from the
// Secret are sent to the token realm (not to the registry API itself).
func TestAuthTransportUsesBasicAuthForToken(t *testing.T) {
	const wantToken = "authed-token"

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "robot" || pass != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": wantToken})
	})

	mux.HandleFunc("/v2/foo/bar/tags/list", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+wantToken {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+server.URL+`/token",service="registry"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	secret := &corev1.Secret{Data: map[string][]byte{
		"username": []byte("robot"),
		"password": []byte("secret"),
	}}
	client := &http.Client{Transport: &authTransport{base: http.DefaultTransport, secret: secret}}

	resp, err := client.Get(server.URL + "/v2/foo/bar/tags/list")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
