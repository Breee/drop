package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

func TestRegistrySource_Fetch(t *testing.T) {
	tests := []struct {
		name           string
		repos          []string
		tagFilter      string
		topX           int32
		imageTemplate  string
		versionPattern string
		tags           []string
		wantCount      int
		wantFirst      string
		wantErr        bool
	}{
		{
			name:      "basic tag listing",
			repos:     []string{"library/nginx"},
			tags:      []string{"1.24", "1.25", "1.26"},
			wantCount: 3,
		},
		{
			name:      "tag filter",
			repos:     []string{"library/nginx"},
			tagFilter: `^1\.2[56]$`,
			tags:      []string{"1.24", "1.25", "1.26"},
			wantCount: 2,
		},
		{
			name:      "topX limit",
			repos:     []string{"library/nginx"},
			topX:      2,
			tags:      []string{"1.24", "1.25", "1.26"},
			wantCount: 2,
		},
		{
			name:          "image template",
			repos:         []string{"gitlab-org/gitlab-runner/gitlab-runner-helper"},
			imageTemplate: "registry.gitlab.com/{{.Repository}}:x86_64-{{.Tag}}",
			tags:          []string{"v16.0", "v16.1"},
			wantCount:     2,
			wantFirst:     "registry.gitlab.com/gitlab-org/gitlab-runner/gitlab-runner-helper:x86_64-v16.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				resp := tagListResponse{
					Name: tt.repos[0],
					Tags: tt.tags,
				}
				w.WriteHeader(http.StatusOK)
				if err := json.NewEncoder(w).Encode(resp); err != nil {
					t.Fatal(err)
				}
			}))
			defer server.Close()

			source := NewRegistrySource(server.URL, tt.repos, tt.tagFilter, "", tt.topX, 0, tt.imageTemplate, tt.versionPattern, server.Client())
			results, err := source.Fetch(context.Background())

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(results) != tt.wantCount {
				t.Errorf("got %d results, want %d", len(results), tt.wantCount)
			}

			if tt.wantFirst != "" && len(results) > 0 {
				// Results sorted by score descending, highest score = last tag
				if results[0].Image != tt.wantFirst {
					t.Errorf("first image = %q, want %q", results[0].Image, tt.wantFirst)
				}
			}
		})
	}
}

// TestRegistrySource_Pagination verifies that the source follows the OCI
// `Link` header to walk every page. This mirrors GitLab's container registry,
// which returns 100 tags per page and links the next page — the newest semver
// tags (e.g. GitLab runner helper x86_64-v*) sort lexically onto later pages.
func TestRegistrySource_Pagination(t *testing.T) {
	repo := "gitlab-org/gitlab-runner/gitlab-runner-helper"
	// Page 1: lexically-early junk tags. Page 2: the real x86_64-v* versions.
	pages := map[string]tagListResponse{
		"": {Name: repo, Tags: []string{"3.18-arm-v17.8.0", "alpine-edge-arm-abc123", "x86_64-latest"}},
		"x86_64-v18.5.0": {Name: repo, Tags: []string{
			"x86_64-v18.5.0", "x86_64-v18.10.0", "x86_64-v19.0.0",
		}},
	}

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		last := r.URL.Query().Get("last")
		page, ok := pages[last]
		if !ok {
			t.Fatalf("unexpected last=%q", last)
		}
		// On the first page, link to the second.
		if last == "" {
			w.Header().Set("Link", "</v2/"+repo+"/tags/list?last=x86_64-v18.5.0&n=1000>; rel=\"next\"")
		}
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(page); err != nil {
			t.Fatal(err)
		}
	}))
	defer server.Close()

	source := NewRegistrySource(server.URL, []string{repo}, `^x86_64-v[0-9]+\.`, "", 2, 0, "", "x86_64-v(.+)", server.Client())
	results, err := source.Fetch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected top 2 results, got %d: %v", len(results), results)
	}
	host := server.URL[len("http://"):]
	if results[0].Image != host+"/"+repo+":x86_64-v19.0.0" {
		t.Errorf("expected x86_64-v19.0.0 first, got %s", results[0].Image)
	}
	if results[1].Image != host+"/"+repo+":x86_64-v18.10.0" {
		t.Errorf("expected x86_64-v18.10.0 second (10 > 5, not lexical), got %s", results[1].Image)
	}
}

// TestRegistrySource_BearerAuth verifies the anonymous OCI token flow used by
// registries like GitLab: the tags endpoint answers 401 with a Bearer challenge
// pointing at a token realm; the source must fetch an anonymous token from that
// realm and retry with an Authorization header.
func TestRegistrySource_BearerAuth(t *testing.T) {
	repo := "gitlab-org/gitlab-runner/gitlab-runner-helper"
	const wantToken = "anon-token-123"

	var registry *httptest.Server
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("service"); got != "container_registry" {
			t.Errorf("token request service = %q, want container_registry", got)
		}
		if got := r.URL.Query().Get("scope"); got != "repository:"+repo+":pull" {
			t.Errorf("token request scope = %q, want repository:%s:pull", got, repo)
		}
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]string{"token": wantToken}); err != nil {
			t.Fatal(err)
		}
	}))
	defer tokenServer.Close()

	var tokenRequests, tagRequests int
	registry = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tagRequests++
		if r.Header.Get("Authorization") != "Bearer "+wantToken {
			w.Header().Set("Www-Authenticate",
				`Bearer realm="`+tokenServer.URL+`",service="container_registry",scope="repository:`+repo+`:pull"`)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}`))
			tokenRequests++
			return
		}
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(tagListResponse{
			Name: repo,
			Tags: []string{"x86_64-v18.5.0", "x86_64-v19.0.0"},
		}); err != nil {
			t.Fatal(err)
		}
	}))
	defer registry.Close()

	source := NewRegistrySource(registry.URL, []string{repo}, `^x86_64-v`, "", 0, 0, "", "x86_64-v(.+)", registry.Client())
	results, err := source.Fetch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}
	host := registry.URL[len("http://"):]
	if results[0].Image != host+"/"+repo+":x86_64-v19.0.0" {
		t.Errorf("expected x86_64-v19.0.0 first, got %s", results[0].Image)
	}
	if tokenRequests != 1 {
		t.Errorf("expected exactly one 401 challenge, got %d", tokenRequests)
	}
	if tagRequests != 2 {
		t.Errorf("expected one challenged + one authorized tag request, got %d", tagRequests)
	}
}

func TestSortTagsNewestFirst(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "plain semver",
			in:   []string{"v1.9.0", "v1.10.0", "v1.2.0"},
			want: []string{"v1.10.0", "v1.9.0", "v1.2.0"},
		},
		{
			name: "gitlab runner helper arch-prefixed",
			in:   []string{"x86_64-v17.4.0", "x86_64-v17.10.0", "x86_64-v17.5.0"},
			want: []string{"x86_64-v17.10.0", "x86_64-v17.5.0", "x86_64-v17.4.0"},
		},
		{
			name: "flavor and arch prefix",
			in:   []string{"ubuntu-x86_64-v16.11.0", "alpine-x86_64-v17.0.0", "ubuntu-x86_64-v17.0.0"},
			want: []string{"alpine-x86_64-v17.0.0", "ubuntu-x86_64-v17.0.0", "ubuntu-x86_64-v16.11.0"},
		},
		{
			name: "non-versioned tags after versioned, push order reversed",
			in:   []string{"x86_64-latest", "x86_64-v17.5.0", "bleeding"},
			want: []string{"x86_64-v17.5.0", "bleeding", "x86_64-latest"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sortTagsNewestFirst(tt.in, nil)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("position %d: got %q, want %q (full: %v)", i, got[i], tt.want[i], got)
				}
			}
		})
	}
}

func TestSortTagsNewestFirst_VersionPattern(t *testing.T) {
	re := regexp.MustCompile(`x86_64-v(.+)`)
	in := []string{"x86_64-v17.4.0", "x86_64-v17.10.0", "ubuntu-v99.0.0", "x86_64-v17.5.0"}
	want := []string{"x86_64-v17.10.0", "x86_64-v17.5.0", "x86_64-v17.4.0", "ubuntu-v99.0.0"}

	got := sortTagsNewestFirst(in, re)
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("position %d: got %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}
