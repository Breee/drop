package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dropv1alpha1 "github.com/corewire/drop/api/v1alpha1"
)

func TestPrometheusSource_Fetch_Instant(t *testing.T) {
	tests := []struct {
		name       string
		response   interface{}
		statusCode int
		wantCount  int
		wantErr    bool
		wantFirst  string
	}{
		{
			name: "valid response with image labels",
			response: prometheusResponse{
				Status: prometheusStatusSuccess,
				Data: struct {
					ResultType string             `json:"resultType"`
					Result     []prometheusResult `json:"result"`
				}{
					ResultType: "vector",
					Result: []prometheusResult{
						{
							Metric: map[string]string{"image": "nginx:1.25"},
							Value:  []interface{}{1234567890.0, "10"},
						},
						{
							Metric: map[string]string{"image": "redis:7.0"},
							Value:  []interface{}{1234567890.0, "5"},
						},
					},
				},
			},
			statusCode: http.StatusOK,
			wantCount:  2,
			wantFirst:  "nginx:1.25",
		},
		{
			name: "skips results without image label",
			response: prometheusResponse{
				Status: prometheusStatusSuccess,
				Data: struct {
					ResultType string             `json:"resultType"`
					Result     []prometheusResult `json:"result"`
				}{
					ResultType: "vector",
					Result: []prometheusResult{
						{
							Metric: map[string]string{"image": "nginx:1.25"},
							Value:  []interface{}{1234567890.0, "10"},
						},
						{
							Metric: map[string]string{"container": "sidecar"},
							Value:  []interface{}{1234567890.0, "3"},
						},
					},
				},
			},
			statusCode: http.StatusOK,
			wantCount:  1,
			wantFirst:  "nginx:1.25",
		},
		{
			name:       "HTTP error returns error",
			response:   "internal server error",
			statusCode: http.StatusInternalServerError,
			wantErr:    true,
		},
		{
			name: "empty results",
			response: prometheusResponse{
				Status: prometheusStatusSuccess,
				Data: struct {
					ResultType string             `json:"resultType"`
					Result     []prometheusResult `json:"result"`
				}{
					ResultType: "vector",
					Result:     []prometheusResult{},
				},
			},
			statusCode: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/query" {
					t.Errorf("unexpected path: %s, want /api/v1/query", r.URL.Path)
				}
				w.WriteHeader(tt.statusCode)
				if err := json.NewEncoder(w).Encode(tt.response); err != nil {
					t.Fatal(err)
				}
			}))
			defer server.Close()

			source := NewPrometheusSource(server.URL, "test_query", dropv1alpha1.QueryTypeInstant, 0, nil, 0, server.Client())
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
				if results[0].Image != tt.wantFirst {
					t.Errorf("first image = %q, want %q", results[0].Image, tt.wantFirst)
				}
			}
		})
	}
}

func TestPrometheusSource_Fetch_Range(t *testing.T) {
	tests := []struct {
		name              string
		aggregationMethod *dropv1alpha1.AggregationMethod
		response          prometheusResponse
		wantCount         int
		wantFirst         string
		wantScore         int64
	}{
		{
			name:              "nil aggregation (last value)",
			aggregationMethod: nil,
			response: prometheusResponse{
				Status: prometheusStatusSuccess,
				Data: struct {
					ResultType string             `json:"resultType"`
					Result     []prometheusResult `json:"result"`
				}{
					ResultType: "matrix",
					Result: []prometheusResult{
						{
							Metric: map[string]string{"image": "nginx:1.25"},
							Values: [][]interface{}{
								{1234567890.0, "10"},
								{1234567950.0, "20"},
								{1234568010.0, "30"},
							},
						},
					},
				},
			},
			wantCount: 1,
			wantFirst: "nginx:1.25",
			wantScore: 30, // last data-point value
		},
		{
			name:              "sum aggregation",
			aggregationMethod: aggregationMethodPtr(dropv1alpha1.AggregationSum),
			response: prometheusResponse{
				Status: prometheusStatusSuccess,
				Data: struct {
					ResultType string             `json:"resultType"`
					Result     []prometheusResult `json:"result"`
				}{
					ResultType: "matrix",
					Result: []prometheusResult{
						{
							Metric: map[string]string{"image": "nginx:1.25"},
							Values: [][]interface{}{
								{1234567890.0, "10"},
								{1234567950.0, "20"},
								{1234568010.0, "30"},
							},
						},
					},
				},
			},
			wantCount: 1,
			wantFirst: "nginx:1.25",
			wantScore: 60, // 10+20+30
		},
		{
			name:              "count aggregation",
			aggregationMethod: aggregationMethodPtr(dropv1alpha1.AggregationCount),
			response: prometheusResponse{
				Status: prometheusStatusSuccess,
				Data: struct {
					ResultType string             `json:"resultType"`
					Result     []prometheusResult `json:"result"`
				}{
					ResultType: "matrix",
					Result: []prometheusResult{
						{
							Metric: map[string]string{"image": "nginx:1.25"},
							Values: [][]interface{}{
								{1234567890.0, "10"},
								{1234567950.0, "20"},
								{1234568010.0, "30"},
							},
						},
					},
				},
			},
			wantCount: 1,
			wantFirst: "nginx:1.25",
			wantScore: 3,
		},
		{
			name:              "avg aggregation",
			aggregationMethod: aggregationMethodPtr(dropv1alpha1.AggregationAvg),
			response: prometheusResponse{
				Status: prometheusStatusSuccess,
				Data: struct {
					ResultType string             `json:"resultType"`
					Result     []prometheusResult `json:"result"`
				}{
					ResultType: "matrix",
					Result: []prometheusResult{
						{
							Metric: map[string]string{"image": "nginx:1.25"},
							Values: [][]interface{}{
								{1234567890.0, "10"},
								{1234567950.0, "20"},
								{1234568010.0, "30"},
							},
						},
					},
				},
			},
			wantCount: 1,
			wantFirst: "nginx:1.25",
			wantScore: 20, // (10+20+30)/3
		},
		{
			name:              "max aggregation",
			aggregationMethod: aggregationMethodPtr(dropv1alpha1.AggregationMax),
			response: prometheusResponse{
				Status: prometheusStatusSuccess,
				Data: struct {
					ResultType string             `json:"resultType"`
					Result     []prometheusResult `json:"result"`
				}{
					ResultType: "matrix",
					Result: []prometheusResult{
						{
							Metric: map[string]string{"image": "nginx:1.25"},
							Values: [][]interface{}{
								{1234567890.0, "10"},
								{1234567950.0, "20"},
								{1234568010.0, "30"},
							},
						},
					},
				},
			},
			wantCount: 1,
			wantFirst: "nginx:1.25",
			wantScore: 30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/query_range" {
					t.Errorf("unexpected path: %s, want /api/v1/query_range", r.URL.Path)
				}
				w.WriteHeader(http.StatusOK)
				if err := json.NewEncoder(w).Encode(tt.response); err != nil {
					t.Fatal(err)
				}
			}))
			defer server.Close()

			source := NewPrometheusSource(server.URL, "test_query", dropv1alpha1.QueryTypeRange, time.Hour, tt.aggregationMethod, 5*time.Minute, server.Client())
			results, err := source.Fetch(context.Background())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(results) != tt.wantCount {
				t.Errorf("got %d results, want %d", len(results), tt.wantCount)
			}

			if tt.wantFirst != "" && len(results) > 0 {
				if results[0].Image != tt.wantFirst {
					t.Errorf("first image = %q, want %q", results[0].Image, tt.wantFirst)
				}
				if results[0].Score != tt.wantScore {
					t.Errorf("score = %d, want %d", results[0].Score, tt.wantScore)
				}
			}
		})
	}
}

func TestPrometheusSource_DefaultQueryType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query_range" {
			t.Errorf("default queryType should use query_range, got path: %s", r.URL.Path)
		}
		resp := prometheusResponse{Status: prometheusStatusSuccess}
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatal(err)
		}
	}))
	defer server.Close()

	// Empty queryType should default to range
	source := NewPrometheusSource(server.URL, "test_query", "", time.Hour, nil, 0, server.Client())
	if source.QueryType != dropv1alpha1.QueryTypeRange {
		t.Errorf("default QueryType = %q, want %q", source.QueryType, dropv1alpha1.QueryTypeRange)
	}
	if source.AggregationMethod != nil {
		t.Errorf("default AggregationMethod = %v, want nil", source.AggregationMethod)
	}
	_, err := source.Fetch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Mimir/Thanos style endpoints carry a base path (e.g. /prometheus). Both Fetch
// and FetchRaw must append the API path instead of replacing it.
func TestPrometheusSource_PreservesEndpointBasePath(t *testing.T) {
	tests := []struct {
		name      string
		queryType dropv1alpha1.QueryType
		wantPath  string
	}{
		{"range", dropv1alpha1.QueryTypeRange, "/prometheus/api/v1/query_range"},
		{"instant", dropv1alpha1.QueryTypeInstant, "/prometheus/api/v1/query"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPaths []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPaths = append(gotPaths, r.URL.Path)
				if r.URL.Path != tt.wantPath {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				resp := prometheusResponse{Status: prometheusStatusSuccess}
				w.WriteHeader(http.StatusOK)
				if err := json.NewEncoder(w).Encode(resp); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()

			source := NewPrometheusSource(server.URL+"/prometheus", "test_query", tt.queryType,
				time.Hour, nil, 5*time.Minute, server.Client())

			if _, err := source.Fetch(context.Background()); err != nil {
				t.Errorf("Fetch() error = %v; requested paths %v, want %q", err, gotPaths, tt.wantPath)
			}
			if _, err := source.FetchRaw(context.Background()); err != nil {
				t.Errorf("FetchRaw() error = %v; requested paths %v, want %q", err, gotPaths, tt.wantPath)
			}
		})
	}
}

func aggregationMethodPtr(m dropv1alpha1.AggregationMethod) *dropv1alpha1.AggregationMethod {
	return &m
}
