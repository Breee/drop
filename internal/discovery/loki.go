package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	dropv1alpha1 "github.com/corewire/drop/api/v1alpha1"
)

const (
	lokiStatusSuccess = "success"
	lokiMessageField  = "message"
	// lokiLimitDefault is the maximum number of log entries to fetch per query.
	lokiLimitDefault = 5000
	// lokiSizeBytesSuffix is appended to image keys for extracted image-size samples.
	lokiSizeBytesSuffix = ":size_bytes"
)

// rePulledDuration matches the pull duration in Pulled event messages.
// Examples: "in 2.345s", "in 100ms", "in 1m", "in 1h"
var rePulledDuration = regexp.MustCompile(`\bin\s+(\d+(?:\.\d+)?)(ms|s|m|h)\b`)

// reImageRef matches an image reference in log messages.
// Handles: Pulling image "nginx:1.25"  /  image "nginx:1.25"
var reImageRef = regexp.MustCompile(`(?:image|Image)\s+"([^"]+)"`)

// reImageSizeBytes matches image size in Pulled messages.
// Example: "Image size: 20461242 bytes"
var reImageSizeBytes = regexp.MustCompile(`(?i)\bimage\s+size:\s*(\d+)\s+bytes\b`)

// lokiResponse is the top-level Loki query_range API response.
type lokiResponse struct {
	Status string   `json:"status"`
	Data   lokiData `json:"data"`
}

// lokiData is the data section of a Loki response.
type lokiData struct {
	ResultType string       `json:"resultType"`
	Result     []lokiStream `json:"result"`
}

// lokiStream is a single log stream from Loki (labels + values).
type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"` // [nanosecond_timestamp_string, log_line]
}

// LokiSource fetches log events from a Loki-compatible API.
type LokiSource struct {
	Endpoint   string
	Query      string
	Lookback   time.Duration
	Parser     *dropv1alpha1.LokiParser
	HTTPClient *http.Client
}

// NewLokiSource creates a new LokiSource.
func NewLokiSource(endpoint, query string, lookback time.Duration, parser *dropv1alpha1.LokiParser, httpClient *http.Client) *LokiSource {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &LokiSource{
		Endpoint:   endpoint,
		Query:      query,
		Lookback:   lookback,
		Parser:     parser,
		HTTPClient: httpClient,
	}
}

// FetchRaw calls /loki/api/v1/query_range and returns per-image timed samples.
//
// For a kubernetesEvents parser, sample values are pull durations in seconds
// (parsed from Pulled event messages). Image sizes are stored under the key
// "image:size_bytes".
//
// Without a parser, each log entry produces a value=1.0 sample keyed by
// the "image" stream label.
func (l *LokiSource) FetchRaw(ctx context.Context) (map[string][]TimedSample, error) {
	u, err := url.Parse(l.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parsing endpoint: %w", err)
	}
	u.Path, err = url.JoinPath(u.Path, "/loki/api/v1/query_range")
	if err != nil {
		return nil, fmt.Errorf("building query URL: %w", err)
	}

	lookback := l.Lookback
	if lookback == 0 {
		lookback = 24 * time.Hour
	}
	now := time.Now().UTC()

	q := u.Query()
	q.Set("query", l.Query)
	q.Set("start", strconv.FormatInt(now.Add(-lookback).UnixNano(), 10))
	q.Set("end", strconv.FormatInt(now.UnixNano(), 10))
	q.Set("limit", strconv.Itoa(lokiLimitDefault))
	q.Set("direction", "forward")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := l.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("querying loki: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("loki returned status %d: %s", resp.StatusCode, string(body))
	}

	var lokiResp lokiResponse
	if err := json.NewDecoder(resp.Body).Decode(&lokiResp); err != nil {
		return nil, fmt.Errorf("decoding loki response: %w", err)
	}
	if lokiResp.Status != lokiStatusSuccess {
		return nil, fmt.Errorf("loki query failed with status: %s", lokiResp.Status)
	}

	return l.parseLokiStreams(lokiResp.Data.Result), nil
}

// parseLokiStreams converts Loki streams into per-image timed samples using
// the configured parser (or a generic image-label fallback).
func (l *LokiSource) parseLokiStreams(streams []lokiStream) map[string][]TimedSample {
	if l.Parser != nil && l.Parser.Type == dropv1alpha1.LokiParserTypeKubernetesEvents {
		return parseKubernetesEventStreams(streams, l.Parser)
	}
	return parseGenericLokiStreams(streams)
}

// parseGenericLokiStreams produces value=1.0 samples keyed by the "image" stream label.
func parseGenericLokiStreams(streams []lokiStream) map[string][]TimedSample {
	out := make(map[string][]TimedSample)
	for _, stream := range streams {
		image := stream.Stream["image"]
		if image == "" {
			continue
		}
		for _, entry := range stream.Values {
			if len(entry) < 2 {
				continue
			}
			ts := parseLokiNanoTimestamp(entry[0])
			out[image] = append(out[image], TimedSample{Timestamp: ts, Value: 1.0})
		}
	}
	return out
}

// lokiEventRecord is an intermediate representation of a parsed Kubernetes Event.
type lokiEventRecord struct {
	image     string
	pod       string
	reason    string
	message   string
	timestamp float64
}

// parseKubernetesEventStreams parses Kubernetes Event records from Loki log entries.
//
// Only Pulled events are consumed. It produces:
//   - samples[image] → pull duration in seconds for each Pulled event
//   - samples[image+":size_bytes"] → image size in bytes per Pulled event (if present)
//
// Durations and sizes are parsed from the Pulled event message text.
func parseKubernetesEventStreams(streams []lokiStream, parser *dropv1alpha1.LokiParser) map[string][]TimedSample {
	reasonField := lokiCoalesceField(parser.ReasonField, "reason")
	podField := lokiCoalesceField(parser.PodField, "involvedObject_name")
	messageField := lokiCoalesceField(parser.MessageField, lokiMessageField)
	imageField := lokiCoalesceField(parser.ImageField, lokiMessageField)

	var records []lokiEventRecord
	for _, stream := range streams {
		for _, entry := range stream.Values {
			if len(entry) < 2 {
				continue
			}
			ts := parseLokiNanoTimestamp(entry[0])

			rec := lokiEventRecord{
				timestamp: ts,
				reason:    stream.Stream[reasonField],
				pod:       stream.Stream[podField],
				message:   stream.Stream[messageField],
			}

			// If key fields are absent from labels, try to parse the log line as JSON.
			if rec.reason == "" || rec.message == "" {
				var parsed map[string]interface{}
				if err := json.Unmarshal([]byte(entry[1]), &parsed); err == nil {
					if rec.reason == "" {
						rec.reason = lokiJSONField(parsed, reasonField, "reason")
					}
					if rec.pod == "" {
						rec.pod = lokiJSONField(parsed, podField, "involvedObject_name", "name")
					}
					if rec.message == "" {
						rec.message = lokiJSONField(parsed, messageField, lokiMessageField, "msg")
					}
				} else if rec.message == "" {
					rec.message = entry[1]
				}
			}

			// Infer reason from message text when no structured label provided it.
			if rec.reason == "" && rec.message != "" {
				rec.reason = lokiInferReasonFromMessage(rec.message)
			}

			// Determine the source string for image extraction.
			var imgSource string
			if imageField == messageField || imageField == lokiMessageField {
				imgSource = rec.message
			} else {
				imgSource = stream.Stream[imageField]
				if imgSource == "" {
					imgSource = rec.message
				}
			}
			rec.image = lokiExtractImageFromMessage(imgSource)
			if rec.image == "" {
				continue
			}
			records = append(records, rec)
		}
	}

	out := make(map[string][]TimedSample)

	for _, rec := range records {
		// Only Pulled events carry the data we rank on (duration + image size).
		if strings.ToLower(rec.reason) != "pulled" {
			continue
		}
		dur := lokiParsePullDuration(rec.message)
		sizeBytes := lokiParseImageSizeBytes(rec.message)
		if dur > 0 {
			out[rec.image] = append(out[rec.image], TimedSample{Timestamp: rec.timestamp, Value: dur})
		}
		if sizeBytes > 0 {
			out[rec.image+lokiSizeBytesSuffix] = append(
				out[rec.image+lokiSizeBytesSuffix],
				TimedSample{Timestamp: rec.timestamp, Value: sizeBytes},
			)
		}
	}

	return out
}

// lokiExtractImageFromMessage extracts an image reference from a message string.
// Handles patterns such as:  Pulling image "nginx:1.25"
func lokiExtractImageFromMessage(msg string) string {
	m := reImageRef.FindStringSubmatch(msg)
	if len(m) > 1 {
		return m[1]
	}
	return ""
}

// lokiParsePullDuration extracts the pull duration in seconds from a Pulled event message.
// Example: "Successfully pulled image \"nginx:1.25\" in 2.345s ..."
func lokiParsePullDuration(msg string) float64 {
	m := rePulledDuration.FindStringSubmatch(msg)
	if len(m) < 3 {
		return 0
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0
	}
	switch m[2] {
	case "ms":
		return v / 1000.0
	case "m":
		return v * 60
	case "h":
		return v * 3600
	default: // "s"
		return v
	}
}

// lokiParseImageSizeBytes extracts image size in bytes from a Pulled event message.
// Example: "... Image size: 20461242 bytes."
func lokiParseImageSizeBytes(msg string) float64 {
	m := reImageSizeBytes.FindStringSubmatch(msg)
	if len(m) < 2 {
		return 0
	}
	v, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return float64(v)
}

// lokiInferReasonFromMessage infers a Kubernetes Event reason from a plain-text log message.
// This is used when the reason field is not present in the Loki stream labels.
// Only Pulled events are relevant to discovery, so other reasons are ignored.
func lokiInferReasonFromMessage(msg string) string {
	if strings.Contains(strings.ToLower(msg), "successfully pulled") {
		return "Pulled"
	}
	return ""
}

// parseLokiNanoTimestamp converts a Loki nanosecond epoch string to Unix seconds (float64).
func parseLokiNanoTimestamp(s string) float64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return float64(v) / 1e9
}

// lokiCoalesceField returns field if non-empty, otherwise defaultVal.
func lokiCoalesceField(field, defaultVal string) string {
	if field != "" {
		return field
	}
	return defaultVal
}

// lokiJSONField reads the first non-empty string value from a JSON event using the
// configured key first, then common aliases (e.g. Grafana Alloy emits "msg"/"name"
// where raw event JSON uses "message"/"involvedObject_name"). Returns "" if none match.
func lokiJSONField(parsed map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if k == "" {
			continue
		}
		if v, ok := parsed[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}
