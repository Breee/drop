/*
Copyright (c) 2026 Breee

SPDX-License-Identifier: MIT
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DiscoveryPolicySpec defines the desired state of DiscoveryPolicy.
type DiscoveryPolicySpec struct {
	// Queries is the list of named raw-data sources. Each query is referenced by name from signals.
	// +optional
	Queries []DiscoveryQuery `json:"queries,omitempty"`
	// Signals is the list of named per-image metrics derived from query results.
	// Each signal is referenced by name from the ranking configuration.
	// +optional
	Signals []DiscoverySignal `json:"signals,omitempty"`
	// Ranking defines how signals are combined into a final ordered image list.
	// +optional
	Ranking *DiscoveryRanking `json:"ranking,omitempty"`
	// ImageFilter is a regex applied to discovered image references. Only matching images are kept.
	// Example: "registry.example.com/team/.*" (only keep images from that registry path)
	// +optional
	ImageFilter string `json:"imageFilter,omitempty"`
	// SyncInterval is how often the operator re-runs the pipeline and updates status.discoveredImages.
	// Default: "30m". Example: "1h", "15m"
	// +kubebuilder:default="30m"
	SyncInterval metav1.Duration `json:"syncInterval,omitempty"`
	// MaxImages caps the total number of images stored in status.discoveredImages.
	// Images are ranked by score; lowest-scoring images are dropped when the cap is exceeded.
	// Default: 50. Example: 30, 100
	// +kubebuilder:default=50
	// +kubebuilder:validation:Minimum=1
	MaxImages int32 `json:"maxImages,omitempty"`
}

// ============================================================
// Stage 1 — Queries
// ============================================================

// DiscoveryQueryType identifies the backend for a named query.
// +kubebuilder:validation:Enum=prometheus;loki;registry
type DiscoveryQueryType string

const (
	// DiscoveryQueryTypePrometheus fetches time-series data from a Prometheus-compatible API.
	DiscoveryQueryTypePrometheus DiscoveryQueryType = "prometheus"
	// DiscoveryQueryTypeLoki fetches log event data from a Loki-compatible API.
	DiscoveryQueryTypeLoki DiscoveryQueryType = "loki"
	// DiscoveryQueryTypeRegistry lists image tags from an OCI-compatible container registry.
	DiscoveryQueryTypeRegistry DiscoveryQueryType = "registry"
)

// DiscoveryQuery defines a named raw-data source referenced by signals.
type DiscoveryQuery struct {
	// Name is the unique identifier for this query within the policy.
	// Signals reference queries by this name via query.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Type selects the backend. Must be "prometheus", "loki", or "registry".
	// +kubebuilder:validation:Enum=prometheus;loki;registry
	Type DiscoveryQueryType `json:"type"`
	// Prometheus contains the configuration when type=prometheus.
	// +optional
	Prometheus *DiscoveryPrometheusQuery `json:"prometheus,omitempty"`
	// Loki contains the configuration when type=loki.
	// +optional
	Loki *DiscoveryLokiQuery `json:"loki,omitempty"`
	// Registry contains the configuration when type=registry.
	// +optional
	Registry *DiscoveryRegistryQuery `json:"registry,omitempty"`
	// SecretRef references a Secret in the pod namespace (default "drop-system") for auth/TLS.
	// Supported Secret keys: token, username, password, ca.crt, tls.crt, tls.key, headers.<name>.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

// DiscoveryRegistryQuery defines OCI registry tag listing configuration for image discovery.
type DiscoveryRegistryQuery struct {
	// URL is the registry base URL (without repository path).
	// Example: "https://registry.example.com", "https://ghcr.io"
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`
	// Repositories is the list of repository paths to list tags from.
	// Example: ["team/app", "team/worker", "infra/tools"]
	// +kubebuilder:validation:MinItems=1
	Repositories []string `json:"repositories"`
	// TagFilter is a regex applied to tag names. Only matching tags are discovered.
	// Example: "^v[0-9]+\\." (semver tags only), "^main-" (main branch builds)
	// +optional
	TagFilter string `json:"tagFilter,omitempty"`
	// TagSeek is a pagination cursor passed to the registry as the `last` query
	// parameter. The registry lists tags lexically after this value, letting you
	// skip large numbers of irrelevant earlier tags without fetching them. It is
	// not a real tag name — any string works.
	// Example: "x86_64-u~" jumps straight to the "x86_64-v*" tags on a repo with
	// tens of thousands of digest tags (GitLab runner helper).
	// +optional
	TagSeek string `json:"tagSeek,omitempty"`
	// TopX limits the number of tags kept per repository after tagFilter is applied.
	// Tags are sorted newest-first (by version) before this cap is applied, so the
	// newest N tags are kept.
	// Example: 3 (keep the 3 newest matching tags per repo)
	// +optional
	// +kubebuilder:validation:Minimum=1
	TopX int32 `json:"topX,omitempty"`
	// MaxScan caps how many tags are fetched per repository before filtering.
	// Registries can hold tens of thousands of tags; this bounds the work. Pair
	// it with tagSeek to fetch only the relevant range. Defaults to 1000 when unset.
	// Example: 500
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxScan int32 `json:"maxScan,omitempty"`
	// VersionPattern is a regex with a single capture group that extracts the
	// version substring from each tag for newest-first sorting. Use it when tags
	// carry a prefix/suffix around the version, e.g. GitLab runner helper tags
	// like "x86_64-v17.5.0" (pattern "x86_64-v(.+)").
	// When unset, Drop tries a strict semver parse, then falls back to extracting
	// an embedded semver substring. Tags with no parseable version keep registry
	// push order and sort after versioned tags.
	// Example: "x86_64-v(.+)"
	// +optional
	VersionPattern string `json:"versionPattern,omitempty"`
	// ImageTemplate is a Go text/template for constructing the full image reference from discovered tags.
	// Available variables: {{.Registry}}, {{.Repository}}, {{.Tag}}
	// Default (when unset): "{{.Registry}}/{{.Repository}}:{{.Tag}}"
	// Example: "registry.example.com/{{.Repository}}:{{.Tag}}"
	// +optional
	ImageTemplate string `json:"imageTemplate,omitempty"`
}

// QueryType defines how the Prometheus query is executed.
// +kubebuilder:validation:Enum=range;instant
type QueryType string

const (
	// QueryTypeRange uses /api/v1/query_range with a time window defined by lookback.
	// Returns multiple data points which are aggregated at the signal stage.
	QueryTypeRange QueryType = "range"
	// QueryTypeInstant uses /api/v1/query for a single point-in-time result.
	// The returned value is used directly as the raw sample value.
	QueryTypeInstant QueryType = "instant"
)

// DiscoveryPrometheusQuery defines the Prometheus-specific query parameters.
// The PromQL result MUST carry an "image" label; that label value is the image reference.
type DiscoveryPrometheusQuery struct {
	// Endpoint is the Prometheus-compatible API URL (Prometheus, Thanos, Mimir, VictoriaMetrics).
	// Include any base path the backend serves its API under; Drop appends /api/v1/query[_range].
	// Grafana Mimir serves under /prometheus, e.g. "https://mimir.example.com/prometheus".
	// Example: "http://prometheus.monitoring.svc:9090", "https://mimir.example.com/prometheus"
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`
	// Query is the PromQL expression. Must return results with an "image" label.
	// Example: count(container_memory_working_set_bytes{namespace="gitlab-runner"}) by (image)
	// +kubebuilder:validation:MinLength=1
	Query string `json:"query"`
	// QueryType controls how the query is executed: "range" or "instant". Default: "range".
	// +kubebuilder:default="range"
	// +optional
	QueryType QueryType `json:"queryType,omitempty"`
	// Lookback is the time window for range queries (start=now-lookback, end=now).
	// Required when queryType is "range". Ignored when queryType is "instant".
	// Example: "168h" (7 days), "24h", "72h"
	// +optional
	Lookback *metav1.Duration `json:"lookback,omitempty"`
	// Step is the resolution step for range queries.
	// Smaller steps increase data-point density but also increase Prometheus load.
	// Default: 5m. Example: "1m", "15m"
	// +optional
	Step *metav1.Duration `json:"step,omitempty"`
}

// LokiQueryType defines how the Loki query is executed.
// +kubebuilder:validation:Enum=range
type LokiQueryType string

const (
	// LokiQueryTypeRange uses /loki/api/v1/query_range with a lookback window.
	LokiQueryTypeRange LokiQueryType = "range"
)

// DiscoveryLokiQuery defines the Loki-specific query parameters.
type DiscoveryLokiQuery struct {
	// Endpoint is the Loki API URL.
	// Example: "https://loki.example.com"
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`
	// Query is the LogQL expression.
	// +kubebuilder:validation:MinLength=1
	Query string `json:"query"`
	// QueryType controls how the query is executed. Currently only "range" is supported.
	// +kubebuilder:default="range"
	// +optional
	QueryType LokiQueryType `json:"queryType,omitempty"`
	// Lookback is the time window for the query (start=now-lookback, end=now).
	// Example: "168h" (7 days), "24h"
	// +optional
	Lookback *metav1.Duration `json:"lookback,omitempty"`
	// Parser configures how log lines are parsed into structured event records.
	// +optional
	Parser *LokiParser `json:"parser,omitempty"`
}

// LokiParserType identifies how Loki log lines are parsed.
// +kubebuilder:validation:Enum=kubernetesEvents
type LokiParserType string

const (
	// LokiParserTypeKubernetesEvents parses Kubernetes Event log lines,
	// extracting pod name, reason, message, and image reference.
	LokiParserTypeKubernetesEvents LokiParserType = "kubernetesEvents"
)

// LokiParser configures structured parsing of Loki log entries.
type LokiParser struct {
	// Type selects the parser. Currently only "kubernetesEvents" is supported.
	// +kubebuilder:validation:Enum=kubernetesEvents
	Type LokiParserType `json:"type"`
	// PodField is the log label or field that contains the pod name.
	// Example: "involvedObject_name"
	// +optional
	PodField string `json:"podField,omitempty"`
	// ReasonField is the log label or field that contains the event reason.
	// Example: "reason"
	// +optional
	ReasonField string `json:"reasonField,omitempty"`
	// MessageField is the log label or field that contains the event message.
	// Example: "message"
	// +optional
	MessageField string `json:"messageField,omitempty"`
	// ImageField is the log label or field from which the image reference is extracted.
	// For kubernetesEvents, the image is parsed out of the message text.
	// Example: "message"
	// +optional
	ImageField string `json:"imageField,omitempty"`
}

// ============================================================
// Stage 2 — Signals
// ============================================================

// SignalType identifies the derivation method for a named signal.
// +kubebuilder:validation:Enum=aggregate;timeWeightedAggregate;windowAggregate;eventPullTime
type SignalType string

const (
	// SignalTypeAggregate aggregates all samples per image using a single method (sum, max, avg, count, min).
	SignalTypeAggregate SignalType = "aggregate"
	// SignalTypeTimeWeightedAggregate applies per-hour-window weights before aggregation.
	SignalTypeTimeWeightedAggregate SignalType = "timeWeightedAggregate"
	// SignalTypeWindowAggregate aggregates only the samples within a specific time sub-window.
	SignalTypeWindowAggregate SignalType = "windowAggregate"
	// SignalTypeEventPullTime derives image pull-time statistics from Loki event records.
	SignalTypeEventPullTime SignalType = "eventPullTime"
)

// AggregationMethod defines how data-point values are combined into a single per-image number.
// +kubebuilder:validation:Enum=sum;count;avg;max;min
type AggregationMethod string

const (
	// AggregationSum adds all data-point values.
	AggregationSum AggregationMethod = "sum"
	// AggregationCount counts the number of data points.
	AggregationCount AggregationMethod = "count"
	// AggregationAvg computes the arithmetic mean of all data-point values.
	AggregationAvg AggregationMethod = "avg"
	// AggregationMax takes the highest single data-point value.
	AggregationMax AggregationMethod = "max"
	// AggregationMin takes the lowest single data-point value.
	AggregationMin AggregationMethod = "min"
)

// DiscoverySignal defines a named per-image metric derived from a single query.
type DiscoverySignal struct {
	// Name is the unique identifier for this signal within the policy.
	// Ranking configurations reference signals by this name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Query is the name of the query that provides raw data for this signal.
	// Must match a queries[].name within the same policy.
	// +kubebuilder:validation:MinLength=1
	Query string `json:"query"`
	// Type selects the signal derivation method.
	// +kubebuilder:validation:Enum=aggregate;timeWeightedAggregate;windowAggregate;eventPullTime
	Type SignalType `json:"type"`
	// Aggregate is required when type=aggregate.
	// +optional
	Aggregate *AggregateSignalConfig `json:"aggregate,omitempty"`
	// TimeWeightedAggregate is required when type=timeWeightedAggregate.
	// +optional
	TimeWeightedAggregate *TimeWeightedAggregateSignalConfig `json:"timeWeightedAggregate,omitempty"`
	// WindowAggregate is required when type=windowAggregate.
	// +optional
	WindowAggregate *WindowAggregateSignalConfig `json:"windowAggregate,omitempty"`
	// EventPullTime is required when type=eventPullTime.
	// +optional
	EventPullTime *EventPullTimeSignalConfig `json:"eventPullTime,omitempty"`
}

// AggregateSignalConfig configures the aggregate signal type.
type AggregateSignalConfig struct {
	// Method is the aggregation function applied to all samples per image.
	// +kubebuilder:validation:Enum=sum;count;avg;max;min
	Method AggregationMethod `json:"method"`
}

// TimeWeightedAggregateSignalConfig configures the timeWeightedAggregate signal type.
// Each sample value is multiplied by the weight of the matching time window before aggregation.
type TimeWeightedAggregateSignalConfig struct {
	// Method is the aggregation function applied after weighting (currently only "sum" is meaningful).
	// +kubebuilder:validation:Enum=sum;count;avg;max;min
	Method AggregationMethod `json:"method"`
	// Timezone is the IANA time zone used to evaluate window boundaries (wall-clock hours).
	// Example: "Europe/Berlin", "America/New_York", "UTC"
	// +kubebuilder:validation:MinLength=1
	Timezone string `json:"timezone"`
	// DefaultWeight is applied to samples that do not fall in any configured window.
	// Use "0" to exclude off-hours samples entirely.
	DefaultWeight resource.Quantity `json:"defaultWeight"`
	// Windows is the list of hour-of-day windows with associated weights.
	// +kubebuilder:validation:MinItems=1
	Windows []TimeWeightedWindow `json:"windows"`
}

// TimeWeightedWindow defines a wall-clock hour range and its weight factor.
type TimeWeightedWindow struct {
	// StartHour is the inclusive start of the window in local time (0–23).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=23
	StartHour int32 `json:"startHour"`
	// EndHour is the exclusive end of the window in local time (1–24).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=24
	EndHour int32 `json:"endHour"`
	// Weight is the factor applied to sample values within this window.
	// Use "1.0" for full weight, "0.3" for partial, "0" to exclude.
	Weight resource.Quantity `json:"weight"`
}

// WindowAggregateSignalConfig configures the windowAggregate signal type.
// Exactly one of relativeWindow or (window + timezone) must be set.
type WindowAggregateSignalConfig struct {
	// Method is the aggregation function applied to the windowed samples.
	// +kubebuilder:validation:Enum=sum;count;avg;max;min
	Method AggregationMethod `json:"method"`
	// RelativeWindow aggregates only samples from the last N duration before now.
	// Mutually exclusive with window + timezone.
	// Example: "2h" (last 2 hours)
	// +optional
	RelativeWindow *metav1.Duration `json:"relativeWindow,omitempty"`
	// Timezone is the IANA time zone for evaluating wall-clock window boundaries.
	// Required when window is set.
	// +optional
	Timezone string `json:"timezone,omitempty"`
	// Window defines fixed wall-clock start/end times within each day.
	// Mutually exclusive with relativeWindow.
	// +optional
	Window *TimeOfDayWindow `json:"window,omitempty"`
}

// TimeOfDayWindow defines a fixed wall-clock time range within each day.
type TimeOfDayWindow struct {
	// Start is the inclusive start time in "HH:MM" format (24-hour, local time).
	// Example: "09:00"
	// +kubebuilder:validation:Pattern=`^([01][0-9]|2[0-3]):[0-5][0-9]$`
	Start string `json:"start"`
	// End is the exclusive end time in "HH:MM" format (24-hour, local time).
	// Example: "17:00"
	// +kubebuilder:validation:Pattern=`^([01][0-9]|2[0-3]):[0-5][0-9]$`
	End string `json:"end"`
}

// EventMetric selects which per-image quantity an event signal measures.
// Both quantities are derived from Pulled events.
// +kubebuilder:validation:Enum=pullTime;imageSize
type EventMetric string

const (
	// EventMetricPullTime measures cold-pull duration in seconds (from Pulled events).
	EventMetricPullTime EventMetric = "pullTime"
	// EventMetricImageSize measures image size in bytes (from Pulled event messages).
	EventMetricImageSize EventMetric = "imageSize"
)

// EventStatistic defines the aggregation applied to the selected metric's samples.
// +kubebuilder:validation:Enum=p50;p90;p95;avg;max;count
type EventStatistic string

const (
	// EventStatisticP50 is the median sample value.
	EventStatisticP50 EventStatistic = "p50"
	// EventStatisticP90 is the 90th-percentile sample value.
	EventStatisticP90 EventStatistic = "p90"
	// EventStatisticP95 is the 95th-percentile sample value.
	EventStatisticP95 EventStatistic = "p95"
	// EventStatisticAvg is the mean sample value.
	EventStatisticAvg EventStatistic = "avg"
	// EventStatisticMax is the maximum sample value.
	EventStatisticMax EventStatistic = "max"
	// EventStatisticCount is the number of samples.
	EventStatisticCount EventStatistic = "count"
)

// EventPullTimeSignalConfig configures the eventPullTime signal type.
// The referenced query must be a Loki query. Pull duration and image size are
// extracted from the same Pulled events; metric selects which one to rank on.
type EventPullTimeSignalConfig struct {
	// Metric selects which per-image quantity to aggregate. Defaults to pullTime,
	// which correlates strongly with cold-start cost. Use imageSize to rank by bytes.
	// +kubebuilder:default=pullTime
	// +optional
	Metric EventMetric `json:"metric,omitempty"`
	// Statistic selects how the metric's samples are aggregated per image.
	// +kubebuilder:validation:Enum=p50;p90;p95;avg;max;count
	Statistic EventStatistic `json:"statistic"`
}

// ============================================================
// Stage 3 — Ranking
// ============================================================

// RankingStrategy identifies which ranking algorithm is applied.
// +kubebuilder:validation:Enum=signal;weightedSum;modelExposure
type RankingStrategy string

const (
	// RankingStrategySignal ranks images directly by the value of a single signal.
	RankingStrategySignal RankingStrategy = "signal"
	// RankingStrategyWeightedSum combines normalized signals using a weighted sum.
	RankingStrategyWeightedSum RankingStrategy = "weightedSum"
	// RankingStrategyModelExposure ranks images by expected post-rotation cold-node exposure.
	RankingStrategyModelExposure RankingStrategy = "modelExposure"
)

// DiscoveryRanking defines how signals are combined into the final ordered image list.
type DiscoveryRanking struct {
	// Strategy selects the ranking algorithm.
	// +kubebuilder:validation:Enum=signal;weightedSum;modelExposure
	Strategy RankingStrategy `json:"strategy"`
	// Signal is the name of the signal whose values determine image rank.
	// Must match a signals[].name within the same policy. Required when strategy=signal.
	// +optional
	Signal string `json:"signal,omitempty"`
	// WeightedSum is required when strategy=weightedSum.
	// +optional
	WeightedSum *WeightedSumRankingConfig `json:"weightedSum,omitempty"`
	// ModelExposure is required when strategy=modelExposure.
	// +optional
	ModelExposure *ModelExposureRankingConfig `json:"modelExposure,omitempty"`
}

// NormalizeMethod defines how signal values are normalized before weighted combination.
// +kubebuilder:validation:Enum=minMax
type NormalizeMethod string

const (
	// NormalizeMethodMinMax applies min-max normalization: (x - min) / (max - min).
	// When all values are equal, normalized(x) = 1.
	NormalizeMethodMinMax NormalizeMethod = "minMax"
)

// MissingSignalBehavior defines what happens when an image has no value for a required signal.
// +kubebuilder:validation:Enum=zero;drop
type MissingSignalBehavior string

const (
	// MissingSignalBehaviorZero treats a missing signal value as zero.
	MissingSignalBehaviorZero MissingSignalBehavior = "zero"
	// MissingSignalBehaviorDrop removes the image from ranking if any required signal is missing.
	MissingSignalBehaviorDrop MissingSignalBehavior = "drop"
)

// WeightedSumTerm defines one signal contribution in a weightedSum ranking.
type WeightedSumTerm struct {
	// Signal is the name of the signal to include in the weighted sum.
	// Must match a signals[].name within the same policy.
	// +kubebuilder:validation:MinLength=1
	Signal string `json:"signal"`
	// Weight is the factor applied to the normalized signal value.
	// All weights should be non-negative; they do not need to sum to 1.
	// Example: "0.7"
	Weight resource.Quantity `json:"weight"`
}

// WeightedSumRankingConfig configures the weightedSum ranking strategy.
// Score = Σ weight_k * normalize(signal_k(image)).
type WeightedSumRankingConfig struct {
	// Normalize selects the normalization method applied to each signal before weighting.
	// Currently only "minMax" is supported.
	// +kubebuilder:validation:Enum=minMax
	// +kubebuilder:default="minMax"
	Normalize NormalizeMethod `json:"normalize"`
	// MissingSignal controls behavior when an image has no value for a required signal.
	// "zero" treats missing as 0; "drop" removes the image from ranking.
	// +kubebuilder:validation:Enum=zero;drop
	// +kubebuilder:default="zero"
	MissingSignal MissingSignalBehavior `json:"missingSignal"`
	// Terms is the list of signals and their weights.
	// +kubebuilder:validation:MinItems=1
	Terms []WeightedSumTerm `json:"terms"`
}

// ModelExposureRankingConfig configures the modelExposure ranking strategy.
// Score = J_target(I) * (1 - 1/N)^J_pre(I) * p_hat(I)
// where N is the node count (see NodeCountConfig), J_pre is pre-window usage,
// J_target is target-window usage, and p_hat is the pull-time signal value.
type ModelExposureRankingConfig struct {
	// Nodes determines N (the eligible node count) in the exposure formula,
	// either as a static count or dynamically via a label selector.
	// +optional
	Nodes *NodeCountConfig `json:"nodes,omitempty"`
	// PreWindowUsageSignal is the name of the signal representing usage before the target window.
	// Must match a signals[].name within the same policy.
	// +kubebuilder:validation:MinLength=1
	PreWindowUsageSignal string `json:"preWindowUsageSignal"`
	// TargetWindowUsageSignal is the name of the signal representing usage during the target window.
	// Must match a signals[].name within the same policy.
	// +kubebuilder:validation:MinLength=1
	TargetWindowUsageSignal string `json:"targetWindowUsageSignal"`
	// PullTimeSignal is the name of the signal providing per-image pull-time estimates.
	// Must match a signals[].name within the same policy.
	// +kubebuilder:validation:MinLength=1
	PullTimeSignal string `json:"pullTimeSignal"`
}

// NodeCountConfig determines N (the eligible node count) for the modelExposure formula.
// Provide a static Count, a dynamic Selector, or both (Selector wins; Count is the fallback).
type NodeCountConfig struct {
	// Count is the static number of eligible nodes.
	// Used when Selector is unset, or as a fallback if node discovery fails.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Count *int32 `json:"count,omitempty"`
	// Selector dynamically determines N by counting Ready nodes that match it via the
	// Kubernetes API at each sync. This is a standard node selector (the same shape used
	// by node affinity): nodeSelectorTerms are ORed, and within a term matchExpressions
	// (node labels) and matchFields (e.g. metadata.name) are ANDed. A nil selector counts
	// all Ready nodes. When set, it takes precedence over Count.
	// +optional
	Selector *corev1.NodeSelector `json:"selector,omitempty"`
}

// ============================================================
// Status
// ============================================================

// QueryResultStatus reports whether a named query succeeded or failed.
// +kubebuilder:validation:Enum=success;failed
type QueryResultStatus string

const (
	// QueryResultStatusSuccess indicates the query executed without errors.
	QueryResultStatusSuccess QueryResultStatus = "success"
	// QueryResultStatusFailed indicates the query encountered an error.
	QueryResultStatusFailed QueryResultStatus = "failed"
)

// QueryResult reports the outcome of a single named query execution.
type QueryResult struct {
	// Name matches the queries[].name that produced this result.
	Name string `json:"name"`
	// Type is the query backend type (prometheus, loki, or registry).
	Type DiscoveryQueryType `json:"type"`
	// Status is "success" or "failed".
	Status QueryResultStatus `json:"status"`
	// Message describes the failure reason when status=failed.
	// +optional
	Message string `json:"message,omitempty"`
}

// DiscoveredImage represents a single discovered and ranked image.
type DiscoveredImage struct {
	// Image is the fully qualified image reference.
	Image string `json:"image"`
	// Rank is the position of this image in the final ordered list (1 = highest score).
	Rank int32 `json:"rank"`
	// FinalScore is the computed ranking score as a decimal string.
	FinalScore string `json:"finalScore"`
}

// DiscoveryPolicyStatus defines the observed state of DiscoveryPolicy.
type DiscoveryPolicyStatus struct {
	// LastSyncTime is the timestamp of the last reconciliation attempt.
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`
	// QueryResults reports the outcome of each named query execution.
	// +optional
	QueryResults []QueryResult `json:"queryResults,omitempty"`
	// DiscoveredImages is the ordered list of discovered and ranked images.
	// +optional
	DiscoveredImages []DiscoveredImage `json:"discoveredImages,omitempty"`
	// ImageCount is the number of discovered images.
	// +optional
	ImageCount int32 `json:"imageCount,omitempty"`
	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,categories=drop
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Images",type=integer,JSONPath=`.status.imageCount`
// +kubebuilder:printcolumn:name="LastSync",type=date,JSONPath=`.status.lastSyncTime`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].message`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DiscoveryPolicy automatically discovers images from registries or Prometheus metrics.
type DiscoveryPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DiscoveryPolicySpec   `json:"spec,omitempty"`
	Status DiscoveryPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DiscoveryPolicyList contains a list of DiscoveryPolicy.
type DiscoveryPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DiscoveryPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &DiscoveryPolicy{}, &DiscoveryPolicyList{})
		return nil
	})
}
