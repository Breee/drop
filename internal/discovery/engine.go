package discovery

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	dropv1alpha1 "github.com/corewire/drop/api/v1alpha1"
)

// QueryRawData holds raw per-image samples from a single query execution.
// For prometheus range queries each image may have multiple samples.
// For prometheus instant and registry queries each image has exactly one sample.
type QueryRawData struct {
	// Samples maps image reference → ordered list of (timestamp, value) pairs.
	// Timestamp is Unix seconds; value is the numeric sample value.
	Samples map[string][]TimedSample
	// QueryType is the DiscoveryQueryType that produced this data.
	QueryType dropv1alpha1.DiscoveryQueryType
}

// TimedSample pairs a Unix timestamp (seconds) with a float64 value.
type TimedSample struct {
	Timestamp float64
	Value     float64
}

// PipelineResult is the output of a full pipeline execution.
type PipelineResult struct {
	QueryResults []dropv1alpha1.QueryResult
	Images       []dropv1alpha1.DiscoveredImage
}

// HTTPClientFunc builds an HTTP client for a query (used by the controller to inject auth/TLS).
type HTTPClientFunc func(ctx context.Context, queryName string) (*http.Client, error)

// scoredItem is an intermediate ranked image used during the ranking stage.
type scoredItem struct {
	image string
	score float64
}

// ExecutePipeline runs all stages of the discovery pipeline and returns a PipelineResult.
//
// queryHTTPClient is called once per query to obtain an HTTP client with appropriate
// auth/TLS configuration. Pass nil to use a plain default client for every query.
func ExecutePipeline(
	ctx context.Context,
	spec dropv1alpha1.DiscoveryPolicySpec,
	queryHTTPClient HTTPClientFunc,
) PipelineResult {
	if queryHTTPClient == nil {
		queryHTTPClient = func(_ context.Context, _ string) (*http.Client, error) {
			return &http.Client{Timeout: 30 * time.Second}, nil
		}
	}

	// ──────────────────────────────────────────────────────────
	// Stage 1 — Execute queries
	// ──────────────────────────────────────────────────────────
	rawByQuery := make(map[string]*QueryRawData, len(spec.Queries))
	qResults := make([]dropv1alpha1.QueryResult, 0, len(spec.Queries))

	for _, q := range spec.Queries {
		httpClient, err := queryHTTPClient(ctx, q.Name)
		if err != nil {
			qResults = append(qResults, dropv1alpha1.QueryResult{
				Name:    q.Name,
				Type:    q.Type,
				Status:  dropv1alpha1.QueryResultStatusFailed,
				Message: fmt.Sprintf("building HTTP client: %v", err),
			})
			continue
		}

		raw, qr := executeQuery(ctx, q, httpClient)
		qResults = append(qResults, qr)
		if raw != nil {
			rawByQuery[q.Name] = raw
		}
	}

	// ──────────────────────────────────────────────────────────
	// Stage 2 — Derive signals
	// ──────────────────────────────────────────────────────────
	signalValues := make(map[string]map[string]float64, len(spec.Signals))
	qResultIndex := make(map[string]int, len(qResults))
	for i := range qResults {
		qResultIndex[qResults[i].Name] = i
	}

	for _, sig := range spec.Signals {
		raw, ok := rawByQuery[sig.Query]
		if !ok {
			continue
		}

		if !isSignalCompatibleWithQueryType(sig.Type, raw.QueryType) {
			if idx, found := qResultIndex[sig.Query]; found {
				msg := fmt.Sprintf(
					"signal %q type=%s is not compatible with query %q type=%s",
					sig.Name, sig.Type, sig.Query, raw.QueryType,
				)
				switch {
				case qResults[idx].Status == dropv1alpha1.QueryResultStatusSuccess:
					qResults[idx].Status = dropv1alpha1.QueryResultStatusFailed
					qResults[idx].Message = msg
				case qResults[idx].Message == "":
					qResults[idx].Message = msg
				default:
					qResults[idx].Message = qResults[idx].Message + "; " + msg
				}
			}
			continue
		}

		values := deriveSignal(sig, raw)
		if values != nil {
			signalValues[sig.Name] = values
		}
	}

	// ──────────────────────────────────────────────────────────
	// Stage 3 — Rank images
	// ──────────────────────────────────────────────────────────
	allImages := collectImages(rawByQuery)

	// Apply image filter
	if spec.ImageFilter != "" {
		re, err := regexp.Compile(spec.ImageFilter)
		if err == nil {
			var filtered []string
			for _, img := range allImages {
				if re.MatchString(img) {
					filtered = append(filtered, img)
				}
			}
			allImages = filtered
		}
	}

	discovered := rankImages(spec.Ranking, signalValues, allImages, defaultScores(rawByQuery))

	// Apply maxImages cap; mark selected
	maxImages := int(spec.MaxImages)
	if maxImages <= 0 {
		maxImages = 50
	}
	if len(discovered) > maxImages {
		discovered = discovered[:maxImages]
	}

	return PipelineResult{
		QueryResults: qResults,
		Images:       discovered,
	}
}

// isSignalCompatibleWithQueryType enforces meaningful signal/query combinations.
func isSignalCompatibleWithQueryType(sigType dropv1alpha1.SignalType, qType dropv1alpha1.DiscoveryQueryType) bool {
	switch sigType {
	case dropv1alpha1.SignalTypeAggregate:
		return true
	case dropv1alpha1.SignalTypeTimeWeightedAggregate, dropv1alpha1.SignalTypeWindowAggregate:
		// Registry queries fetch tag snapshots, not time series.
		return qType != dropv1alpha1.DiscoveryQueryTypeRegistry
	case dropv1alpha1.SignalTypeEventPullTime:
		return qType == dropv1alpha1.DiscoveryQueryTypeLoki
	default:
		return false
	}
}

// executeQuery fetches raw data for a single DiscoveryQuery.
func executeQuery(ctx context.Context, q dropv1alpha1.DiscoveryQuery, httpClient *http.Client) (*QueryRawData, dropv1alpha1.QueryResult) {
	qr := dropv1alpha1.QueryResult{Name: q.Name, Type: q.Type}

	switch q.Type {
	case dropv1alpha1.DiscoveryQueryTypePrometheus:
		if q.Prometheus == nil {
			qr.Status = dropv1alpha1.QueryResultStatusFailed
			qr.Message = "prometheus config is required when type=prometheus"
			return nil, qr
		}
		raw, err := executePrometheusQuery(ctx, q.Prometheus, httpClient)
		if err != nil {
			qr.Status = dropv1alpha1.QueryResultStatusFailed
			qr.Message = err.Error()
			return nil, qr
		}
		qr.Status = dropv1alpha1.QueryResultStatusSuccess
		return raw, qr

	case dropv1alpha1.DiscoveryQueryTypeRegistry:
		if q.Registry == nil {
			qr.Status = dropv1alpha1.QueryResultStatusFailed
			qr.Message = "registry config is required when type=registry"
			return nil, qr
		}
		raw, err := executeRegistryQuery(ctx, q.Registry, httpClient)
		if err != nil {
			qr.Status = dropv1alpha1.QueryResultStatusFailed
			qr.Message = err.Error()
			return nil, qr
		}
		qr.Status = dropv1alpha1.QueryResultStatusSuccess
		return raw, qr

	case dropv1alpha1.DiscoveryQueryTypeLoki:
		if q.Loki == nil {
			qr.Status = dropv1alpha1.QueryResultStatusFailed
			qr.Message = "loki config is required when type=loki"
			return nil, qr
		}
		raw, err := executeLokiQuery(ctx, q.Loki, httpClient)
		if err != nil {
			qr.Status = dropv1alpha1.QueryResultStatusFailed
			qr.Message = err.Error()
			return nil, qr
		}
		qr.Status = dropv1alpha1.QueryResultStatusSuccess
		return raw, qr

	default:
		qr.Status = dropv1alpha1.QueryResultStatusFailed
		qr.Message = fmt.Sprintf("unsupported query type: %s", q.Type)
		return nil, qr
	}
}

// executePrometheusQuery runs a Prometheus range or instant query and returns raw samples.
func executePrometheusQuery(ctx context.Context, cfg *dropv1alpha1.DiscoveryPrometheusQuery, httpClient *http.Client) (*QueryRawData, error) {
	var lookback time.Duration
	if cfg.Lookback != nil {
		lookback = cfg.Lookback.Duration
	}
	var step time.Duration
	if cfg.Step != nil {
		step = cfg.Step.Duration
	}

	src := NewPrometheusSource(cfg.Endpoint, cfg.Query, cfg.QueryType, lookback, nil, step, httpClient)
	results, err := src.FetchRaw(ctx)
	if err != nil {
		return nil, err
	}

	raw := &QueryRawData{
		Samples:   results,
		QueryType: dropv1alpha1.DiscoveryQueryTypePrometheus,
	}
	return raw, nil
}

// executeRegistryQuery lists tags from an OCI registry and returns raw samples.
func executeRegistryQuery(ctx context.Context, cfg *dropv1alpha1.DiscoveryRegistryQuery, httpClient *http.Client) (*QueryRawData, error) {
	src := NewRegistrySource(cfg.URL, cfg.Repositories, cfg.TagFilter, cfg.TagSeek, cfg.TopX, cfg.MaxScan, cfg.ImageTemplate, cfg.VersionPattern, httpClient)
	results, err := src.Fetch(ctx)
	if err != nil {
		return nil, err
	}

	raw := &QueryRawData{
		Samples:   make(map[string][]TimedSample, len(results)),
		QueryType: dropv1alpha1.DiscoveryQueryTypeRegistry,
	}
	now := float64(time.Now().Unix())
	for _, r := range results {
		raw.Samples[r.Image] = []TimedSample{{Timestamp: now, Value: float64(r.Score)}}
	}
	return raw, nil
}

// executeLokiQuery fetches log entries from Loki and returns raw per-image samples.
func executeLokiQuery(ctx context.Context, cfg *dropv1alpha1.DiscoveryLokiQuery, httpClient *http.Client) (*QueryRawData, error) {
	var lookback time.Duration
	if cfg.Lookback != nil {
		lookback = cfg.Lookback.Duration
	}
	src := NewLokiSource(cfg.Endpoint, cfg.Query, lookback, cfg.Parser, httpClient)
	results, err := src.FetchRaw(ctx)
	if err != nil {
		return nil, err
	}
	raw := &QueryRawData{
		Samples:   results,
		QueryType: dropv1alpha1.DiscoveryQueryTypeLoki,
	}
	return raw, nil
}

// deriveSignal computes per-image float64 values for a single signal.
func deriveSignal(sig dropv1alpha1.DiscoverySignal, raw *QueryRawData) map[string]float64 {
	switch sig.Type {
	case dropv1alpha1.SignalTypeAggregate:
		if sig.Aggregate == nil {
			return nil
		}
		return aggregateSamples(raw.Samples, sig.Aggregate.Method, nil)

	case dropv1alpha1.SignalTypeTimeWeightedAggregate:
		if sig.TimeWeightedAggregate == nil {
			return nil
		}
		values, err := deriveTimeWeightedAggregate(raw.Samples, sig.TimeWeightedAggregate)
		if err != nil {
			return nil
		}
		return values

	case dropv1alpha1.SignalTypeWindowAggregate:
		if sig.WindowAggregate == nil {
			return nil
		}
		values, err := deriveWindowAggregate(raw.Samples, sig.WindowAggregate)
		if err != nil {
			return nil
		}
		return values

	case dropv1alpha1.SignalTypeEventPullTime:
		if sig.EventPullTime == nil {
			return nil
		}
		return deriveEventPullTime(raw.Samples, sig.EventPullTime)

	default:
		return nil
	}
}

// aggregateSamples applies an AggregationMethod to per-image sample lists.
// cutoffUnix, when non-nil, excludes samples with timestamp < cutoffUnix.
func aggregateSamples(samples map[string][]TimedSample, method dropv1alpha1.AggregationMethod, cutoffUnix *float64) map[string]float64 {
	out := make(map[string]float64, len(samples))
	for image, pts := range samples {
		vals := make([]float64, 0, len(pts))
		for _, pt := range pts {
			if cutoffUnix != nil && pt.Timestamp < *cutoffUnix {
				continue
			}
			vals = append(vals, pt.Value)
		}
		if len(vals) == 0 {
			continue
		}
		out[image] = applyMethod(vals, method)
	}
	return out
}

// applyMethod applies a single AggregationMethod to a non-empty slice of values.
func applyMethod(vals []float64, method dropv1alpha1.AggregationMethod) float64 {
	switch method {
	case dropv1alpha1.AggregationCount:
		return float64(len(vals))
	case dropv1alpha1.AggregationAvg:
		var sum float64
		for _, v := range vals {
			sum += v
		}
		return sum / float64(len(vals))
	case dropv1alpha1.AggregationMax:
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m
	case dropv1alpha1.AggregationMin:
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m
	default: // sum
		var s float64
		for _, v := range vals {
			s += v
		}
		return s
	}
}

// deriveTimeWeightedAggregate applies per-hour weights before aggregating.
func deriveTimeWeightedAggregate(samples map[string][]TimedSample, cfg *dropv1alpha1.TimeWeightedAggregateSignalConfig) (map[string]float64, error) {
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, fmt.Errorf("loading timezone %q: %w", cfg.Timezone, err)
	}

	defaultWeightQ := cfg.DefaultWeight.AsApproximateFloat64()

	out := make(map[string]float64, len(samples))
	for image, pts := range samples {
		var weighted []float64
		for _, pt := range pts {
			t := time.Unix(int64(pt.Timestamp), 0).In(loc)
			hour := int32(t.Hour())

			w := defaultWeightQ
			for _, win := range cfg.Windows {
				if hour >= win.StartHour && hour < win.EndHour {
					w = win.Weight.AsApproximateFloat64()
					break
				}
			}
			weighted = append(weighted, pt.Value*w)
		}
		if len(weighted) == 0 {
			continue
		}
		out[image] = applyMethod(weighted, cfg.Method)
	}
	return out, nil
}

// deriveWindowAggregate aggregates only samples in a specific time window.
func deriveWindowAggregate(samples map[string][]TimedSample, cfg *dropv1alpha1.WindowAggregateSignalConfig) (map[string]float64, error) {
	now := time.Now().UTC()

	var cutoff *float64
	var windowEnd *float64

	if cfg.RelativeWindow != nil {
		c := float64(now.Add(-cfg.RelativeWindow.Duration).Unix())
		cutoff = &c
	} else if cfg.Window != nil {
		if cfg.Timezone == "" {
			return nil, fmt.Errorf("timezone is required when window is set")
		}
		loc, err := time.LoadLocation(cfg.Timezone)
		if err != nil {
			return nil, fmt.Errorf("loading timezone %q: %w", cfg.Timezone, err)
		}
		startT, err := parseTimeOfDay(cfg.Window.Start, now.In(loc))
		if err != nil {
			return nil, fmt.Errorf("parsing window start: %w", err)
		}
		endT, err := parseTimeOfDay(cfg.Window.End, now.In(loc))
		if err != nil {
			return nil, fmt.Errorf("parsing window end: %w", err)
		}
		c := float64(startT.Unix())
		e := float64(endT.Unix())
		cutoff = &c
		windowEnd = &e
	}

	out := make(map[string]float64, len(samples))
	for image, pts := range samples {
		vals := make([]float64, 0, len(pts))
		for _, pt := range pts {
			if cutoff != nil && pt.Timestamp < *cutoff {
				continue
			}
			if windowEnd != nil && pt.Timestamp > *windowEnd {
				continue
			}
			vals = append(vals, pt.Value)
		}
		if len(vals) == 0 {
			continue
		}
		out[image] = applyMethod(vals, cfg.Method)
	}
	return out, nil
}

// parseTimeOfDay parses a "HH:MM" time string relative to a reference day.
func parseTimeOfDay(hhmm string, ref time.Time) (time.Time, error) {
	parts := strings.SplitN(hhmm, ":", 2)
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("invalid time format %q (want HH:MM)", hhmm)
	}
	h, errH := strconv.Atoi(parts[0])
	m, errM := strconv.Atoi(parts[1])
	if errH != nil || errM != nil {
		return time.Time{}, fmt.Errorf("invalid time format %q (want HH:MM)", hhmm)
	}
	return time.Date(ref.Year(), ref.Month(), ref.Day(), h, m, 0, 0, ref.Location()), nil
}

// rankImages converts per-signal values into an ordered DiscoveredImage slice.
func rankImages(ranking *dropv1alpha1.DiscoveryRanking, signals map[string]map[string]float64, images []string, fallback map[string]float64) []dropv1alpha1.DiscoveredImage {
	if ranking == nil || ranking.Strategy == "" || len(images) == 0 {
		// No ranking configured: order by the per-query score (registry source
		// already returns its tags newest-first), then alphabetically. This lets
		// registry queries work without an explicit signal+ranking dance.
		sorted := append([]string(nil), images...)
		sort.Slice(sorted, func(i, j int) bool {
			si, sj := fallback[sorted[i]], fallback[sorted[j]]
			if si != sj {
				return si > sj
			}
			return sorted[i] < sorted[j]
		})
		out := make([]dropv1alpha1.DiscoveredImage, len(sorted))
		for i, img := range sorted {
			out[i] = dropv1alpha1.DiscoveredImage{
				Image:      img,
				Rank:       int32(i + 1),
				FinalScore: strconv.FormatFloat(fallback[img], 'f', -1, 64),
			}
		}
		return out
	}

	var items []scoredItem

	switch ranking.Strategy {
	case dropv1alpha1.RankingStrategySignal:
		sigMap := signals[ranking.Signal]
		for _, img := range images {
			v := sigMap[img]
			items = append(items, scoredItem{
				image: img,
				score: v,
			})
		}

	case dropv1alpha1.RankingStrategyWeightedSum:
		if ranking.WeightedSum != nil {
			items = weightedSumRank(ranking.WeightedSum, signals, images)
		}

	case dropv1alpha1.RankingStrategyModelExposure:
		if ranking.ModelExposure != nil {
			items = modelExposureRank(ranking.ModelExposure, signals, images)
		}

	default:
		// Unknown strategy: score 0
		for _, img := range images {
			items = append(items, scoredItem{image: img})
		}
	}

	// Sort descending by score, then alphabetically for stability
	sort.Slice(items, func(i, j int) bool {
		if items[i].score != items[j].score {
			return items[i].score > items[j].score
		}
		return items[i].image < items[j].image
	})

	out := make([]dropv1alpha1.DiscoveredImage, len(items))
	for i, it := range items {
		out[i] = dropv1alpha1.DiscoveredImage{
			Image:      it.image,
			Rank:       int32(i + 1),
			FinalScore: strconv.FormatFloat(it.score, 'f', -1, 64),
		}
	}
	return out
}

// weightedSumRank computes Score = Σ weight_k * normalize(signal_k(image)).
func weightedSumRank(cfg *dropv1alpha1.WeightedSumRankingConfig, signals map[string]map[string]float64, images []string) []scoredItem {
	// Compute min/max per signal for minMax normalization
	type minMax struct{ min, max float64 }
	bounds := make(map[string]minMax, len(cfg.Terms))
	for _, term := range cfg.Terms {
		sigMap := signals[term.Signal]
		var mn, mx float64
		first := true
		for _, img := range images {
			v, ok := sigMap[img]
			if !ok {
				continue
			}
			if first || v < mn {
				mn = v
			}
			if first || v > mx {
				mx = v
			}
			first = false
		}
		bounds[term.Signal] = minMax{min: mn, max: mx}
	}

	normalize := func(v float64, b minMax) float64 {
		if b.max == b.min {
			return 1.0
		}
		return (v - b.min) / (b.max - b.min)
	}

	var out []scoredItem
	for _, img := range images {
		var totalScore float64

		drop := false
		for _, term := range cfg.Terms {
			sigMap := signals[term.Signal]
			v, ok := sigMap[img]
			if !ok {
				if cfg.MissingSignal == dropv1alpha1.MissingSignalBehaviorDrop {
					drop = true
					break
				}
				v = 0
			}
			b := bounds[term.Signal]
			norm := normalize(v, b)
			wf := term.Weight.AsApproximateFloat64()
			totalScore += wf * norm
		}
		if drop {
			continue
		}
		out = append(out, scoredItem{
			image: img,
			score: totalScore,
		})
	}
	return out
}

// modelExposureRank computes Score = J_target * (1 - 1/N)^J_pre * p_hat.
func modelExposureRank(cfg *dropv1alpha1.ModelExposureRankingConfig, signals map[string]map[string]float64, images []string) []scoredItem {
	n := 1.0
	if cfg.Nodes != nil && cfg.Nodes.Count != nil && *cfg.Nodes.Count > 1 {
		n = float64(*cfg.Nodes.Count)
	}
	oneMinusInvN := 1.0 - 1.0/n

	preMap := signals[cfg.PreWindowUsageSignal]
	targetMap := signals[cfg.TargetWindowUsageSignal]
	pullMap := signals[cfg.PullTimeSignal]

	out := make([]scoredItem, 0, len(images))
	for _, img := range images {
		jPre := preMap[img]
		jTarget := targetMap[img]
		pHat := pullMap[img]

		score := jTarget * math.Pow(oneMinusInvN, jPre) * pHat

		out = append(out, scoredItem{
			image: img,
			score: score,
		})
	}
	return out
}

// collectImages returns a sorted, deduplicated list of all image references across all query results.
// For Loki query data, the per-image size suffix key (":size_bytes") is stripped to its base
// image name so that images are deduplicated correctly.
func collectImages(rawByQuery map[string]*QueryRawData) []string {
	seen := make(map[string]struct{})
	for _, raw := range rawByQuery {
		for img := range raw.Samples {
			if strings.HasSuffix(img, lokiSizeBytesSuffix) {
				seen[strings.TrimSuffix(img, lokiSizeBytesSuffix)] = struct{}{}
			} else {
				seen[img] = struct{}{}
			}
		}
	}
	images := make([]string, 0, len(seen))
	for img := range seen {
		images = append(images, img)
	}
	sort.Strings(images)
	return images
}

// defaultScores derives a fallback per-image score used when no ranking is
// configured. Each image is scored by the max value of its non-suffixed
// samples (registry queries store newest-first scores there), so registry
// queries rank correctly without an explicit signal+ranking definition.
func defaultScores(rawByQuery map[string]*QueryRawData) map[string]float64 {
	out := make(map[string]float64)
	for _, raw := range rawByQuery {
		for key, samples := range raw.Samples {
			if strings.HasSuffix(key, lokiSizeBytesSuffix) {
				continue
			}
			for _, s := range samples {
				if cur, ok := out[key]; !ok || s.Value > cur {
					out[key] = s.Value
				}
			}
		}
	}
	return out
}

// deriveEventPullTime computes per-image statistics from Loki event samples.
//
// The samples map is expected to come from a Loki kubernetesEvents query:
//   - samples[image]              → pull duration values in seconds (from Pulled events)
//   - samples[image+":size_bytes"]→ image size values in bytes (from Pulled event messages)
//
// cfg.Metric selects which series to aggregate; cfg.Statistic selects how.
func deriveEventPullTime(samples map[string][]TimedSample, cfg *dropv1alpha1.EventPullTimeSignalConfig) map[string]float64 {
	imageSet := make(map[string]struct{})
	for key := range samples {
		if strings.HasSuffix(key, lokiSizeBytesSuffix) {
			imageSet[strings.TrimSuffix(key, lokiSizeBytesSuffix)] = struct{}{}
		} else {
			imageSet[key] = struct{}{}
		}
	}

	metric := cfg.Metric
	if metric == "" {
		metric = dropv1alpha1.EventMetricPullTime
	}

	out := make(map[string]float64, len(imageSet))
	for img := range imageSet {
		var pts []TimedSample
		switch metric {
		case dropv1alpha1.EventMetricImageSize:
			pts = samples[img+lokiSizeBytesSuffix]
		default: // pullTime
			pts = samples[img]
		}
		if len(pts) == 0 {
			continue
		}
		vals := make([]float64, len(pts))
		for i, pt := range pts {
			vals[i] = pt.Value
		}
		out[img] = computeEventStat(vals, cfg.Statistic)
	}
	return out
}

// computeEventStat aggregates a non-empty slice using the configured statistic.
func computeEventStat(vals []float64, stat dropv1alpha1.EventStatistic) float64 {
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	switch stat {
	case dropv1alpha1.EventStatisticP50:
		return durationPercentile(sorted, 50)
	case dropv1alpha1.EventStatisticP90:
		return durationPercentile(sorted, 90)
	case dropv1alpha1.EventStatisticP95:
		return durationPercentile(sorted, 95)
	case dropv1alpha1.EventStatisticAvg:
		var sum float64
		for _, v := range sorted {
			sum += v
		}
		return sum / float64(len(sorted))
	case dropv1alpha1.EventStatisticMax:
		return sorted[len(sorted)-1]
	case dropv1alpha1.EventStatisticCount:
		return float64(len(sorted))
	default:
		return 0
	}
}

// durationPercentile returns the p-th percentile of a sorted slice using linear interpolation.
func durationPercentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 1 {
		return sorted[0]
	}
	rank := p / 100.0 * float64(n-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= n {
		return sorted[n-1]
	}
	return sorted[lo] + (rank-float64(lo))*(sorted[hi]-sorted[lo])
}
