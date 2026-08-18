/*
Copyright (c) 2026 Breee

SPDX-License-Identifier: MIT
*/

package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	dropv1alpha1 "github.com/corewire/drop/api/v1alpha1"
	"github.com/corewire/drop/internal/discovery"
	dropmetrics "github.com/corewire/drop/internal/metrics"
)

// DiscoveryPolicyReconciler reconciles a DiscoveryPolicy object
type DiscoveryPolicyReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	SecretNamespace string
}

const (
	reasonDNSError          = "DNSError"
	reasonConnectionRefused = "ConnectionRefused"
	secretHeaderPrefix      = "headers."

	defaultDiscoverySyncInterval = 30 * time.Minute
)

// +kubebuilder:rbac:groups=drop.corewire.io,resources=discoverypolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=drop.corewire.io,resources=discoverypolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=drop.corewire.io,resources=discoverypolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile executes the query/signal/ranking pipeline for a DiscoveryPolicy and updates status.
func (r *DiscoveryPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. Fetch DiscoveryPolicy
	dp := &dropv1alpha1.DiscoveryPolicy{}
	if err := r.Get(ctx, req.NamespacedName, dp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Skip the backend queries while the last successful sync is still fresh.
	// Reconcile is enqueued by more than the sync timer (operator restart, cache
	// resync, future watches); without this gate each of those re-runs every
	// query. Failed syncs deliberately fall through to the workqueue backoff.
	syncInterval := dp.Spec.SyncInterval.Duration
	if syncInterval == 0 {
		syncInterval = defaultDiscoverySyncInterval
	}
	if dp.Status.LastSyncTime != nil && meta.IsStatusConditionTrue(dp.Status.Conditions, conditionTypeReady) {
		if elapsed := time.Since(dp.Status.LastSyncTime.Time); elapsed < syncInterval {
			return ctrl.Result{RequeueAfter: syncInterval - elapsed}, nil
		}
	}

	log.Info("reconciling DiscoveryPolicy",
		"queries", len(dp.Spec.Queries),
		"signals", len(dp.Spec.Signals),
	)

	// 3. Execute pipeline
	// Resolve dynamic node count (modelExposure nodeSelector) into a spec copy so
	// the pure pipeline sees a concrete N. The live object is never mutated.
	spec := dp.Spec.DeepCopy()
	if err := r.resolveDynamicNodeCount(ctx, spec); err != nil {
		log.Error(err, "resolving dynamic node count; falling back to static nodeCount")
	}
	httpClientFunc := r.buildHTTPClientFunc(dp)
	result := discovery.ExecutePipeline(ctx, *spec, httpClientFunc)

	// 4. Build status patch
	patch := client.MergeFrom(dp.DeepCopy())
	now := metav1.Now()

	dp.Status.LastSyncTime = &now
	dp.Status.QueryResults = result.QueryResults
	dp.Status.DiscoveredImages = result.Images
	dp.Status.ImageCount = int32(len(result.Images))

	// Determine overall health from query results
	allHealthy, failReason, failMsg := summarizeQueryResults(result.QueryResults)

	// Emit per-query metrics
	for _, qr := range result.QueryResults {
		healthy := float64(0)
		if qr.Status == dropv1alpha1.QueryResultStatusSuccess {
			healthy = 1
		}
		dropmetrics.DiscoverySourceHealth.WithLabelValues(dp.Name, string(qr.Type), qr.Name).Set(healthy)
	}

	// 4. Set Ready condition
	readyCondition := metav1.Condition{
		Type:               conditionTypeReady,
		ObservedGeneration: dp.Generation,
		LastTransitionTime: now,
	}
	if allHealthy || len(result.Images) > 0 {
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = "Synced"
		readyCondition.Message = fmt.Sprintf("Pipeline executed successfully; %d images discovered.", len(result.Images))
	} else {
		readyCondition.Status = metav1.ConditionFalse
		readyCondition.Reason = failReason
		readyCondition.Message = failMsg
	}
	meta.SetStatusCondition(&dp.Status.Conditions, readyCondition)

	if err := r.Status().Patch(ctx, dp, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patching status: %w", err)
	}

	// 5. Requeue after sync interval
	// Return an error to trigger rate-limited backoff when all queries failed and no images available.
	if !allHealthy && len(result.Images) == 0 {
		return ctrl.Result{}, fmt.Errorf("discovery sync failed: %s", failMsg)
	}

	return ctrl.Result{RequeueAfter: jitterInterval(syncInterval, dp.Name)}, nil
}

// jitterInterval offsets the next sync by a stable per-policy amount within ±10%,
// so policies applied together (e.g. one GitOps sync) do not hit a shared backend
// in lockstep. Derived from the name rather than randomly so the offset survives
// operator restarts instead of re-clustering.
func jitterInterval(d time.Duration, name string) time.Duration {
	spread := int64(d) / 5
	if spread <= 0 {
		return d
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return d - time.Duration(spread/2) + time.Duration(h.Sum64()%uint64(spread))
}

// buildHTTPClientFunc returns a discovery.HTTPClientFunc that provides per-query auth/TLS clients.
func (r *DiscoveryPolicyReconciler) buildHTTPClientFunc(dp *dropv1alpha1.DiscoveryPolicy) discovery.HTTPClientFunc {
	// Build a name → secretRef index for quick lookup
	secretIndex := make(map[string]*corev1.LocalObjectReference, len(dp.Spec.Queries))
	for _, q := range dp.Spec.Queries {
		if q.SecretRef != nil {
			secretIndex[q.Name] = q.SecretRef
		}
	}

	return func(innerCtx context.Context, queryName string) (*http.Client, error) {
		secretRef, hasSecret := secretIndex[queryName]
		if !hasSecret {
			return &http.Client{Timeout: 30 * time.Second}, nil
		}
		return r.buildHTTPClient(innerCtx, secretRef)
	}
}

// resolveDynamicNodeCount counts Ready nodes matching the modelExposure node selector
// (if configured) and writes the result into spec.Ranking.ModelExposure.Nodes.Count.
// spec must be a deep copy — the live DiscoveryPolicy object is never mutated.
// When the selector is unset, the static count is left untouched.
func (r *DiscoveryPolicyReconciler) resolveDynamicNodeCount(ctx context.Context, spec *dropv1alpha1.DiscoveryPolicySpec) error {
	if spec.Ranking == nil || spec.Ranking.ModelExposure == nil {
		return nil
	}
	nodes := spec.Ranking.ModelExposure.Nodes
	if nodes == nil || nodes.Selector == nil {
		return nil
	}

	// Use the scheduler's own matcher so matchExpressions (labels) and matchFields
	// (e.g. metadata.name) are evaluated exactly as node affinity would.
	matcher, err := nodeaffinity.NewNodeSelector(nodes.Selector)
	if err != nil {
		return fmt.Errorf("invalid node selector: %w", err)
	}

	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return fmt.Errorf("listing nodes: %w", err)
	}

	count := int32(0)
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if isNodeReady(node) && matcher.Match(node) {
			count++
		}
	}
	nodes.Count = &count
	return nil
}

// isNodeReady reports whether a node has a Ready condition set to True.
func isNodeReady(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// summarizeQueryResults determines overall health and a human-readable reason/message.
func summarizeQueryResults(qrs []dropv1alpha1.QueryResult) (allHealthy bool, reason, message string) {
	if len(qrs) == 0 {
		return true, "Synced", "No queries configured."
	}

	var failures []string
	for _, qr := range qrs {
		if qr.Status != dropv1alpha1.QueryResultStatusSuccess {
			failures = append(failures, fmt.Sprintf("%s: %s", qr.Name, qr.Message))
		}
	}

	if len(failures) == 0 {
		return true, "Synced", ""
	}

	// Classify the first failure for the Reason field
	reason = classifyReason(failures[0])
	message = strings.Join(failures, "; ")
	return false, reason, message
}

// classifyReason maps a failure message to a k8s-style reason string.
func classifyReason(msg string) string {
	switch {
	case strings.Contains(msg, "no such host") || strings.Contains(msg, "server misbehaving") || strings.Contains(msg, "lookup"):
		return reasonDNSError
	case strings.Contains(msg, "connection refused"):
		return reasonConnectionRefused
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded"):
		return "Timeout"
	case strings.Contains(msg, "401") || strings.Contains(msg, "Unauthorized"):
		return "Unauthorized"
	case strings.Contains(msg, "403") || strings.Contains(msg, "Forbidden"):
		return "Forbidden"
	case strings.Contains(msg, "404") || strings.Contains(msg, "NotFound"):
		return "NotFound"
	case strings.Contains(msg, "certificate") || strings.Contains(msg, "x509"):
		return "TLSError"
	default:
		return "SyncFailed"
	}
}

// buildHTTPClient creates an HTTP client with auth/TLS from a Secret. The client
// always follows the Docker/OCI token workflow (a 401 "WWW-Authenticate: Bearer"
// challenge), so registry discovery works against token-auth registries such as
// GitLab, Docker Hub, GHCR and Artifactory — anonymously when no Secret is set,
// or authenticated with the Secret's basic credentials when one is.
func (r *DiscoveryPolicyReconciler) buildHTTPClient(ctx context.Context, secretRef *corev1.LocalObjectReference) (*http.Client, error) {
	httpClient := &http.Client{Timeout: 30 * time.Second}

	var secret *corev1.Secret
	if secretRef != nil {
		secret = &corev1.Secret{}
		secretNamespace := r.SecretNamespace
		if secretNamespace == "" {
			secretNamespace = "drop-system"
		}
		key := types.NamespacedName{Name: secretRef.Name, Namespace: secretNamespace}
		if err := r.Get(ctx, key, secret); err != nil {
			return nil, fmt.Errorf("fetching secret %s/%s: %w", secretNamespace, secretRef.Name, err)
		}
	}

	transport := &authTransport{
		base:   http.DefaultTransport,
		secret: secret,
	}

	// Configure TLS if cert data is present
	if secret != nil {
		if caCert, ok := secret.Data["ca.crt"]; ok {
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caCert)

			tlsConfig := &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,
			}

			if cert, ok := secret.Data["tls.crt"]; ok {
				if key, ok := secret.Data["tls.key"]; ok {
					clientCert, err := tls.X509KeyPair(cert, key)
					if err == nil {
						tlsConfig.Certificates = []tls.Certificate{clientCert}
					}
				}
			}

			transport.base = &http.Transport{TLSClientConfig: tlsConfig}
		}
	}

	httpClient.Transport = transport
	return httpClient, nil
}

// authTransport authenticates outgoing HTTP requests. It applies static
// credentials from a Secret (bearer token, basic auth, custom headers) and, for
// registries using the Docker/OCI token workflow, transparently follows a 401
// "WWW-Authenticate: Bearer" challenge to obtain and cache a short-lived token.
// The Secret may be nil, in which case only anonymous challenge-following is done.
type authTransport struct {
	base   http.RoundTripper
	secret *corev1.Secret

	mu     sync.Mutex
	tokens map[string]string // challenge (realm|service|scope) -> bearer token
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.applyStaticAuth(req)

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	// Follow the Docker/OCI token workflow: on a Bearer challenge, fetch a token
	// from the realm (once) and retry the request with it.
	if resp.StatusCode == http.StatusUnauthorized {
		if challenge := parseBearerChallenge(resp.Header.Get("WWW-Authenticate")); challenge != nil {
			token, terr := t.fetchToken(req.Context(), challenge)
			if terr == nil && token != "" {
				drainAndClose(resp)
				retry := req.Clone(req.Context())
				retry.Header.Set("Authorization", "Bearer "+token)
				return t.base.RoundTrip(retry)
			}
		}
	}

	return resp, nil
}

// applyStaticAuth sets static credentials from the Secret on the request.
func (t *authTransport) applyStaticAuth(req *http.Request) {
	if t.secret == nil {
		return
	}
	if token, ok := t.secret.Data["token"]; ok {
		req.Header.Set("Authorization", "Bearer "+string(token))
	}
	if username, ok := t.secret.Data["username"]; ok {
		if password, ok := t.secret.Data["password"]; ok {
			req.SetBasicAuth(string(username), string(password))
		}
	}
	for key, value := range t.secret.Data {
		if strings.HasPrefix(key, secretHeaderPrefix) {
			headerName := key[len(secretHeaderPrefix):]
			req.Header.Set(headerName, string(value))
		}
	}
}

// fetchToken performs the Docker/OCI token request against the challenge realm,
// authenticating with the Secret's basic credentials when present. Tokens are
// cached per (realm, service, scope) for the lifetime of the client.
func (t *authTransport) fetchToken(ctx context.Context, c *bearerChallenge) (string, error) {
	cacheKey := c.realm + "|" + c.service + "|" + c.scope

	t.mu.Lock()
	if tok, ok := t.tokens[cacheKey]; ok {
		t.mu.Unlock()
		return tok, nil
	}
	t.mu.Unlock()

	u, err := url.Parse(c.realm)
	if err != nil {
		return "", fmt.Errorf("parsing token realm: %w", err)
	}
	q := u.Query()
	if c.service != "" {
		q.Set("service", c.service)
	}
	if c.scope != "" {
		q.Set("scope", c.scope)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	// Authenticate the token request with basic credentials when available.
	if t.secret != nil {
		if username, ok := t.secret.Data["username"]; ok {
			if password, ok := t.secret.Data["password"]; ok {
				req.SetBasicAuth(string(username), string(password))
			}
		}
	}

	// Use the base transport directly to avoid recursing into RoundTrip.
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return "", err
	}
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("token endpoint returned status %d: %s", resp.StatusCode, string(body))
	}

	var tr struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tr); err != nil {
		return "", fmt.Errorf("decoding token response: %w", err)
	}
	token := tr.Token
	if token == "" {
		token = tr.AccessToken
	}
	if token == "" {
		return "", fmt.Errorf("token endpoint returned no token")
	}

	t.mu.Lock()
	if t.tokens == nil {
		t.tokens = make(map[string]string)
	}
	t.tokens[cacheKey] = token
	t.mu.Unlock()

	return token, nil
}

// bearerChallenge holds the parsed parameters of a "WWW-Authenticate: Bearer"
// challenge returned by a registry.
type bearerChallenge struct {
	realm   string
	service string
	scope   string
}

// parseBearerChallenge parses a `Bearer realm="...",service="...",scope="..."`
// header value. It returns nil when the header is absent, not a Bearer scheme,
// or missing a realm.
func parseBearerChallenge(header string) *bearerChallenge {
	const prefix = "bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return nil
	}

	c := &bearerChallenge{}
	for _, part := range splitOutsideQuotes(header[len(prefix):], ',') {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.Trim(strings.TrimSpace(kv[1]), `"`)
		switch strings.ToLower(key) {
		case "realm":
			c.realm = val
		case "service":
			c.service = val
		case "scope":
			c.scope = val
		}
	}

	if c.realm == "" {
		return nil
	}
	return c
}

// splitOutsideQuotes splits s on sep, ignoring separators inside double quotes.
func splitOutsideQuotes(s string, sep rune) []string {
	var parts []string
	var buf strings.Builder
	inQuotes := false
	for _, r := range s {
		switch {
		case r == '"':
			inQuotes = !inQuotes
			buf.WriteRune(r)
		case r == sep && !inQuotes:
			parts = append(parts, buf.String())
			buf.Reset()
		default:
			buf.WriteRune(r)
		}
	}
	if buf.Len() > 0 {
		parts = append(parts, buf.String())
	}
	return parts
}

// drainAndClose discards and closes a response body so the connection can be
// reused.
func drainAndClose(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
}

// SetupWithManager sets up the controller with the Manager.
func (r *DiscoveryPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Every reconcile writes status (LastSyncTime always moves), which would
		// re-trigger this watch and spin a hot loop. Only spec changes may enqueue;
		// periodic syncs come from RequeueAfter.
		For(&dropv1alpha1.DiscoveryPolicy{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("discoverypolicy").
		Complete(r)
}
