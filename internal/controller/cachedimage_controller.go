/*
Copyright (c) 2026 Breee

SPDX-License-Identifier: MIT
*/

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dropv1alpha1 "github.com/corewire/drop/api/v1alpha1"
	dropmetrics "github.com/corewire/drop/internal/metrics"
	"github.com/corewire/drop/internal/pacing"
	"github.com/corewire/drop/internal/podbuilder"
)

const (
	conditionTypeReady        = "Ready"
	conditionTypePullProgress = "PullProgress"
	phasePending              = "Pending"
	phaseReady                = "Ready"
	phasePulling              = "Pulling"
	phaseDegraded             = "Degraded"
)

// CachedImageReconciler reconciles a CachedImage object
type CachedImageReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	PacingEngine *pacing.Engine
	Recorder     events.EventRecorder
	PodNamespace string
}

// +kubebuilder:rbac:groups=drop.corewire.io,resources=cachedimages,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=drop.corewire.io,resources=cachedimages/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=drop.corewire.io,resources=cachedimages/finalizers,verbs=update
// +kubebuilder:rbac:groups=drop.corewire.io,resources=pullpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// nodeState tracks the pull state for a single node.
type nodeState struct {
	pod         *corev1.Pod
	ready       bool
	failed      bool
	failReason  string // e.g. "ErrImagePull", "ImagePullBackOff", "PodFailed"
	failMessage string
}

// Reconcile moves the cluster state closer to the desired state for a CachedImage.
func (r *CachedImageReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// 1. Fetch CachedImage
	ci := &dropv1alpha1.CachedImage{}
	if err := r.Get(ctx, req.NamespacedName, ci); err != nil {
		if errors.IsNotFound(err) {
			// CachedImage was deleted — clean up any orphaned drop pods
			return ctrl.Result{}, r.cleanupOrphanPods(ctx, req.Name)
		}
		return ctrl.Result{}, err
	}

	// 2-3. Resolve target nodes
	targetNodes, err := r.resolveTargetNodes(ctx, ci)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 4. Fetch referenced PullPolicy
	policy, err := r.fetchPullPolicy(ctx, ci)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 5-6. Build per-node state from owned Pods
	stateMap, err := r.buildNodeStateMap(ctx, ci, targetNodes)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 6.5. If repull is due, mark cached nodes as needing re-pull
	r.markNodesForRepull(ci, policy, stateMap)

	// 7-8. Process pod states
	nodesReady, requeueNeeded := r.processPodStates(ctx, ci, stateMap)

	// 9-10. Schedule pulls for nodes that need them
	requeueAfter, pullRequeue, err := r.schedulePulls(ctx, ci, policy, stateMap)
	if err != nil {
		return ctrl.Result{}, err
	}
	requeueNeeded = requeueNeeded || pullRequeue

	// 11. Update status via patch (avoids conflict on rapid reconciles)
	nodesTargeted := int32(len(targetNodes))
	now := metav1.Now()
	patch := client.MergeFrom(ci.DeepCopy())
	r.updateCachedImageStatus(ci, stateMap, nodesTargeted, nodesReady, now)

	if err := r.Status().Patch(ctx, ci, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patching status: %w", err)
	}

	// Emit gauge metrics for observability
	dropmetrics.NodesTargeted.WithLabelValues(ci.Name).Set(float64(ci.Status.NodesTargeted))
	dropmetrics.NodesCached.WithLabelValues(ci.Name).Set(float64(ci.Status.NodesReady))
	dropmetrics.ConsecutiveFailures.WithLabelValues(ci.Name, ci.Spec.Image).Set(float64(ci.Status.ConsecutiveFailures))

	// 12. Determine requeue
	// If degraded with no running pods, apply exponential backoff based on PullPolicy config.
	if ci.Status.Phase == phaseDegraded && !requeueNeeded {
		backoff := computeBackoff(policy, ci.Status.ConsecutiveFailures)
		return ctrl.Result{RequeueAfter: backoff}, nil
	}

	if requeueNeeded {
		if requeueAfter == 0 {
			requeueAfter = 5 * time.Second
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	// If fully cached and repull is enabled, schedule next re-pull.
	if ci.Status.Phase == phaseReady {
		if interval := r.repullInterval(ci, policy); interval > 0 {
			return ctrl.Result{RequeueAfter: interval}, nil
		}
	}

	return ctrl.Result{}, nil
}

// computeBackoff calculates exponential backoff delay from PullPolicy config and failure count.
// Defaults: initial=30s, max=5m. Doubles on each consecutive failure.
func computeBackoff(policy *dropv1alpha1.PullPolicy, failures int32) time.Duration {
	initial := 30 * time.Second
	max := 5 * time.Minute

	if policy != nil && policy.Spec.FailureBackoff != nil {
		if policy.Spec.FailureBackoff.Initial.Duration > 0 {
			initial = policy.Spec.FailureBackoff.Initial.Duration
		}
		if policy.Spec.FailureBackoff.Max.Duration > 0 {
			max = policy.Spec.FailureBackoff.Max.Duration
		}
	}

	delay := initial
	for i := int32(1); i < failures; i++ {
		delay *= 2
		if delay > max {
			delay = max
			break
		}
	}

	return delay
}

// repullInterval returns the repull interval from the PullPolicy, or 0 if disabled.
func (r *CachedImageReconciler) repullInterval(_ *dropv1alpha1.CachedImage, policy *dropv1alpha1.PullPolicy) time.Duration {
	if policy == nil || policy.Spec.RepullInterval == nil {
		return 0
	}
	return policy.Spec.RepullInterval.Duration
}

// markNodesForRepull clears the ready state on cached nodes when a repull is due.
func (r *CachedImageReconciler) markNodesForRepull(ci *dropv1alpha1.CachedImage, policy *dropv1alpha1.PullPolicy, stateMap map[string]*nodeState) {
	interval := r.repullInterval(ci, policy)
	if interval <= 0 {
		return
	}
	// Check if enough time has passed since last successful pull
	if ci.Status.LastPulledAt == nil {
		return
	}
	elapsed := time.Since(ci.Status.LastPulledAt.Time)
	if elapsed < interval {
		return
	}
	// Time to re-pull: clear ready state on nodes that have no active pod
	for _, state := range stateMap {
		if state.ready && state.pod == nil {
			state.ready = false
		}
	}
}

// resolveTargetNodes lists and filters nodes matching the CachedImage spec.
func (r *CachedImageReconciler) resolveTargetNodes(ctx context.Context, ci *dropv1alpha1.CachedImage) ([]corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	listOpts := &client.ListOptions{}
	if len(ci.Spec.NodeSelector) > 0 {
		listOpts.LabelSelector = labels.SelectorFromSet(ci.Spec.NodeSelector)
	}
	if err := r.List(ctx, nodeList, listOpts); err != nil {
		return nil, fmt.Errorf("listing nodes: %w", err)
	}
	return filterNodesByTolerations(nodeList.Items, ci.Spec.Tolerations), nil
}

// fetchPullPolicy retrieves the referenced PullPolicy, if any.
func (r *CachedImageReconciler) fetchPullPolicy(ctx context.Context, ci *dropv1alpha1.CachedImage) (*dropv1alpha1.PullPolicy, error) {
	if ci.Spec.PolicyRef == nil {
		return nil, nil
	}
	log := logf.FromContext(ctx)
	policy := &dropv1alpha1.PullPolicy{}
	policyKey := client.ObjectKey{Name: ci.Spec.PolicyRef.Name}
	if err := r.Get(ctx, policyKey, policy); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("fetching PullPolicy: %w", err)
		}
		log.Info("referenced PullPolicy not found, using defaults", "policy", ci.Spec.PolicyRef.Name)
		return nil, nil
	}
	return policy, nil
}

// buildNodeStateMap creates the per-node state map from owned Pods.
func (r *CachedImageReconciler) buildNodeStateMap(ctx context.Context, ci *dropv1alpha1.CachedImage, targetNodes []corev1.Node) (map[string]*nodeState, error) {
	log := logf.FromContext(ctx)

	podList := &corev1.PodList{}
	ns := r.PodNamespace
	if ns == "" {
		ns = podbuilder.DefaultPodNamespace
	}
	if err := r.List(ctx, podList, client.InNamespace(ns), client.MatchingLabels{
		podbuilder.LabelManagedBy:   podbuilder.LabelManagedByValue,
		podbuilder.LabelCachedImage: ci.Name,
	}); err != nil {
		return nil, fmt.Errorf("listing owned pods: %w", err)
	}

	// Build set of previously cached nodes from status
	cachedSet := make(map[string]struct{}, len(ci.Status.CachedNodes))
	for _, n := range ci.Status.CachedNodes {
		cachedSet[n] = struct{}{}
	}

	stateMap := make(map[string]*nodeState, len(targetNodes))
	for i := range targetNodes {
		ns := &nodeState{}
		// Mark as ready if previously cached
		if _, ok := cachedSet[targetNodes[i].Name]; ok {
			ns.ready = true
		}
		stateMap[targetNodes[i].Name] = ns
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		nodeName := pod.Labels[podbuilder.LabelNode]
		state, exists := stateMap[nodeName]
		if !exists {
			if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
				log.Error(err, "deleting orphan pod", "pod", pod.Name)
			}
			continue
		}
		state.pod = pod
	}

	return stateMap, nil
}

// processPodStates evaluates completed/failed/running pods and returns ready count.
func (r *CachedImageReconciler) processPodStates(ctx context.Context, ci *dropv1alpha1.CachedImage, stateMap map[string]*nodeState) (int32, bool) {
	log := logf.FromContext(ctx)
	var nodesReady int32
	var requeueNeeded bool

	for nodeName, state := range stateMap {
		// Count nodes already cached (from previous reconciles)
		if state.ready && state.pod == nil {
			nodesReady++
			continue
		}

		if state.pod == nil {
			continue
		}

		switch state.pod.Status.Phase {
		case corev1.PodSucceeded:
			state.ready = true
			nodesReady++
			// Capture the resolved digest from the container runtime
			if digest := extractResolvedDigest(state.pod); digest != "" {
				ci.Status.ResolvedDigest = digest
			}
			dropmetrics.ActivePulls.Dec()
			dropmetrics.ImagesCachedTotal.WithLabelValues(ci.Spec.Image, nodeName).Inc()
			r.Recorder.Eventf(ci, nil, corev1.EventTypeNormal, "PullSucceeded", "CacheImage", "Image %s cached on node %s", ci.Spec.Image, nodeName)
			if err := r.Delete(ctx, state.pod); client.IgnoreNotFound(err) != nil {
				log.Error(err, "deleting succeeded pod", "pod", state.pod.Name, "node", nodeName)
			}
		case corev1.PodFailed:
			state.failed = true
			state.failReason, state.failMessage = extractPodFailureReason(state.pod)
			dropmetrics.ActivePulls.Dec()
			dropmetrics.PullErrorsTotal.WithLabelValues(ci.Spec.Image, nodeName).Inc()
			r.Recorder.Eventf(ci, nil, corev1.EventTypeWarning, state.failReason, "CacheImage", "Failed to pull image %s on node %s: %s", ci.Spec.Image, nodeName, state.failMessage)
			log.Info("drop pod failed", "pod", state.pod.Name, "node", nodeName, "reason", state.failReason)
			if err := r.Delete(ctx, state.pod); client.IgnoreNotFound(err) != nil {
				log.Error(err, "deleting failed pod", "pod", state.pod.Name, "node", nodeName)
			}
		case corev1.PodRunning, corev1.PodPending:
			// Check for image pull errors on waiting containers
			if reason, msg := extractContainerWaitingReason(state.pod); reason != "" {
				state.failed = true
				state.failReason = reason
				state.failMessage = msg
				dropmetrics.ActivePulls.Dec()
				dropmetrics.PullErrorsTotal.WithLabelValues(ci.Spec.Image, nodeName).Inc()
				r.Recorder.Eventf(ci, nil, corev1.EventTypeWarning, reason, "CacheImage", "Image %s on node %s: %s", ci.Spec.Image, nodeName, msg)
				// Delete the stuck pod; backoff retry will create a new one
				if err := r.Delete(ctx, state.pod); client.IgnoreNotFound(err) != nil {
					log.Error(err, "deleting stuck pod", "pod", state.pod.Name, "node", nodeName)
				}
			} else {
				requeueNeeded = true
			}
		}
	}

	return nodesReady, requeueNeeded
}

// extractContainerWaitingReason checks init/regular container statuses for image pull errors.
func extractContainerWaitingReason(pod *corev1.Pod) (string, string) {
	for _, cs := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
		if cs.State.Waiting != nil {
			switch cs.State.Waiting.Reason {
			case "ErrImagePull", "ImagePullBackOff", "InvalidImageName", "RegistryUnavailable":
				return cs.State.Waiting.Reason, cleanPullMessage(cs.State.Waiting.Message)
			}
		}
	}
	return "", ""
}

// cleanPullMessage extracts the root cause from verbose kubelet error chains.
// Input like: Back-off pulling image "img": ErrImagePull: failed to pull and unpack image "img":
//
//	failed to resolve reference "img": failed to do request: Head "https://...":
//	dial tcp: lookup registry.invalid.local on 172.30.0.1:53: server misbehaving
//
// Output: "dns: cannot resolve registry.invalid.local"
func cleanPullMessage(msg string) string {
	lower := strings.ToLower(msg)

	// DNS errors
	if strings.Contains(lower, "no such host") || strings.Contains(lower, "server misbehaving") {
		if host := extractHostFromPullError(msg); host != "" {
			return fmt.Sprintf("dns: cannot resolve %s", host)
		}
	}

	// Connection refused
	if strings.Contains(lower, "connection refused") {
		if host := extractHostFromPullError(msg); host != "" {
			return fmt.Sprintf("connection refused: %s", host)
		}
	}

	// TLS errors
	if strings.Contains(lower, "x509") || strings.Contains(lower, "certificate") {
		return "tls: certificate error"
	}

	// Timeout
	if strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline exceeded") {
		if host := extractHostFromPullError(msg); host != "" {
			return fmt.Sprintf("timeout connecting to %s", host)
		}
		return "timeout"
	}

	// Auth errors
	if strings.Contains(lower, "401") || strings.Contains(lower, "unauthorized") {
		return "unauthorized: check imagePullSecrets"
	}
	if strings.Contains(lower, "403") || strings.Contains(lower, "forbidden") {
		return "forbidden: access denied"
	}

	// 404 / not found
	if strings.Contains(lower, "not found") || strings.Contains(lower, "404") || strings.Contains(lower, "manifest unknown") {
		return "image not found"
	}

	// Fallback: take the last meaningful segment
	parts := strings.Split(msg, ": ")
	if len(parts) > 2 {
		return strings.Join(parts[len(parts)-2:], ": ")
	}
	if len(msg) > 120 {
		return msg[:120] + "..."
	}
	return msg
}

// extractHostFromPullError pulls the registry host from a kubelet pull error message.
func extractHostFromPullError(msg string) string {
	// Look for "lookup <host> on" pattern
	if idx := strings.Index(msg, "lookup "); idx != -1 {
		rest := msg[idx+len("lookup "):]
		if end := strings.IndexAny(rest, " :"); end != -1 {
			return rest[:end]
		}
	}
	// Look for "https://<host>" or "http://<host>"
	for _, scheme := range []string{"https://", "http://"} {
		if idx := strings.Index(msg, scheme); idx != -1 {
			rest := msg[idx+len(scheme):]
			if end := strings.IndexAny(rest, "/?\" "); end != -1 {
				return rest[:end]
			}
		}
	}
	return ""
}

// extractResolvedDigest extracts the image digest from a succeeded pod's container status.
// The kubelet reports the resolved imageID as "docker-pullable://image@sha256:abc..." or "image@sha256:abc...".
func extractResolvedDigest(pod *corev1.Pod) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.ImageID != "" {
			// ImageID is typically "docker-pullable://registry/repo@sha256:..." or "registry/repo@sha256:..."
			if idx := strings.Index(cs.ImageID, "sha256:"); idx != -1 {
				return cs.ImageID[idx:]
			}
		}
	}
	return ""
}

// extractPodFailureReason extracts a reason from a failed pod's container statuses or status message.
func extractPodFailureReason(pod *corev1.Pod) (string, string) {
	// Check terminated container reasons first
	for _, cs := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return cs.State.Terminated.Reason, cleanPullMessage(cs.State.Terminated.Message)
		}
	}
	// Fall back to pod status reason/message
	if pod.Status.Reason != "" {
		return pod.Status.Reason, cleanPullMessage(pod.Status.Message)
	}
	return "PodFailed", cleanPullMessage(pod.Status.Message)
}

// schedulePulls creates drop pods for nodes that need them, respecting pacing.
func (r *CachedImageReconciler) schedulePulls(ctx context.Context, ci *dropv1alpha1.CachedImage, policy *dropv1alpha1.PullPolicy, stateMap map[string]*nodeState) (time.Duration, bool, error) {
	log := logf.FromContext(ctx)
	var requeueAfter time.Duration
	var requeueNeeded bool

	// If any node failed THIS reconcile, don't create new pods.
	// The image is broken — it will fail on all nodes. Let the requeue timer handle retry.
	for _, state := range stateMap {
		if state.failed {
			log.V(1).Info("failure observed this reconcile, skipping all pulls")
			return 0, false, nil
		}
	}

	// If we have consecutive failures from previous reconciles, enforce backoff.
	if ci.Status.ConsecutiveFailures > 0 {
		backoff := computeBackoff(policy, ci.Status.ConsecutiveFailures)
		if ci.Status.LastAttemptedAt != nil {
			elapsed := time.Since(ci.Status.LastAttemptedAt.Time)
			if elapsed < backoff {
				remaining := backoff - elapsed
				log.V(1).Info("in backoff period, skipping pulls", "remaining", remaining, "failures", ci.Status.ConsecutiveFailures)
				return remaining, true, nil
			}
		} else {
			// No LastAttemptedAt yet (pre-existing resource) — backoff and let status patch set it.
			log.V(1).Info("backoff: no lastAttemptedAt, will set on next status patch", "failures", ci.Status.ConsecutiveFailures)
			return backoff, true, nil
		}
	}

	// Determine how many new pulls may start this reconcile for this image.
	decision, err := r.PacingEngine.PullSlots(ctx, policy, ci.Name)
	if err != nil {
		return 0, false, fmt.Errorf("checking pacing: %w", err)
	}

	if decision.Slots <= 0 {
		// A concurrency cap or the wave stagger is active — requeue and retry.
		if decision.RequeueIn > requeueAfter {
			requeueAfter = decision.RequeueIn
		}
		// Only request a requeue if some node still needs a pull.
		for _, state := range stateMap {
			if !state.ready && state.pod == nil && !state.failed {
				requeueNeeded = true
				break
			}
		}
		return requeueAfter, requeueNeeded, nil
	}

	// Create up to `Slots` pods this reconcile (per-image fan-out wave).
	created := 0
	for nodeName, state := range stateMap {
		if state.ready || state.pod != nil || state.failed {
			continue
		}
		if created >= decision.Slots {
			// More nodes need this image but the wave is full — requeue.
			requeueNeeded = true
			break
		}

		pod, err := podbuilder.BuildDropPod(ci, nodeName, r.PodNamespace)
		if err != nil {
			return 0, false, fmt.Errorf("building drop pod: %w", err)
		}

		if err := r.Create(ctx, pod); err != nil {
			if !errors.IsAlreadyExists(err) {
				return 0, false, fmt.Errorf("creating drop pod: %w", err)
			}
		} else {
			// Mark the attempt time so backoff is measured from now
			now := metav1.Now()
			ci.Status.LastAttemptedAt = &now
			dropmetrics.ActivePulls.Inc()
			r.Recorder.Eventf(ci, nil, corev1.EventTypeNormal, "PullStarted", "CacheImage", "Started pulling image %s on node %s", ci.Spec.Image, nodeName)
			log.Info("created drop pod", "pod", pod.Name, "node", nodeName, "image", ci.Spec.Image)
			created++
		}

		requeueNeeded = true
	}

	return requeueAfter, requeueNeeded, nil
}

// updateCachedImageStatus computes and sets the status fields on the CachedImage.
func (r *CachedImageReconciler) updateCachedImageStatus(ci *dropv1alpha1.CachedImage, stateMap map[string]*nodeState, nodesTargeted, nodesReady int32, now metav1.Time) {
	phase, nodesPulling, newFailureObserved, failReason, failMessage := computePhase(ci, stateMap, nodesTargeted, nodesReady)

	// Persist the list of nodes that have successfully cached the image
	cachedNodes := make([]string, 0, nodesReady)
	for nodeName, state := range stateMap {
		if state.ready {
			cachedNodes = append(cachedNodes, nodeName)
		}
	}

	ci.Status.ObservedGeneration = ci.Generation
	ci.Status.NodesTargeted = nodesTargeted
	ci.Status.NodesReady = nodesReady
	ci.Status.NodesPulling = nodesPulling
	ci.Status.Ready = fmt.Sprintf("%d/%d", nodesReady, nodesTargeted)
	ci.Status.CachedNodes = cachedNodes
	ci.Status.Phase = phase

	// Track consecutive failures for backoff calculation.
	if newFailureObserved {
		ci.Status.ConsecutiveFailures++
		ci.Status.LastAttemptedAt = &now
	} else if phase == phaseReady {
		ci.Status.ConsecutiveFailures = 0
	}

	if nodesReady > 0 {
		ci.Status.LastPulledAt = &now
	}

	meta.SetStatusCondition(&ci.Status.Conditions, buildReadyCondition(ci.Generation, now, phase, nodesTargeted, nodesReady, failReason, failMessage))
	meta.SetStatusCondition(&ci.Status.Conditions, buildPullProgressCondition(ci.Generation, now, phase, nodesPulling, nodesTargeted, nodesReady))
}

// computePhase determines the current phase and failure state from the node state map.
func computePhase(ci *dropv1alpha1.CachedImage, stateMap map[string]*nodeState, nodesTargeted, nodesReady int32) (phase string, nodesPulling int32, newFailureObserved bool, failReason, failMessage string) {
	phase = phasePending
	if nodesReady == nodesTargeted && nodesTargeted > 0 {
		phase = phaseReady
	} else if nodesReady > 0 {
		phase = phasePulling
	}

	for _, state := range stateMap {
		if state.pod != nil && !state.ready && !state.failed {
			nodesPulling++
		}
	}

	for _, state := range stateMap {
		if state.failed && !state.ready {
			phase = phaseDegraded
			newFailureObserved = true
			if state.failReason != "" && failReason == "" {
				failReason = state.failReason
				failMessage = state.failMessage
			}
		}
	}

	if nodesPulling > 0 && phase != phaseDegraded {
		phase = phasePulling
	}

	if !newFailureObserved && ci.Status.ConsecutiveFailures > 0 && phase != phaseReady {
		phase = phaseDegraded
		if existing := meta.FindStatusCondition(ci.Status.Conditions, conditionTypeReady); existing != nil && existing.Status == metav1.ConditionFalse {
			failReason = existing.Reason
			failMessage = existing.Message
		}
	}

	return phase, nodesPulling, newFailureObserved, failReason, failMessage
}

// buildReadyCondition constructs the Ready status condition.
func buildReadyCondition(generation int64, now metav1.Time, phase string, nodesTargeted, nodesReady int32, failReason, failMessage string) metav1.Condition {
	c := metav1.Condition{
		Type:               conditionTypeReady,
		ObservedGeneration: generation,
		LastTransitionTime: now,
	}
	switch {
	case phase == phaseReady:
		c.Status = metav1.ConditionTrue
		c.Reason = "Cached"
		c.Message = fmt.Sprintf("Image cached on all %d target nodes", nodesTargeted)
	case phase == phaseDegraded && failReason != "":
		c.Status = metav1.ConditionFalse
		c.Reason = failReason
		if failMessage != "" {
			c.Message = failMessage
		} else {
			c.Message = fmt.Sprintf("%d/%d nodes ready", nodesReady, nodesTargeted)
		}
	case phase == phaseDegraded:
		c.Status = metav1.ConditionFalse
		c.Reason = "PullFailed"
		c.Message = fmt.Sprintf("%d/%d nodes ready", nodesReady, nodesTargeted)
	default:
		c.Status = metav1.ConditionFalse
		c.Reason = "InProgress"
		c.Message = fmt.Sprintf("%d/%d nodes ready", nodesReady, nodesTargeted)
	}
	return c
}

// buildPullProgressCondition constructs the PullProgress status condition.
func buildPullProgressCondition(generation int64, now metav1.Time, phase string, nodesPulling, nodesTargeted, nodesReady int32) metav1.Condition {
	c := metav1.Condition{
		Type:               conditionTypePullProgress,
		ObservedGeneration: generation,
		LastTransitionTime: now,
	}
	switch {
	case nodesPulling > 0:
		c.Status = metav1.ConditionTrue
		c.Reason = "Pulling"
		c.Message = fmt.Sprintf("Actively pulling on %d node(s), %d/%d complete", nodesPulling, nodesReady, nodesTargeted)
	case phase == phaseReady:
		c.Status = metav1.ConditionFalse
		c.Reason = "Complete"
		c.Message = "All pulls complete"
	case phase == phaseDegraded:
		c.Status = metav1.ConditionFalse
		c.Reason = "Stalled"
		c.Message = fmt.Sprintf("Pull stalled: %d/%d nodes ready, retrying with backoff", nodesReady, nodesTargeted)
	default:
		c.Status = metav1.ConditionFalse
		c.Reason = "Idle"
		c.Message = "Waiting to start pulls"
	}
	return c
}

// filterNodesByTolerations returns nodes whose taints are tolerated.
func filterNodesByTolerations(nodes []corev1.Node, tolerations []corev1.Toleration) []corev1.Node {
	if len(tolerations) == 0 {
		// If no tolerations, only accept nodes without NoSchedule/NoExecute taints
		var result []corev1.Node
		for i := range nodes {
			if !hasUntoleratableTaints(nodes[i].Spec.Taints) {
				result = append(result, nodes[i])
			}
		}
		return result
	}

	var result []corev1.Node
	for i := range nodes {
		if allTaintsTolerated(nodes[i].Spec.Taints, tolerations) {
			result = append(result, nodes[i])
		}
	}
	return result
}

// hasUntoleratableTaints checks if any taint prevents scheduling.
func hasUntoleratableTaints(taints []corev1.Taint) bool {
	for _, taint := range taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			return true
		}
	}
	return false
}

// allTaintsTolerated checks if all NoSchedule/NoExecute taints are tolerated.
func allTaintsTolerated(taints []corev1.Taint, tolerations []corev1.Toleration) bool {
	for _, taint := range taints {
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if !taintTolerated(taint, tolerations) {
			return false
		}
	}
	return true
}

// taintTolerated checks if a single taint is tolerated by any toleration.
func taintTolerated(taint corev1.Taint, tolerations []corev1.Toleration) bool {
	for _, toleration := range tolerations {
		if toleration.Operator == corev1.TolerationOpExists {
			if toleration.Key == "" {
				return true // Tolerates everything
			}
			if toleration.Key == taint.Key {
				if toleration.Effect == "" || toleration.Effect == taint.Effect {
					return true
				}
			}
		}
		if toleration.Operator == corev1.TolerationOpEqual || toleration.Operator == "" {
			if toleration.Key == taint.Key && toleration.Value == taint.Value {
				if toleration.Effect == "" || toleration.Effect == taint.Effect {
					return true
				}
			}
		}
	}
	return false
}

// cleanupOrphanPods deletes all drop pods that reference a deleted CachedImage.
func (r *CachedImageReconciler) cleanupOrphanPods(ctx context.Context, cachedImageName string) error {
	log := logf.FromContext(ctx)
	ns := r.PodNamespace
	if ns == "" {
		ns = podbuilder.DefaultPodNamespace
	}
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(ns), client.MatchingLabels{
		podbuilder.LabelManagedBy:   podbuilder.LabelManagedByValue,
		podbuilder.LabelCachedImage: cachedImageName,
	}); err != nil {
		return fmt.Errorf("listing orphan pods: %w", err)
	}
	for i := range podList.Items {
		log.Info("deleting orphan pod", "pod", podList.Items[i].Name, "cachedImage", cachedImageName)
		if err := r.Delete(ctx, &podList.Items[i]); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting orphan pod %s: %w", podList.Items[i].Name, err)
		}
	}
	return nil
}

func (r *CachedImageReconciler) mapNodeToCachedImages(ctx context.Context, _ client.Object) []reconcile.Request {
	ciList := &dropv1alpha1.CachedImageList{}
	if err := r.List(ctx, ciList); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0, len(ciList.Items))
	for i := range ciList.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: ciList.Items[i].Name}})
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *CachedImageReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dropv1alpha1.CachedImage{}).
		// Watch nodes so scale-out/scale-in events trigger reconciliation immediately.
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.mapNodeToCachedImages),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(event.CreateEvent) bool { return true },
				DeleteFunc:  func(event.DeleteEvent) bool { return true },
				UpdateFunc:  func(event.UpdateEvent) bool { return false },
				GenericFunc: func(event.GenericEvent) bool { return false },
			}),
		).
		// Watch drop pods and map them back to the owning CachedImage via label.
		// We can't use Owns() because CachedImage is cluster-scoped and pods are namespaced.
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(
			func(ctx context.Context, obj client.Object) []reconcile.Request {
				pod, ok := obj.(*corev1.Pod)
				if !ok {
					return nil
				}
				if pod.Labels[podbuilder.LabelManagedBy] != podbuilder.LabelManagedByValue {
					return nil
				}
				// Skip delete events for completed pods — we already handled them
				// in the reconcile that deleted them. Re-reconciling before the
				// status patch lands causes transient status flapping.
				if pod.DeletionTimestamp != nil && pod.Status.Phase == corev1.PodSucceeded {
					return nil
				}
				// Skip delete events for completed pods — we already handled them
				// in the reconcile that deleted them. Re-reconciling before the
				// status patch lands causes transient status flapping.
				if pod.DeletionTimestamp != nil && pod.Status.Phase == corev1.PodSucceeded {
					return nil
				}
				ciName := pod.Labels[podbuilder.LabelCachedImage]
				if ciName == "" {
					return nil
				}
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: ciName}}}
			},
		)).
		Named("cachedimage").
		Complete(r)
}
