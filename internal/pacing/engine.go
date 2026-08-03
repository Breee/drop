package pacing

import (
	"context"
	"time"

	v1alpha1 "github.com/corewire/drop/api/v1alpha1"
	"github.com/corewire/drop/internal/podbuilder"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Decision reports how many new pull Pods may start now for one image and, when
// none may start yet, a hint for when to requeue.
type Decision struct {
	// Slots is the number of new pull Pods that may be created now for the image.
	Slots int
	// RequeueIn is a requeue hint used when Slots == 0 but the image still needs pulls.
	RequeueIn time.Duration
}

// Engine evaluates pacing constraints before creating new drop Pods.
type Engine struct {
	// Reader is used to list active pull Pods. It must bypass the informer cache
	// (e.g. mgr.GetAPIReader()) so that pods created in the current reconcile
	// cycle are visible to subsequent reconciles before the cache is updated.
	Reader       client.Reader
	PodNamespace string
}

// NewEngine creates a new pacing engine. reader should be a direct API-server
// reader (mgr.GetAPIReader()) to avoid stale cache reads when counting active pods.
func NewEngine(reader client.Reader, podNamespace string) *Engine {
	return &Engine{Reader: reader, PodNamespace: podNamespace}
}

const (
	defaultMaxConcurrentNodes = int32(1)
	defaultMinDelay           = 10 * time.Second
	// requeueWhenSaturated is the requeue delay used when a concurrency cap is hit.
	requeueWhenSaturated = 5 * time.Second
)

// PullSlots evaluates pacing constraints and returns how many new pull Pods may
// start now for the given CachedImage.
//
// Semantics:
//   - maxConcurrentNodes caps simultaneous pulls for THIS image (per-image fan-out).
//   - minDelayBetweenPulls staggers successive waves of starts for THIS image.
//   - maxConcurrentPulls caps the TOTAL pull Pods across all images (global valve).
//
// On the first wave for an image, up to maxConcurrentNodes pulls may start at
// once; subsequent waves wait at least minDelayBetweenPulls.
func (e *Engine) PullSlots(ctx context.Context, policy *v1alpha1.PullPolicy, cachedImageName string) (Decision, error) {
	maxConcurrentNodes := defaultMaxConcurrentNodes
	minDelay := defaultMinDelay
	var maxConcurrentPulls int32 // 0 = unlimited

	if policy != nil {
		if policy.Spec.MaxConcurrentNodes > 0 {
			maxConcurrentNodes = policy.Spec.MaxConcurrentNodes
		}
		if policy.Spec.MinDelayBetweenPulls.Duration > 0 {
			minDelay = policy.Spec.MinDelayBetweenPulls.Duration
		}
		if policy.Spec.MaxConcurrentPulls != nil && *policy.Spec.MaxConcurrentPulls > 0 {
			maxConcurrentPulls = *policy.Spec.MaxConcurrentPulls
		}
	}

	// List active drop Pods (Running or Pending)
	podList := &corev1.PodList{}
	ns := e.PodNamespace
	if ns == "" {
		ns = podbuilder.DefaultPodNamespace
	}
	listOpts := []client.ListOption{
		client.InNamespace(ns),
		client.MatchingLabels{podbuilder.LabelManagedBy: podbuilder.LabelManagedByValue},
	}
	if err := e.Reader.List(ctx, podList, listOpts...); err != nil {
		return Decision{}, err
	}

	// Count active pods globally and for this image, tracking the most recent
	// start time for this image (used for the per-image wave stagger).
	var (
		imageActive     int32
		globalActive    int32
		imageMostRecent time.Time
	)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase != corev1.PodPending && pod.Status.Phase != corev1.PodRunning {
			continue
		}
		// Skip pods stuck in image pull errors — they're about to be cleaned up.
		if isStuckImagePull(pod) {
			continue
		}
		globalActive++
		if pod.Labels[podbuilder.LabelCachedImage] == cachedImageName {
			imageActive++
			if created := pod.CreationTimestamp.Time; created.After(imageMostRecent) {
				imageMostRecent = created
			}
		}
	}

	// Per-image fan-out cap.
	perImageRemaining := maxConcurrentNodes - imageActive
	if perImageRemaining <= 0 {
		return Decision{Slots: 0, RequeueIn: requeueWhenSaturated}, nil
	}

	// Global total cap (bandwidth valve). When unset, only the per-image cap applies.
	slots := perImageRemaining
	if maxConcurrentPulls > 0 {
		globalRemaining := maxConcurrentPulls - globalActive
		if globalRemaining <= 0 {
			return Decision{Slots: 0, RequeueIn: requeueWhenSaturated}, nil
		}
		if globalRemaining < slots {
			slots = globalRemaining
		}
	}

	// Per-image stagger between successive waves.
	if !imageMostRecent.IsZero() {
		if elapsed := time.Since(imageMostRecent); elapsed < minDelay {
			return Decision{Slots: 0, RequeueIn: minDelay - elapsed}, nil
		}
	}

	return Decision{Slots: int(slots)}, nil
}

// isStuckImagePull returns true if a pod has a container waiting due to image pull failure.
func isStuckImagePull(pod *corev1.Pod) bool {
	for _, cs := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
		if cs.State.Waiting != nil {
			switch cs.State.Waiting.Reason {
			case "ErrImagePull", "ImagePullBackOff", "InvalidImageName", "RegistryUnavailable":
				return true
			}
		}
	}
	return false
}
