/*
Copyright (c) 2026 Breee

SPDX-License-Identifier: MIT
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PullPolicySpec defines pacing and behavior configuration for image pulls.
// A PullPolicy is referenced by CachedImage or CachedImageSet via policyRef.
type PullPolicySpec struct {
	// MaxConcurrentNodes is the maximum number of Pods pulling a SINGLE image
	// simultaneously — the fan-out of one image across its target nodes. Different
	// images pull independently in parallel (bounded by MaxConcurrentPulls). drop
	// may start up to this many pulls for an image in one pass, then staggers
	// further waves via MinDelayBetweenPulls. Increase for faster per-image rollout
	// on large clusters; keep low for bandwidth-constrained nodes.
	// Default: 1. Example: 3 (pull one image on up to 3 nodes at once)
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	MaxConcurrentNodes int32 `json:"maxConcurrentNodes,omitempty"`
	// MaxConcurrentPulls is a global cap on the TOTAL number of pull Pods running
	// simultaneously across ALL images that reference this policy. It is a
	// cluster-wide bandwidth valve, independent of the per-image MaxConcurrentNodes
	// fan-out. Unset or zero means unlimited (only the per-image cap applies).
	// Example: 20
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxConcurrentPulls *int32 `json:"maxConcurrentPulls,omitempty"`
	// MinDelayBetweenPulls is the minimum wait between successive waves of pull
	// starts for the SAME image. drop may start up to MaxConcurrentNodes pulls for
	// an image at once, then waits at least this long before starting more for that
	// image. Applied per image, not globally, so different images are not throttled
	// against each other. Prevents a tight loop of new Pods.
	// Default: "10s". Example: "30s", "1m"
	// +kubebuilder:default="10s"
	MinDelayBetweenPulls metav1.Duration `json:"minDelayBetweenPulls,omitempty"`
	// FailureBackoff configures exponential retry delays when a pull fails.
	// If unset, defaults to initial=30s, max=5m.
	// +optional
	FailureBackoff *BackoffConfig `json:"failureBackoff,omitempty"`
	// RepullInterval defines how often to re-pull already-cached images to pick up digest changes.
	// Unset or zero means never re-pull (rely on imagePullPolicy=Always on the CachedImage instead).
	// Example: "24h" (re-pull daily), "6h"
	// +optional
	RepullInterval *metav1.Duration `json:"repullInterval,omitempty"`
	// NodeSelector scopes this policy to a specific node pool.
	// Only relevant when the same PullPolicy should only pace pulls on a subset of nodes.
	// Example: {"node-role.kubernetes.io/build": "true"}
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Tolerations allow the pull pods created under this policy to schedule on tainted nodes.
	// Example: [{key: "dedicated", value: "ci", effect: "NoSchedule"}]
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// BackoffConfig defines exponential retry backoff behavior for failed pulls.
type BackoffConfig struct {
	// Initial delay before the first retry attempt after a failure.
	// Default: "30s". Example: "1m"
	// +kubebuilder:default="30s"
	Initial metav1.Duration `json:"initial,omitempty"`
	// Max is the upper bound on backoff delay. Retries will never wait longer than this.
	// Default: "5m". Example: "10m"
	// +kubebuilder:default="5m"
	Max metav1.Duration `json:"max,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,categories=drop
// +kubebuilder:printcolumn:name="MaxNodes",type=integer,JSONPath=`.spec.maxConcurrentNodes`
// +kubebuilder:printcolumn:name="MaxPulls",type=integer,JSONPath=`.spec.maxConcurrentPulls`
// +kubebuilder:printcolumn:name="MinDelay",type=string,JSONPath=`.spec.minDelayBetweenPulls`
// +kubebuilder:printcolumn:name="RepullInterval",type=string,JSONPath=`.spec.repullInterval`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PullPolicy controls the pacing and retry behavior for image pulls across cluster nodes.
// It is a configuration-only resource with no status.
type PullPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PullPolicySpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// PullPolicyList contains a list of PullPolicy.
type PullPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PullPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &PullPolicy{}, &PullPolicyList{})
		return nil
	})
}
