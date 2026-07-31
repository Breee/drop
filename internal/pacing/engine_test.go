package pacing

import (
	"context"
	"testing"
	"time"

	v1alpha1 "github.com/corewire/drop/api/v1alpha1"
	"github.com/corewire/drop/internal/podbuilder"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

// activePod builds a running managed pull Pod for the given image, created
// agoSeconds in the past.
func activePod(name, image string, agoSeconds int) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "drop-system",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Duration(agoSeconds) * time.Second)),
			Labels: map[string]string{
				podbuilder.LabelManagedBy:   podbuilder.LabelManagedByValue,
				podbuilder.LabelCachedImage: image,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func testPolicy(maxNodes int32, minDelay time.Duration, maxPulls *int32) *v1alpha1.PullPolicy {
	return &v1alpha1.PullPolicy{
		Spec: v1alpha1.PullPolicySpec{
			MaxConcurrentNodes:   maxNodes,
			MinDelayBetweenPulls: metav1.Duration{Duration: minDelay},
			MaxConcurrentPulls:   maxPulls,
		},
	}
}

func TestPullSlots(t *testing.T) {
	const image = "test-image"

	tests := []struct {
		name        string
		policy      *v1alpha1.PullPolicy
		activePods  []corev1.Pod
		wantSlots   int
		wantRequeue bool
	}{
		{
			name:      "full fan-out available when no pulls active",
			policy:    testPolicy(3, time.Second, nil),
			wantSlots: 3,
		},
		{
			name:      "nil policy defaults to one slot",
			policy:    nil,
			wantSlots: 1,
		},
		{
			name:        "per-image cap reached blocks",
			policy:      testPolicy(2, 10*time.Second, nil),
			activePods:  []corev1.Pod{activePod("p1", image, 30), activePod("p2", image, 30)},
			wantSlots:   0,
			wantRequeue: true,
		},
		{
			name:       "partial fan-out returns remaining slots",
			policy:     testPolicy(3, time.Second, nil),
			activePods: []corev1.Pod{activePod("p1", image, 30)},
			wantSlots:  2,
		},
		{
			name:        "wave stagger not elapsed blocks",
			policy:      testPolicy(5, 60*time.Second, nil),
			activePods:  []corev1.Pod{activePod("p1", image, 5)},
			wantSlots:   0,
			wantRequeue: true,
		},
		{
			name:       "other images do not count against per-image cap",
			policy:     testPolicy(2, time.Second, nil),
			activePods: []corev1.Pod{activePod("o1", "other-image", 30), activePod("o2", "other-image", 30)},
			wantSlots:  2,
		},
		{
			name:       "global cap limits slots across images",
			policy:     testPolicy(5, time.Second, ptr.To(int32(3))),
			activePods: []corev1.Pod{activePod("o1", "other-image", 30), activePod("o2", "other-image", 30)},
			wantSlots:  1, // global remaining = 3 - 2 = 1
		},
		{
			name:        "global cap saturated blocks",
			policy:      testPolicy(5, time.Second, ptr.To(int32(2))),
			activePods:  []corev1.Pod{activePod("o1", "other-image", 30), activePod("o2", "other-image", 30)},
			wantSlots:   0,
			wantRequeue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := testScheme()

			objs := make([]runtime.Object, 0, len(tt.activePods))
			for i := range tt.activePods {
				objs = append(objs, &tt.activePods[i])
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithRuntimeObjects(objs...).
				Build()

			engine := NewEngine(fakeClient, "drop-system")
			decision, err := engine.PullSlots(context.Background(), tt.policy, image)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if decision.Slots != tt.wantSlots {
				t.Errorf("Slots = %d, want %d", decision.Slots, tt.wantSlots)
			}
			if tt.wantRequeue && decision.RequeueIn == 0 {
				t.Error("expected non-zero RequeueIn")
			}
			if !tt.wantRequeue && decision.RequeueIn != 0 {
				t.Errorf("expected zero RequeueIn, got %v", decision.RequeueIn)
			}
		})
	}
}

// TestPullSlots_StuckPodsIgnored verifies pods stuck in ImagePullBackOff are not
// counted as active.
func TestPullSlots_StuckPodsIgnored(t *testing.T) {
	const image = "test-image"
	stuck := activePod("stuck", image, 30)
	stuck.Status.ContainerStatuses = []corev1.ContainerStatus{{
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
	}}

	scheme := testScheme()
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(&stuck).
		Build()

	engine := NewEngine(fakeClient, "drop-system")
	decision, err := engine.PullSlots(context.Background(), testPolicy(2, time.Second, nil), image)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision.Slots != 2 {
		t.Errorf("Slots = %d, want 2 (stuck pod ignored)", decision.Slots)
	}
}
