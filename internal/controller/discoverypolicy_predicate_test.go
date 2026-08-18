/*
Copyright (c) 2026 Breee

SPDX-License-Identifier: MIT
*/

package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	dropv1alpha1 "github.com/corewire/drop/api/v1alpha1"
)

// Every reconcile writes status (LastSyncTime always moves). Without a
// generation filter that write re-triggers the watch and spins a hot loop that
// re-runs every discovery query, ignoring syncInterval. Status is a subresource
// here, so status writes leave Generation untouched.
func TestDiscoveryPolicyPredicate_IgnoresStatusOnlyUpdates(t *testing.T) {
	base := func(generation int64, syncTime time.Time) *dropv1alpha1.DiscoveryPolicy {
		return &dropv1alpha1.DiscoveryPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "popular-build-images", Generation: generation},
			Status:     dropv1alpha1.DiscoveryPolicyStatus{LastSyncTime: &metav1.Time{Time: syncTime}},
		}
	}

	now := time.Now()
	tests := []struct {
		name string
		old  *dropv1alpha1.DiscoveryPolicy
		new  *dropv1alpha1.DiscoveryPolicy
		want bool
	}{
		{
			name: "status-only write must not requeue",
			old:  base(1, now),
			new:  base(1, now.Add(time.Second)),
			want: false,
		},
		{
			name: "spec change must requeue",
			old:  base(1, now),
			new:  base(2, now),
			want: true,
		},
	}

	p := predicate.GenerationChangedPredicate{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.Update(event.UpdateEvent{ObjectOld: tt.old, ObjectNew: tt.new})
			if got != tt.want {
				t.Errorf("Update() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJitterInterval(t *testing.T) {
	const d = time.Hour
	minWant, maxWant := d-d/10, d+d/10

	distinct := map[time.Duration]struct{}{}
	for _, name := range []string{"popular-build-images", "ci-images", "base-images", "gpu-images"} {
		got := jitterInterval(d, name)
		if got < minWant || got > maxWant {
			t.Errorf("jitterInterval(%v, %q) = %v, want within [%v, %v]", d, name, got, minWant, maxWant)
		}
		if got != jitterInterval(d, name) {
			t.Errorf("jitterInterval(%v, %q) is not stable across calls", d, name)
		}
		distinct[got] = struct{}{}
	}
	if len(distinct) < 2 {
		t.Error("all policies got the same interval; they would stay aligned on the backend")
	}

	// Intervals too small to jitter must pass through rather than underflow.
	if got := jitterInterval(time.Nanosecond, "x"); got != time.Nanosecond {
		t.Errorf("jitterInterval(1ns) = %v, want 1ns", got)
	}
}
