/*
Copyright (c) 2026 Breee

SPDX-License-Identifier: MIT
*/

package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dropv1alpha1 "github.com/corewire/drop/api/v1alpha1"
)

// imageName() keeps only the last path segment, so images differing only by
// registry or org used to map onto one child name. The loser could never be
// created and the set controller retried it forever.
func TestChildName_DistinguishesSameShortNameAcrossRegistries(t *testing.T) {
	const parent = "auto-cached-ci-images"

	tests := []struct {
		name string
		a, b dropv1alpha1.ImageEntry
	}{
		{
			name: "different org",
			a:    dropv1alpha1.ImageEntry{Image: "registry.haufe.io/rd/tools", Tag: "stable"},
			b:    dropv1alpha1.ImageEntry{Image: "registry.gitlab.com/dos-devinfra/tools", Tag: "stable"},
		},
		{
			name: "different registry, same org",
			a:    dropv1alpha1.ImageEntry{Image: "registry.haufe.io/rd/service-dind", Tag: "linux-prod"},
			b:    dropv1alpha1.ImageEntry{Image: "other.example.com/rd/service-dind", Tag: "linux-prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotA, gotB := childName(parent, tt.a), childName(parent, tt.b)
			if gotA == gotB {
				t.Errorf("childName collision: %q used for both %q and %q", gotA, tt.a.Image, tt.b.Image)
			}
			if gotA != childName(parent, tt.a) {
				t.Error("childName is not deterministic")
			}
		})
	}
}

func TestChildName_ValidAndBounded(t *testing.T) {
	long := "registry.example.com/" + string(make([]byte, 0))
	for range 40 {
		long += "verylongpathsegment/"
	}
	entry := dropv1alpha1.ImageEntry{Image: long + "image", Tag: "sometag"}

	got := childName("parent", entry)
	if len(got) > 253 {
		t.Errorf("childName length = %d, want <= 253", len(got))
	}
	if got[len(got)-1] == '-' {
		t.Errorf("childName %q must not end in a hyphen", got)
	}

	// Truncation must not discard the uniquifying suffix.
	other := dropv1alpha1.ImageEntry{Image: long + "image", Tag: "othertag"}
	if got == childName("parent", other) {
		t.Error("truncation dropped the unique suffix; long names collide")
	}
}

// LastPulledAt is documented as the most recent successful pull. Stamping it on
// every reconcile rewrote the object forever and pinned markNodesForRepull's
// elapsed time near zero, silently disabling repull.
func TestUpdateCachedImageStatus_LastPulledAtOnlyOnPull(t *testing.T) {
	r := &CachedImageReconciler{}
	ci := &dropv1alpha1.CachedImage{}
	stateMap := map[string]*nodeState{
		"node-a": {ready: true},
		"node-b": {ready: true},
	}

	first := metav1.NewTime(time.Now().Add(-time.Hour))
	r.updateCachedImageStatus(ci, stateMap, 2, 2, first)
	if ci.Status.LastPulledAt == nil {
		t.Fatal("LastPulledAt not set after the initial pull completed")
	}
	if !ci.Status.LastPulledAt.Equal(&first) {
		t.Fatalf("LastPulledAt = %v, want %v", ci.Status.LastPulledAt, first)
	}

	later := metav1.NewTime(time.Now())
	r.updateCachedImageStatus(ci, stateMap, 2, 2, later)
	if !ci.Status.LastPulledAt.Equal(&first) {
		t.Errorf("LastPulledAt moved to %v on a re-observation; want it pinned at %v",
			ci.Status.LastPulledAt, first)
	}

	stateMap["node-c"] = &nodeState{ready: true}
	r.updateCachedImageStatus(ci, stateMap, 3, 3, later)
	if !ci.Status.LastPulledAt.Equal(&later) {
		t.Errorf("LastPulledAt = %v after a new node cached the image, want %v",
			ci.Status.LastPulledAt, later)
	}
}
