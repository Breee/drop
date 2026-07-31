/*
Copyright (c) 2026 Breee

SPDX-License-Identifier: MIT
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dropv1alpha1 "github.com/corewire/drop/api/v1alpha1"
)

var _ = Describe("CachedImageSet Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-imageset"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name: resourceName,
		}
		cachedimageset := &dropv1alpha1.CachedImageSet{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind CachedImageSet")
			err := k8sClient.Get(ctx, typeNamespacedName, cachedimageset)
			if err != nil && errors.IsNotFound(err) {
				resource := &dropv1alpha1.CachedImageSet{
					ObjectMeta: metav1.ObjectMeta{
						Name: resourceName,
					},
					Spec: dropv1alpha1.CachedImageSetSpec{
						Images: []dropv1alpha1.ImageEntry{
							{Image: "docker.io/library/nginx", Tag: "1.25"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &dropv1alpha1.CachedImageSet{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Cleanup the specific resource instance CachedImageSet")
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &CachedImageSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When the referenced DiscoveryPolicy is failing", func() {
		ctx := context.Background()

		It("marks the CachedImageSet Degraded and surfaces the discovery failure", func() {
			By("creating a DiscoveryPolicy with a failing Ready condition")
			dp := &dropv1alpha1.DiscoveryPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "broken-discovery"},
			}
			Expect(k8sClient.Create(ctx, dp)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, dp) })

			dp.Status.Conditions = []metav1.Condition{{
				Type:               conditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             "Unauthorized",
				Message:            "registry returned status 401",
				LastTransitionTime: metav1.Now(),
			}}
			Expect(k8sClient.Status().Update(ctx, dp)).To(Succeed())

			By("creating a CachedImageSet backed by the failing DiscoveryPolicy")
			set := &dropv1alpha1.CachedImageSet{
				ObjectMeta: metav1.ObjectMeta{Name: "discovery-backed-set"},
				Spec: dropv1alpha1.CachedImageSetSpec{
					DiscoveryPolicyRef: &dropv1alpha1.DiscoveryPolicyReference{Name: "broken-discovery"},
				},
			}
			Expect(k8sClient.Create(ctx, set)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, set) })

			controllerReconciler := &CachedImageSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "discovery-backed-set"},
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking the set is Degraded with the discovery failure surfaced")
			updated := &dropv1alpha1.CachedImageSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "discovery-backed-set"}, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(phaseDegraded))

			cond := meta.FindStatusCondition(updated.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("Unauthorized"))
			Expect(cond.Message).To(ContainSubstring("broken-discovery"))
			Expect(cond.Message).To(ContainSubstring("401"))
		})

		It("does not mark the set Degraded while discovery is still pending", func() {
			By("creating a DiscoveryPolicy with no Ready condition yet")
			dp := &dropv1alpha1.DiscoveryPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "pending-discovery"},
			}
			Expect(k8sClient.Create(ctx, dp)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, dp) })

			set := &dropv1alpha1.CachedImageSet{
				ObjectMeta: metav1.ObjectMeta{Name: "pending-backed-set"},
				Spec: dropv1alpha1.CachedImageSetSpec{
					DiscoveryPolicyRef: &dropv1alpha1.DiscoveryPolicyReference{Name: "pending-discovery"},
				},
			}
			Expect(k8sClient.Create(ctx, set)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, set) })

			controllerReconciler := &CachedImageSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "pending-backed-set"},
			})
			Expect(err).NotTo(HaveOccurred())

			updated := &dropv1alpha1.CachedImageSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pending-backed-set"}, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(phasePending))
		})

		It("marks the set Degraded when the DiscoveryPolicy does not exist", func() {
			set := &dropv1alpha1.CachedImageSet{
				ObjectMeta: metav1.ObjectMeta{Name: "missing-discovery-set"},
				Spec: dropv1alpha1.CachedImageSetSpec{
					DiscoveryPolicyRef: &dropv1alpha1.DiscoveryPolicyReference{Name: "does-not-exist"},
				},
			}
			Expect(k8sClient.Create(ctx, set)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, set) })

			controllerReconciler := &CachedImageSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "missing-discovery-set"},
			})
			Expect(err).NotTo(HaveOccurred())

			updated := &dropv1alpha1.CachedImageSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "missing-discovery-set"}, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(phaseDegraded))
			cond := meta.FindStatusCondition(updated.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal("DiscoveryPolicyNotFound"))
		})
	})
})
