/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sparkinteractiveiov1alpha1 "github.com/padod/spark-session-operator/api/v1alpha1"
)

var _ = Describe("SparkSessionPool Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		sparksessionpool := &sparkinteractiveiov1alpha1.SparkSessionPool{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind SparkSessionPool")
			err := k8sClient.Get(ctx, typeNamespacedName, sparksessionpool)
			if err != nil && errors.IsNotFound(err) {
				resource := &sparkinteractiveiov1alpha1.SparkSessionPool{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: sparkinteractiveiov1alpha1.SparkSessionPoolSpec{
						Type: "connect",
						Host: "test-pool.example.com",
						Replicas: sparkinteractiveiov1alpha1.ReplicaSpec{
							Min: 0,
							Max: 1,
						},
						Scaling: sparkinteractiveiov1alpha1.ScalingSpec{
							Metrics: sparkinteractiveiov1alpha1.ScalingMetricsSpec{
								Type:              "activeSessions",
								TargetPerInstance: 20,
							},
						},
						SparkApplicationTemplate: sparkinteractiveiov1alpha1.SparkApplicationTemplateSpec{
							Spec: &apiextensionsv1.JSON{Raw: []byte(`{}`)},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &sparkinteractiveiov1alpha1.SparkSessionPool{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance SparkSessionPool")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			// Use the cached manager client: the reconciler issues
			// List(... MatchingFields{"spec.pool": ...}), which only works
			// against the cached client that has the field index registered.
			controllerReconciler := &SparkSessionPoolReconciler{
				Client: mgrClient,
				Scheme: mgrClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})
