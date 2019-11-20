/*
Copyright 2018 The Kubernetes Authors.

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

package genericreconciler_test

import (
	"context"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-reconciler/pkg/genericreconciler"
	test "sigs.k8s.io/controller-reconciler/pkg/genericreconciler/v1alpha1"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var AddToSchemes runtime.SchemeBuilder
var _ = Describe("Reconciler", func() {
	cl := fake.NewFakeClient()
	AddToSchemes.AddToScheme(scheme.Scheme)
	gr := genericreconciler.Reconciler{}
	// TODO gr.CR = customresource.CustomResource{Handle: &test.Foo{}}
	gr.For(&test.Foo{}, test.SchemeGroupVersion).
		WithResourceManager(k8s.Getter(context.TODO(), cl, scheme.Scheme)).
		Using(&test.FooHandler{})
	m1 := reconciler.KVMap{"k1": "v1"}
	m2 := reconciler.KVMap{"k2": "v2"}
	m3 := reconciler.KVMap{"k3": "v3"}

	BeforeEach(func() {})

	Describe("Status", func() {
		It("should be able to Merge KVmap", func(done Done) {
			m1.Merge(m2, m3, reconciler.KVMap{"k3": "v3"})
			Expect(len(m1)).To(Equal(3))
			close(done)
		})
		It("should not be able to Get non-existing deployment", func() {
			By("Getting a deployment")
			namespacedName := types.NamespacedName{
				Name:      "test-deployment",
				Namespace: "ns1",
			}
			obj := &appsv1.Deployment{}
			err := cl.Get(nil, namespacedName, obj)
			Expect(err).NotTo(BeNil())
		})
		It("should able to List Service", func() {
			By("Listing service")
			opts := client.ListOptions{}
			opts.Raw = &metav1.ListOptions{TypeMeta: metav1.TypeMeta{Kind: "Service", APIVersion: "v1"}}
			err := cl.List(context.TODO(), &opts, &corev1.ServiceList{})
			Expect(err).To(BeNil())
		})
		It("should not be able to Reconcile missing custom resource", func() {
			By("Getting a deployment")
			namespacedName := types.NamespacedName{
				Name:      "crA",
				Namespace: "ns1",
			}
			_, err := gr.ReconcileResource(namespacedName)
			Expect(err).To(BeNil())
		})
		It("should be able to Reconcile custom resource", func() {
			By("Getting a deployment")
			cl.Create(context.TODO(), &test.Foo{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crA",
					Namespace: "ns1",
				},
				Spec: test.FooSpec{
					Version: "",
				},
			})
			namespacedName := types.NamespacedName{
				Name:      "crA",
				Namespace: "ns1",
			}
			_, err := gr.ReconcileResource(namespacedName)
			Expect(err).To(BeNil())
			sts := &appsv1.Deployment{}
			err = cl.Get(nil, types.NamespacedName{Name: "crA-deploy", Namespace: "ns1"}, sts)
			Expect(err).To(BeNil())
			cm := &corev1.ConfigMap{}
			err = cl.Get(nil, types.NamespacedName{Name: "crA-cm", Namespace: "ns1"}, cm)
			Expect(err).To(BeNil())
		})

	})
})
