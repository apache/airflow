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

package finalizer_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-reconciler/pkg/finalizer"
)

var _ = Describe("Finalizer", func() {

	deployment := &appsv1.Deployment{}
	BeforeEach(func() {
		deployment = &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "n-deploy",
				Namespace: "ns",
			},
		}
	})

	Describe("Finalizer", func() {
		It("Add finalizer", func(done Done) {
			Expect(len(deployment.ObjectMeta.Finalizers)).To(Equal(0))
			finalizer.Add(deployment, finalizer.Cleanup)
			Expect(len(deployment.ObjectMeta.Finalizers)).To(Equal(1))
			finalizer.Add(deployment, finalizer.Cleanup)
			Expect(len(deployment.ObjectMeta.Finalizers)).To(Equal(1))
			finalizer.Add(deployment, "second")
			Expect(len(deployment.ObjectMeta.Finalizers)).To(Equal(2))
			close(done)
		})
		It("Remove finalizer", func(done Done) {
			Expect(len(deployment.ObjectMeta.Finalizers)).To(Equal(0))
			finalizer.Add(deployment, finalizer.Cleanup)
			finalizer.Add(deployment, "second")
			Expect(len(deployment.ObjectMeta.Finalizers)).To(Equal(2))
			finalizer.Remove(deployment, "second")
			Expect(len(deployment.ObjectMeta.Finalizers)).To(Equal(1))
			finalizer.Remove(deployment, "second")
			Expect(len(deployment.ObjectMeta.Finalizers)).To(Equal(1))
			finalizer.Remove(deployment, finalizer.Cleanup)
			Expect(len(deployment.ObjectMeta.Finalizers)).To(Equal(0))
			close(done)
		})
	})
})
