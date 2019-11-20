// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package application_test

import (
	app "github.com/kubernetes-sigs/application/pkg/apis/app/v1beta1"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/airflow-operator/pkg/controller/application"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
)

var _ = Describe("Application", func() {
	BeforeEach(func() {
	})
	labels := map[string]string{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	}
	var resources []reconciler.Object = []reconciler.Object{
		{
			Lifecycle: reconciler.LifecycleManaged,
			Type:      k8s.Type,
			Obj: &k8s.Object{
				ObjList: &appsv1.DeploymentList{},
				Obj: &appsv1.Deployment{
					TypeMeta: metav1.TypeMeta{
						Kind:       "k3",
						APIVersion: "v4",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "n-deploy",
						Namespace: "ns",
						Labels:    labels,
					},
				},
			},
		},
		{
			Lifecycle: reconciler.LifecycleManaged,
			Type:      k8s.Type,
			Obj: &k8s.Object{
				ObjList: &corev1.ConfigMapList{},
				Obj: &corev1.ConfigMap{
					TypeMeta: metav1.TypeMeta{
						Kind:       "k1",
						APIVersion: "v2",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "n-cm",
						Namespace: "ns",
						Labels:    labels,
					},
					Data: map[string]string{
						"test-key": "test-value",
					},
				},
			},
		},
	}
	a := application.NewApplication(&app.Application{})
	Describe("Status", func() {
		It("Sets Component Group Kind", func(done Done) {
			a.SetComponentGK(resources)
			Expect(len(a.Spec.ComponentGroupKinds)).To(Equal(len(resources)))
			close(done)
		})
		It("Sets Selector", func(done Done) {
			a.SetSelector(labels)
			Expect(a.Spec.Selector.MatchLabels).To(Equal(labels))
			close(done)
		})
		It("Sets Meta Name Namespace Labels", func(done Done) {
			a.SetName("somename").SetNamespace("somens").SetLabels(labels)
			Expect(a.ObjectMeta.Name).To(Equal("somename"))
			Expect(a.ObjectMeta.Namespace).To(Equal("somens"))
			Expect(a.ObjectMeta.Labels).To(Equal(labels))
			close(done)
		})
	})
})
