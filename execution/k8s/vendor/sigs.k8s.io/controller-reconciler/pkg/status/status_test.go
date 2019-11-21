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

package status_test

import (
	//"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
	"sigs.k8s.io/controller-reconciler/pkg/status"
)

var _ = Describe("Status", func() {
	var replicas int32 = 3
	type testStatus struct {
		status.Meta
		status.ComponentMeta
	}
	var condition1 status.ConditionType = "condition1"
	var cstatusT corev1.ConditionStatus = corev1.ConditionTrue
	var cstatusF corev1.ConditionStatus = corev1.ConditionFalse
	var cstatusU corev1.ConditionStatus = corev1.ConditionUnknown

	var teststatus testStatus
	resources := []reconciler.Object{
		{
			Type: k8s.Type,
			Obj: &k8s.Object{
				Obj: &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "svcOK",
						Namespace: "default",
						SelfLink:  "ignorethislink",
					},
					Status: corev1.ServiceStatus{},
				},
			},
		},
		{
			Type: k8s.Type,
			Obj: &k8s.Object{
				Obj: &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "stsOK",
						Namespace: "default",
						SelfLink:  "ignorethislink",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: &replicas,
					},
					Status: appsv1.StatefulSetStatus{
						Replicas:        3,
						ReadyReplicas:   3,
						CurrentReplicas: 3,
					},
				},
			},
		},
		{
			Type: k8s.Type,
			Obj: &k8s.Object{
				Obj: &policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pdbOK",
						Namespace: "default",
						SelfLink:  "ignorethislink",
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						CurrentHealthy: 3,
						DesiredHealthy: 3,
					},
				},
			},
		},
	}
	stsNotOk := reconciler.Object{
		Type: k8s.Type,
		Obj: &k8s.Object{
			Obj: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "stsnok",
					Namespace: "default",
					SelfLink:  "ignorethislink",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &replicas,
				},
				Status: appsv1.StatefulSetStatus{
					Replicas:        3,
					ReadyReplicas:   2,
					CurrentReplicas: 3,
				},
			},
		},
	}
	pdbNotOk := reconciler.Object{
		Type: k8s.Type,
		Obj: &k8s.Object{
			Obj: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pdbnok",
					Namespace: "default",
					SelfLink:  "ignorethislink",
				},
				Status: policyv1.PodDisruptionBudgetStatus{
					CurrentHealthy: 2,
					DesiredHealthy: 3,
				},
			},
		},
	}

	BeforeEach(func() {
	})

	Describe("ResourceStatus", func() {
		It("Getting status from all ok reosurces", func(done Done) {
			teststatus = testStatus{}
			ready := teststatus.ComponentMeta.UpdateStatus(resources)
			teststatus.Meta.UpdateStatus(&ready, nil)
			Expect(teststatus.Meta.IsReady()).To(Equal(true))
			close(done)
		})
		It("Getting status from all not ready sts is InProgress", func(done Done) {
			ready := teststatus.ComponentMeta.UpdateStatus(append(resources, stsNotOk))
			teststatus.Meta.UpdateStatus(&ready, nil)
			Expect(teststatus.Meta.IsReady()).To(Equal(false))
			close(done)
		})
		It("Getting status from all not ready pdb is InProgress", func(done Done) {
			ready := teststatus.ComponentMeta.UpdateStatus(append(resources, pdbNotOk))
			teststatus.Meta.UpdateStatus(&ready, nil)
			Expect(teststatus.Meta.IsReady()).To(Equal(false))
			close(done)
		})
	})

	Describe("StatusMeta", func() {
		It("Setting new condition works", func(done Done) {
			teststatus = testStatus{}
			Expect(len(teststatus.Conditions)).To(Equal(0))
			teststatus.SetCondition("condition1", "no reason", "nothing to see here")
			//fmt.Printf(">>>>>>%s\n", teststatus.Conditions[0].Type)
			Expect(len(teststatus.Conditions)).To(Equal(1))
			close(done)
		})
		It("Getting condition works", func(done Done) {
			c := teststatus.GetCondition("condition1")
			Expect(c.Type).To(Equal(condition1))
			Expect(c.Status).To(Equal(cstatusT))
			Expect(c.LastUpdateTime).To(Equal(c.LastTransitionTime))
			close(done)
		})
		It("Changing status of existing condition changes transition time", func(done Done) {
			c := teststatus.GetCondition("condition1")
			ttime := c.LastTransitionTime
			teststatus.ClearCondition("condition1", "some reason", "something to see here")
			c = teststatus.GetCondition("condition1")
			Expect(c.Type).To(Equal(condition1))
			Expect(c.Status).To(Equal(cstatusF))
			Expect(c.LastUpdateTime).To(Equal(c.LastTransitionTime))
			Expect(c.LastTransitionTime).ToNot(Equal(ttime))
			close(done)
		})
		It("Changing reson of existing condition does not change transition time", func(done Done) {
			c := teststatus.GetCondition("condition1")
			ttime := c.LastTransitionTime
			teststatus.ClearCondition("condition1", "some new reason", "something else to see here")
			c = teststatus.GetCondition("condition1")
			Expect(c.Type).To(Equal(condition1))
			Expect(c.Status).To(Equal(cstatusF))
			Expect(c.LastUpdateTime).ToNot(Equal(c.LastTransitionTime))
			Expect(c.LastTransitionTime).To(Equal(ttime))
			close(done)
		})
		It("Deleting condition works", func(done Done) {
			teststatus.RemoveCondition("condition1")
			Expect(len(teststatus.Conditions)).To(Equal(0))
			close(done)
		})
		It("Ensure condition works", func(done Done) {
			Expect(len(teststatus.Conditions)).To(Equal(0))
			teststatus.EnsureCondition(status.TrafficReady)
			Expect(len(teststatus.Conditions)).To(Equal(1))
			c := teststatus.GetCondition(status.TrafficReady)
			Expect(c.Status).To(Equal(cstatusU))
			close(done)
		})
		It("Ensure standard conditions work", func(done Done) {
			Expect(len(teststatus.Conditions)).To(Equal(1))
			teststatus.EnsureStandardConditions()
			Expect(len(teststatus.Conditions)).To(Equal(4))
			c := teststatus.GetCondition(status.Ready)
			Expect(c.Status).To(Equal(cstatusU))
			c = teststatus.GetCondition(status.Settled)
			Expect(c.Status).To(Equal(cstatusU))
			close(done)
		})
		It("Ready() sets Ready condition to true/False", func(done Done) {
			c := teststatus.GetCondition(status.Ready)
			Expect(c.Status).To(Equal(cstatusU))
			teststatus.Ready("", "")
			c = teststatus.GetCondition(status.Ready)
			Expect(c.Status).To(Equal(cstatusT))
			teststatus.NotReady("", "")
			c = teststatus.GetCondition(status.Ready)
			Expect(c.Status).To(Equal(cstatusF))
			close(done)
		})
		It("Settled() sets Settled condition to true/False", func(done Done) {
			c := teststatus.GetCondition(status.Settled)
			Expect(c.Status).To(Equal(cstatusU))
			teststatus.Settled("", "")
			c = teststatus.GetCondition(status.Settled)
			Expect(c.Status).To(Equal(cstatusT))
			teststatus.NotSettled("", "")
			c = teststatus.GetCondition(status.Settled)
			Expect(c.Status).To(Equal(cstatusF))
			close(done)
		})
	})
})
