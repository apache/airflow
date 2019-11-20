/*
Copyright 2018 Google LLC
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

package status

import (
	//"fmt"
	appsv1 "k8s.io/api/apps/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
)

// Constants defining labels
const (
	StatusReady      = "Ready"
	StatusInProgress = "InProgress"
	StatusDisabled   = "Disabled"
)

func (s *Statefulset) update(rsrc *appsv1.StatefulSet) string {
	s.Replicas = rsrc.Status.Replicas
	s.ReadyReplicas = rsrc.Status.ReadyReplicas
	s.CurrentReplicas = rsrc.Status.CurrentReplicas
	if rsrc.Status.ReadyReplicas == *rsrc.Spec.Replicas && rsrc.Status.CurrentReplicas == *rsrc.Spec.Replicas {
		return StatusReady
	}
	return StatusInProgress
}

func (s *ObjectStatus) update(rsrc metav1.Object) {
	ro := rsrc.(runtime.Object)
	gvk := ro.GetObjectKind().GroupVersionKind()
	s.Link = rsrc.GetSelfLink()
	s.Name = rsrc.GetName()
	s.Group = gvk.GroupVersion().String()
	s.Kind = gvk.GroupKind().Kind
	s.Status = StatusReady
}

// Pdb is a generic status holder for pdb
func (s *Pdb) update(rsrc *policyv1.PodDisruptionBudget) string {
	s.CurrentHealthy = rsrc.Status.CurrentHealthy
	s.DesiredHealthy = rsrc.Status.DesiredHealthy
	if s.CurrentHealthy >= s.DesiredHealthy {
		return StatusReady
	}
	return StatusInProgress
}

// ResetComponentList - reset component list objects
func (cm *ComponentMeta) ResetComponentList() {
	cm.ComponentList.Objects = []ObjectStatus{}
}

// UpdateStatus the component status
func (cm *ComponentMeta) UpdateStatus(items []reconciler.Object) bool {
	var ready = true
	for _, item := range items {
		r := item.Obj.(*k8s.Object)
		os := ObjectStatus{}
		os.update(r.Obj)
		switch r.Obj.(type) {
		case *appsv1.StatefulSet:
			os.ExtendedStatus.STS = &Statefulset{}
			os.Status = os.ExtendedStatus.STS.update(r.Obj.(*appsv1.StatefulSet))
		case *policyv1.PodDisruptionBudget:
			os.ExtendedStatus.PDB = &Pdb{}
			os.Status = os.ExtendedStatus.PDB.update(r.Obj.(*policyv1.PodDisruptionBudget))
		}
		cm.ComponentList.Objects = append(cm.ComponentList.Objects, os)
	}
	for _, os := range cm.ComponentList.Objects {
		if os.Status != StatusReady {
			ready = false
		}
	}

	return ready
}

// UpdateStatus the component status
func (m *Meta) UpdateStatus(componentready *bool, err error) {
	if componentready != nil {
		if *componentready {
			m.Ready("ComponentsReady", "all components ready")
		} else {
			m.NotReady("ComponentsNotReady", "some components not ready")
		}
	}
	if err != nil {
		m.SetCondition(Error, "ErrorSeen", err.Error())
	}
}
