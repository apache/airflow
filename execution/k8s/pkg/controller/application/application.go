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

package application

import (
	app "github.com/kubernetes-sigs/application/pkg/apis/app/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
)

// Application obj to attach methods
type Application struct {
	*app.Application
}

// NewApplication - return Application object from runtime Object
func NewApplication(obj metav1.Object) Application {
	return Application{obj.(*app.Application)}
}

// SetSelector attaches selectors to Application object
func (a *Application) SetSelector(labels map[string]string) *Application {
	a.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	return a
}

// SetName sets name
func (a *Application) SetName(value string) *Application {
	a.ObjectMeta.Name = value
	return a
}

// SetNamespace asets namespace
func (a *Application) SetNamespace(value string) *Application {
	a.ObjectMeta.Namespace = value
	return a
}

// AddLabels adds more labels
func (a *Application) AddLabels(value reconciler.KVMap) *Application {
	value.Merge(a.ObjectMeta.Labels)
	a.ObjectMeta.Labels = value
	return a
}

// Observable returns resource object
func (a *Application) Observable() *reconciler.Observable {
	return &reconciler.Observable{
		Obj: k8s.Observable{
			Obj:     a.Application,
			ObjList: &app.ApplicationList{},
			Labels:  a.GetLabels(),
		},
		Type: k8s.Type,
	}
}

// Item returns resource object
func (a *Application) Item() *reconciler.Object {
	return &reconciler.Object{
		Lifecycle: reconciler.LifecycleManaged,
		Type:      k8s.Type,
		Obj: &k8s.Object{
			Obj:     a.Application,
			ObjList: &app.ApplicationList{},
		},
	}
}

// AddToScheme return AddToScheme of application crd
func AddToScheme(sb *runtime.SchemeBuilder) {
	*sb = append(*sb, app.AddToScheme)
}

// SetComponentGK attaches component GK to Application object
func (a *Application) SetComponentGK(bag []reconciler.Object) *Application {
	a.Spec.ComponentGroupKinds = []metav1.GroupKind{}
	gkmap := map[schema.GroupKind]struct{}{}
	for _, item := range reconciler.ObjectsByType(bag, k8s.Type) {
		obj := item.Obj.(*k8s.Object)
		if obj.ObjList != nil {
			ro := obj.Obj.(runtime.Object)
			gk := ro.GetObjectKind().GroupVersionKind().GroupKind()
			if _, ok := gkmap[gk]; !ok {
				gkmap[gk] = struct{}{}
				mgk := metav1.GroupKind{
					Group: gk.Group,
					Kind:  gk.Kind,
				}
				a.Spec.ComponentGroupKinds = append(a.Spec.ComponentGroupKinds, mgk)
			}
		}
	}
	return a
}
