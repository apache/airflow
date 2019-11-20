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

package genericreconciler

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	rm "sigs.k8s.io/controller-reconciler/pkg/reconciler/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ reconcile.Reconciler = &Reconciler{}

// Reconciler defines fields needed for all airflow controllers
// +k8s:deepcopy-gen=false
type Reconciler struct {
	gv            schema.GroupVersion
	errorHandler  func(interface{}, error, string)
	validate      func(interface{}) error
	applyDefaults func(interface{})
	resource      runtime.Object
	manager       manager.Manager
	rsrcMgr       rm.ResourceManager
	using         []Handler
}
