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

package airflowbase

// Major:
// TODO retry.Retry
//
// Minor:
// TODO reconcile based on hash(spec)
// TODO validation: assume resources and volume claims are validated by api server ?
// TODO parameterize controller using config maps for default images, versions, resources etc
// TODO documentation for CRD spec

import (
	"encoding/base64"
	app "github.com/kubernetes-sigs/application/pkg/apis/app/v1beta1"
	alpha1 "k8s.io/airflow-operator/pkg/apis/airflow/v1alpha1"
	"k8s.io/airflow-operator/pkg/controller/application"
	"k8s.io/airflow-operator/pkg/controller/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	gr "sigs.k8s.io/controller-reconciler/pkg/genericreconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"time"
)

// Add creates a new AirflowBase Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	r := newReconciler(mgr)
	return r.Controller(nil)
}

func newReconciler(mgr manager.Manager) *gr.Reconciler {
	return gr.
		WithManager(mgr).
		For(&alpha1.AirflowBase{}, alpha1.SchemeGroupVersion).
		Using(&MySQL{}).
		Using(&Postgres{}).
		Using(&SQLProxy{}).
		Using(&NFS{}).
		Using(&AirflowBase{}).
		WithErrorHandler(handleError).
		WithValidator(validate).
		WithDefaulter(applyDefaults).
		RegisterSchemeBuilder(app.SchemeBuilder).
		Build()
}

func handleError(resource interface{}, err error, kind string) {
	ab := resource.(*alpha1.AirflowBase)
	if err != nil {
		ab.Status.SetError("ErrorSeen", err.Error())
	} else {
		ab.Status.ClearError()
	}
}

func validate(resource interface{}) error {
	ab := resource.(*alpha1.AirflowBase)
	return ab.Validate()
}

func applyDefaults(resource interface{}) {
	ab := resource.(*alpha1.AirflowBase)
	ab.ApplyDefaults()
}

// AirflowBase - interface to handle airflowbase
type AirflowBase struct{}

// MySQL - interface to handle redis
type MySQL struct{}

// Postgres  - interface to handle flower
type Postgres struct{}

// SQLProxy - interface to handle scheduler
type SQLProxy struct{}

// NFS - interface to handle worker
type NFS struct{}

// =-------------------------- common ------------------------------------

func templateValue(r *alpha1.AirflowBase, component, altcomponent string, label, selector, ports map[string]string) *common.TemplateValue {
	if altcomponent == "" {
		altcomponent = component
	}
	return &common.TemplateValue{
		Name:       common.RsrcName(r.Name, component, ""),
		Namespace:  r.Namespace,
		SecretName: common.RsrcName(r.Name, altcomponent, ""),
		SvcName:    common.RsrcName(r.Name, altcomponent, ""),
		Base:       r,
		Labels:     label,
		Selector:   selector,
		Ports:      ports,
	}
}

// updateStatus use reconciled objects to update component status
func updateStatus(rsrc interface{}, reconciled []reconciler.Object, err error) time.Duration {
	var period time.Duration
	stts := &rsrc.(*alpha1.AirflowBase).Status
	ready := stts.ComponentMeta.UpdateStatus(reconciler.ObjectsByType(reconciled, k8s.Type))
	stts.Meta.UpdateStatus(&ready, err)
	return period
}

// ------------------------------ MYSQL  ---------------------------------------

func (s *MySQL) sts(o *reconciler.Object, v interface{}) {
	r := v.(*common.TemplateValue)
	sts := o.Obj.(*k8s.Object).Obj.(*appsv1.StatefulSet)
	sts.Spec.Template.Spec.Containers[0].Resources = r.Base.Spec.MySQL.Resources
	if r.Base.Spec.MySQL.VolumeClaimTemplate != nil {
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{*r.Base.Spec.MySQL.VolumeClaimTemplate}
	}
}

// Observables asd
func (s *MySQL) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&appsv1.StatefulSetList{}).
		For(&corev1.SecretList{}).
		For(&policyv1.PodDisruptionBudgetList{}).
		For(&corev1.ServiceList{}).
		Get()
}

// Objects returns the list of resource/name for those resources created by
// the operator for this spec and those resources referenced by this operator.
// Mark resources as owned, referred
func (s *MySQL) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowBase)
	if r.Spec.MySQL == nil {
		return []reconciler.Object{}, nil
	}
	ngdata := templateValue(r, common.ValueAirflowComponentMySQL, common.ValueAirflowComponentSQL, rsrclabels, rsrclabels, map[string]string{"mysql": "3306"})
	ngdata.Secret = map[string]string{
		"password":     base64.StdEncoding.EncodeToString(common.RandomAlphanumericString(16)),
		"rootpassword": base64.StdEncoding.EncodeToString(common.RandomAlphanumericString(16)),
	}
	ngdata.PDBMinAvail = "100%"

	return k8s.NewObjects().
		WithValue(ngdata).
		WithTemplate("mysql-sts.yaml", &appsv1.StatefulSetList{}, s.sts).
		WithTemplate("secret.yaml", &corev1.SecretList{}, reconciler.NoUpdate).
		WithTemplate("pdb.yaml", &policyv1.PodDisruptionBudgetList{}).
		WithTemplate("svc.yaml", &corev1.ServiceList{}).
		Build()
}

// UpdateStatus use reconciled objects to update component status
func (s *MySQL) UpdateStatus(rsrc interface{}, reconciled []reconciler.Object, err error) time.Duration {
	return updateStatus(rsrc, reconciled, err)
}

// ------------------------------ POSTGRES  ---------------------------------------

func (s *Postgres) sts(o *reconciler.Object, v interface{}) {
	r := v.(*common.TemplateValue)
	sts := o.Obj.(*k8s.Object).Obj.(*appsv1.StatefulSet)
	sts.Spec.Template.Spec.Containers[0].Resources = r.Base.Spec.Postgres.Resources
	if r.Base.Spec.Postgres.VolumeClaimTemplate != nil {
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{*r.Base.Spec.Postgres.VolumeClaimTemplate}
	}
}

// Observables asd
func (s *Postgres) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&appsv1.StatefulSetList{}).
		For(&corev1.SecretList{}).
		For(&policyv1.PodDisruptionBudgetList{}).
		For(&corev1.ServiceList{}).
		Get()
}

// Objects returns the list of resource/name for those resources created by
// the operator for this spec and those resources referenced by this operator.
// Mark resources as owned, referred
func (s *Postgres) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowBase)
	if r.Spec.Postgres == nil {
		return []reconciler.Object{}, nil
	}
	ngdata := templateValue(r, common.ValueAirflowComponentPostgres, common.ValueAirflowComponentSQL, rsrclabels, rsrclabels, map[string]string{"postgres": "5432"})
	ngdata.Secret = map[string]string{
		"password":     base64.StdEncoding.EncodeToString(common.RandomAlphanumericString(16)),
		"rootpassword": base64.StdEncoding.EncodeToString(common.RandomAlphanumericString(16)),
	}
	ngdata.PDBMinAvail = "100%"

	return k8s.NewObjects().
		WithValue(ngdata).
		WithTemplate("postgres-sts.yaml", &appsv1.StatefulSetList{}, s.sts).
		WithTemplate("secret.yaml", &corev1.SecretList{}, reconciler.NoUpdate).
		WithTemplate("pdb.yaml", &policyv1.PodDisruptionBudgetList{}).
		WithTemplate("svc.yaml", &corev1.ServiceList{}).
		Build()
}

// UpdateStatus use reconciled objects to update component status
func (s *Postgres) UpdateStatus(rsrc interface{}, reconciled []reconciler.Object, err error) time.Duration {
	return updateStatus(rsrc, reconciled, err)
}

// ------------------------------ NFSStoreSpec ---------------------------------------

// Observables asd
func (s *NFS) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&appsv1.StatefulSetList{}).
		For(&policyv1.PodDisruptionBudgetList{}).
		For(&corev1.ServiceList{}).
		Get()
}

// Objects returns the list of resource/name for those resources created by
func (s *NFS) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowBase)
	if r.Spec.Storage == nil {
		return []reconciler.Object{}, nil
	}
	ngdata := templateValue(r, common.ValueAirflowComponentNFS, "", rsrclabels, rsrclabels, map[string]string{"nfs": "2049", "mountd": "20048", "rpcbind": "111"})
	ngdata.PDBMinAvail = "100%"

	return k8s.NewObjects().
		WithValue(ngdata).
		WithTemplate("nfs-sts.yaml", &appsv1.StatefulSetList{}, s.sts).
		WithTemplate("pdb.yaml", &policyv1.PodDisruptionBudgetList{}).
		WithTemplate("svc.yaml", &corev1.ServiceList{}).
		Build()
}

func (s *NFS) sts(o *reconciler.Object, v interface{}) {
	r := v.(*common.TemplateValue)
	sts := o.Obj.(*k8s.Object).Obj.(*appsv1.StatefulSet)
	sts.Spec.Template.Spec.Containers[0].Resources = r.Base.Spec.Storage.Resources
	if r.Base.Spec.Storage.Volume != nil {
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{*r.Base.Spec.Storage.Volume}
	}
}

// UpdateStatus use reconciled objects to update component status
func (s *NFS) UpdateStatus(rsrc interface{}, reconciled []reconciler.Object, err error) time.Duration {
	return updateStatus(rsrc, reconciled, err)
}

// ------------------------------ SQLProxy ---------------------------------------

// Observables asd
func (s *SQLProxy) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&appsv1.StatefulSetList{}).
		For(&corev1.ServiceList{}).
		Get()
}

// Objects returns the list of resource/name for those resources created by
// the operator for this spec and those resources referenced by this operator.
// Mark resources as owned, referred
func (s *SQLProxy) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowBase)
	if r.Spec.SQLProxy == nil {
		return []reconciler.Object{}, nil
	}
	sqlname := common.RsrcName(r.Name, common.ValueAirflowComponentSQL, "")

	port := "3306"
	if r.Spec.SQLProxy.Type == common.ValueSQLProxyTypePostgres {
		port = "5432"
	}
	ngdata := templateValue(r, common.ValueAirflowComponentSQLProxy, "", rsrclabels, rsrclabels, map[string]string{"sqlproxy": port})
	ngdata.SvcName = sqlname

	return k8s.NewObjects().
		WithValue(ngdata).
		WithFolder("templates/").
		WithTemplate("sqlproxy-sts.yaml", &appsv1.StatefulSetList{}).
		WithTemplate("svc.yaml", &corev1.ServiceList{}).
		WithReferredItem(&corev1.Secret{}, sqlname, r.Namespace).
		Build()
}

// UpdateStatus use reconciled objects to update component status
func (s *SQLProxy) UpdateStatus(rsrc interface{}, reconciled []reconciler.Object, err error) time.Duration {
	return updateStatus(rsrc, reconciled, err)
}

// ---------------- Global AirflowBase component -------------------------

// Observables asd
func (s *AirflowBase) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&app.ApplicationList{}).
		Get()
}

// Objects returns the list of resource/name for those resources created by
// the operator for this spec and those resources referenced by this operator.
// Mark resources as owned, referred
func (s *AirflowBase) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowBase)
	selectors := make(map[string]string)
	for k, v := range rsrclabels {
		selectors[k] = v
	}
	delete(selectors, gr.LabelUsing)
	ngdata := templateValue(r, common.ValueAirflowComponentBase, "", rsrclabels, selectors, nil)
	ngdata.Expected = aggregated

	return k8s.NewObjects().
		WithValue(ngdata).
		WithTemplate("base-application.yaml", &app.ApplicationList{},
			func(o *reconciler.Object, v interface{}) {
				ao := application.NewApplication(o.Obj.(*k8s.Object).Obj)
				o = ao.SetSelector(r.Labels).
					SetComponentGK(aggregated).
					Item()
			}).
		Build()
}

// UpdateStatus use reconciled objects to update component status
func (s *AirflowBase) UpdateStatus(rsrc interface{}, reconciled []reconciler.Object, err error) time.Duration {
	return updateStatus(rsrc, reconciled, err)
}
