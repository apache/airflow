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
	"context"
	"fmt"
	apierror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	urt "k8s.io/apimachinery/pkg/util/runtime"
	"log"
	"reflect"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	rmanager "sigs.k8s.io/controller-reconciler/pkg/reconciler/manager"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
	build "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/runtime/scheme"
	"time"
)

// Constants
const (
	DefaultReconcilePeriod      = 3 * time.Minute
	FinalizeReconcilePeriod     = 30 * time.Second
	CRGetFailureReconcilePeriod = 30 * time.Second
)

func handleErrorArr(info string, name string, e error, errs []error) []error {
	handleError(info, name, e)
	return append(errs, e)
}

// handleError common error handling routine
func handleError(info string, name string, e error) error {
	urt.HandleError(fmt.Errorf("Failed: [%s] %s. %s", name, info, e.Error()))
	return e
}

func (gr *Reconciler) itemMgr(i reconciler.Object) (rmanager.Manager, error) {
	m := gr.rsrcMgr.Get(i.Type)
	if m == nil {
		return m, fmt.Errorf("Resource Manager not registered for Type: %s", i.Type)
	}
	return m, nil
}

// ReconcileResource is a generic function that reconciles expected and observed resources
func (gr *Reconciler) ReconcileResource(namespacedname types.NamespacedName) (reconcile.Result, error) {
	var p time.Duration
	period := DefaultReconcilePeriod
	expected := []reconciler.Object{}
	resource := gr.resource.(runtime.Object).DeepCopyObject()
	nname := reflect.TypeOf(resource).String() + "/" + namespacedname.String()
	rm := gr.rsrcMgr.Get("k8s")
	err := k8s.Get(rm, namespacedname, resource.(runtime.Object))
	if err != nil {
		if apierror.IsNotFound(err) {
			urt.HandleError(fmt.Errorf("not found %s. %s", nname, err.Error()))
			return reconcile.Result{}, nil
		}
		return reconcile.Result{RequeueAfter: CRGetFailureReconcilePeriod}, err
	}

	errkind := ""
	if gr.validate != nil {
		log.Printf("%s Validating spec\n", nname)
		err = gr.validate(resource)
	}

	if err == nil {
		if gr.applyDefaults != nil {
			log.Printf("%s Applying defaults\n", nname)
			gr.applyDefaults(resource)
		}
		o := resource.(metav1.Object)

		for _, h := range gr.using {

			if o.GetDeletionTimestamp() == nil {
				p, err = gr.reconcileUsing(h, resource, nname, expected)
			} else {
				err = gr.finalizeUsing(h, resource, nname, expected)
				p = FinalizeReconcilePeriod
			}
			if p != 0 && p < period {
				period = p
			}

			if err != nil {
				break
			}
		}
	} else {
		errkind = "validate"
	}

	if err != nil {
		urt.HandleError(fmt.Errorf("error reconciling %s. %s", nname, err.Error()))
		if gr.errorHandler != nil {
			gr.errorHandler(resource, err, errkind)
		}
	}

	err = rm.Update(reconciler.Object{Obj: &k8s.Object{Obj: resource.(metav1.Object)}})
	if err != nil {
		urt.HandleError(fmt.Errorf("error updating %s. %s", nname, err.Error()))
	}
	return reconcile.Result{RequeueAfter: period}, err
}

func (gr *Reconciler) observe(observables []reconciler.Observable) ([]reconciler.Object, error) {
	seen := []reconciler.Object{}
	for _, m := range gr.rsrcMgr.All() {
		o, err := m.Observe(observables...)
		if err != nil {
			return []reconciler.Object{}, err
		}
		seen = append(seen, o...)
	}
	return seen, nil
}

// observeAndMutate is a function that is called to observe and mutate expected resources
func (gr *Reconciler) observeAndMutate(h Handler, resource runtime.Object, crname string, aggregated []reconciler.Object) ([]reconciler.Object, []reconciler.Object, []reconciler.Object, string, error) {
	var err error
	var expected, observed, dependent []reconciler.Object

	// Get dependenta objects

	// Loop over all RMs
	stage := "dependent resources"
	labels := getLabels(resource, reflect.TypeOf(h).String())
	dependent, err = gr.observe(getAllObservables(&gr.rsrcMgr, dependentResources(h, resource), labels))
	if err == nil && dependent != nil {

		// TODO get and the nobjects

		// Get Observe observables
		stage = "observing resources"
		observed, err = gr.observe(h.Observables(resource, labels, dependent))
		if err == nil && observed != nil {
			// Get Expected resources
			stage = "gathering expected resources"
			expected, err = h.Objects(resource, labels, observed, dependent, aggregated)
		}
	}
	if err != nil {
		observed = []reconciler.Object{}
		expected = []reconciler.Object{}
	}
	if expected == nil {
		expected = []reconciler.Object{}
	}
	if observed == nil {
		observed = []reconciler.Object{}
	}
	return expected, observed, dependent, stage, err
}

// finalizeUsing is a function that finalizes component
func (gr *Reconciler) finalizeUsing(h Handler, resource runtime.Object, crname string, aggregated []reconciler.Object) error {
	cname := crname + "(using:" + reflect.TypeOf(h).String() + ")"
	log.Printf("%s  { finalizing\n", cname)
	defer log.Printf("%s  } finalizing\n", cname)

	expected, observed, dependent, stage, err := gr.observeAndMutate(h, resource, crname, aggregated)

	if err != nil {
		handleError(stage, crname, err)
	}
	aggregated = append(aggregated, expected...)
	stage = "finalizing resource"
	err = finalize(h, resource, observed, dependent)
	if err == nil {
		stage = "finalizing deletion of objects"
		for _, o := range observed {
			oRsrcName := o.Obj.GetName()
			if o.Delete {
				if rm, e := gr.itemMgr(o); e != nil {
					err = e
					break
				} else if e := rm.Delete(o); e != nil {
					err = e
					break
				} else {
					log.Printf("%s   -delete: %s\n", cname, oRsrcName)
				}

			}
		}
	}
	if err != nil {
		handleError(stage, crname, err)
	}
	return err
}

func (gr *Reconciler) ownerRef(resource runtime.Object) *metav1.OwnerReference {
	return metav1.NewControllerRef(resource.(metav1.Object), schema.GroupVersionKind{
		Group:   gr.gv.Group,
		Version: gr.gv.Version,
		Kind:    reflect.TypeOf(gr.resource).String(),
	})
}

// reconcileUsing is a generic function that reconciles expected and observed resources
func (gr *Reconciler) reconcileUsing(h Handler, resource runtime.Object, crname string, aggregated []reconciler.Object) (time.Duration, error) {
	errs := []error{}
	var reconciled []reconciler.Object

	cname := crname + "(cmpnt:" + reflect.TypeOf(h).String() + ")"
	log.Printf("%s  { reconciling component\n", cname)
	defer log.Printf("%s  } reconciling component\n", cname)

	expected, observed, _, stage, err := gr.observeAndMutate(h, resource, crname, aggregated)

	// Reconciliation logic is straight-forward:
	// This method gets the list of expected resources and observed resources
	// We compare the 2 lists and:
	//  create(rsrc) where rsrc is in expected but not in observed
	//  delete(rsrc) where rsrc is in observed but not in expected
	//  update(rsrc) where rsrc is in observed and expected
	//
	// We have a notion of Managed and Referred resources
	// Only Managed resources are CRUD'd
	// Missing Reffered resources are treated as errors and surfaced as such in the status field
	//

	if err != nil {
		errs = handleErrorArr(stage, crname, err, errs)
	} else {
		aggregated = append(aggregated, expected...)
		log.Printf("%s  Expected Resources:\n", cname)
		for _, e := range expected {
			log.Printf("%s   exp: %s %s\n", cname, e.Type, e.Obj.GetName())
		}
		log.Printf("%s  Observed Resources:\n", cname)
		for _, e := range observed {
			log.Printf("%s   obs: %s\n", cname, e.Obj.GetName())
		}

		log.Printf("%s  Reconciling Resources:\n", cname)
	}
	for _, e := range expected {
		seen := false
		eRsrcName := e.Obj.GetName()
		for _, o := range observed {
			if e.Type != o.Type || !e.Obj.IsSameAs(o.Obj) {
				continue
			}
			// rsrc is seen in both expected and observed, update it if needed
			reconciled = append(reconciled, o)
			seen = true

			if e.Lifecycle == reconciler.LifecycleReferred {
				log.Printf("%s   notmanaged: %s\n", cname, eRsrcName)
				break
			}

			rm, err := gr.itemMgr(e)
			if err != nil {
				errs = handleErrorArr("update", eRsrcName, err, errs)
				break
			}

			// canupdate
			canupdate := e.Lifecycle != reconciler.LifecycleNoUpdate
			// Component Differs is not expected to mutate e based on o
			compDiffers := differs(h, e, o)
			// Resource Manager Differs can mutate e based on o
			rmDiffers := rm.SpecDiffers(&e, &o)
			refchange := e.Obj.SetOwnerReferences(gr.ownerRef(resource))
			if canupdate && rmDiffers && compDiffers || refchange {
				if err := rm.Update(e); err != nil {
					errs = handleErrorArr("update", eRsrcName, err, errs)
				} else {
					log.Printf("%s   update: %s\n", cname, eRsrcName)
				}
			} else {
				log.Printf("%s   nochange: %s\n", cname, eRsrcName)
			}
			break
		}
		// rsrc is in expected but not in observed - create
		if !seen {
			if e.Lifecycle != reconciler.LifecycleReferred {
				e.Obj.SetOwnerReferences(gr.ownerRef(resource))
				if rm, err := gr.itemMgr(e); err != nil {
					errs = handleErrorArr("Create", cname, err, errs)
				} else if err := rm.Create(e); err != nil {
					errs = handleErrorArr("Create", cname, err, errs)
				} else {
					log.Printf("%s   +create: %s\n", cname, eRsrcName)
					reconciled = append(reconciled, e)
				}
			} else {
				err := fmt.Errorf("missing resource not managed by %s: %s", cname, eRsrcName)
				errs = handleErrorArr("missing resource", cname, err, errs)
			}
		}
	}

	// delete(observed - expected)
	for _, o := range observed {
		seen := false
		oRsrcName := o.Obj.GetName()
		if o.Lifecycle == reconciler.LifecycleDecorate {
			if o.Update {
				if rm, err := gr.itemMgr(o); err != nil {
					errs = handleErrorArr("decorate", oRsrcName, err, errs)
				} else if err := rm.Update(o); err != nil {
					errs = handleErrorArr("update", oRsrcName, err, errs)
				} else {
					log.Printf("%s   decorate: %s\n", cname, oRsrcName)
				}
			}
			continue
		}
		for _, e := range expected {
			if e.Type == o.Type && e.Obj.IsSameAs(o.Obj) {
				seen = true
				break
			}
		}
		// rsrc is in observed but not in expected - delete
		if !seen {
			if rm, err := gr.itemMgr(o); err != nil {
				errs = handleErrorArr("delete", oRsrcName, err, errs)
			} else if err := rm.Delete(o); err != nil {
				errs = handleErrorArr("delete", oRsrcName, err, errs)
			} else {
				log.Printf("%s   -delete: %s\n", cname, oRsrcName)
			}
		}
	}

	err = utilerrors.NewAggregate(errs)
	period := updateStatus(h, resource, reconciled, err)
	return period, err
}

// Reconcile expected by kubebuilder
func (gr *Reconciler) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	r, err := gr.ReconcileResource(request.NamespacedName)
	if err != nil {
		fmt.Printf("err: %s", err.Error())
	}
	return r, err
}

// WithManager - add manager
func WithManager(m manager.Manager) *Reconciler {
	gr := new(Reconciler)
	gr.manager = m
	return gr
}

// For - register api type
func (gr *Reconciler) For(resource runtime.Object, gv schema.GroupVersion) *Reconciler {
	gr.resource = resource
	gr.gv = gv
	return gr
}

// Using - using handler
func (gr *Reconciler) Using(h Handler) *Reconciler {
	gr.using = append(gr.using, h)
	return gr
}

// WithResourceManager - call getters
func (gr *Reconciler) WithResourceManager(getter func() (string, rmanager.Manager, error)) *Reconciler {
	mgrtype, mgr, err := getter()
	if err == nil {
		gr.rsrcMgr.Add(mgrtype, mgr)
	}
	return gr
}

// AddToSchemes for adding Application to scheme
var AddToSchemes runtime.SchemeBuilder

// RegisterSchemeBuilder - create controller
func (gr *Reconciler) RegisterSchemeBuilder(builder *scheme.Builder) *Reconciler {
	AddToSchemes = append(AddToSchemes, builder.AddToScheme)
	return gr
}

// Build - create controller
func (gr *Reconciler) Build() *Reconciler {
	km := k8s.NewRsrcManager(context.TODO(), gr.manager.GetClient(), gr.manager.GetScheme())
	gr.rsrcMgr.Add(k8s.Type, km)
	AddToSchemes.AddToScheme(gr.manager.GetScheme())
	return gr
}

// Controller = create controller
func (gr *Reconciler) Controller(r reconcile.Reconciler) error {
	if r == nil {
		r = gr
	}
	_, err := build.SimpleController().
		WithManager(gr.manager).
		ForType(gr.resource).
		Build(r)
	return err
}

// WithErrorHandler - callback for error handling
func (gr *Reconciler) WithErrorHandler(eh func(interface{}, error, string)) *Reconciler {
	gr.errorHandler = eh
	return gr
}

// WithValidator - callback for error handling
func (gr *Reconciler) WithValidator(v func(interface{}) error) *Reconciler {
	gr.validate = v
	return gr
}

// WithDefaulter - callback for error handling
func (gr *Reconciler) WithDefaulter(d func(interface{})) *Reconciler {
	gr.applyDefaults = d
	return gr
}
