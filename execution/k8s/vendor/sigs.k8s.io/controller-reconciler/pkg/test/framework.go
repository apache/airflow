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

package test

import (
	"context"
	"flag"
	"fmt"
	"time"

	gi "github.com/onsi/ginkgo"
	g "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const ()

var (
	ns = flag.String("namespace", "", "e2e test namespace")
)

// Framework is responsible for setting up the test environment.
type Framework struct {
	typename  string
	Namespace string
	mgr       manager.Manager
	stopCh    chan struct{}
	client    client.Client
}

// Context context
type Context struct {
	CR      metav1.Object
	timeout time.Duration
	F       *Framework
}

// New framework
func New(typename string) *Framework {
	gi.By("Initializing the framework")
	flag.Parse()

	cfg, err := config.GetConfig()
	g.Expect(err).NotTo(g.HaveOccurred(), "failed to initialize the Framework: %v", err)

	mgr, err := manager.New(cfg, manager.Options{})
	g.Expect(err).NotTo(g.HaveOccurred(), "failed to initialize the Framework: %v", err)

	err = apiextensionsv1beta1.AddToScheme(mgr.GetScheme())
	g.Expect(err).NotTo(g.HaveOccurred(), "failed to initialize the Framework: %v", err)

	namespace := *ns
	if namespace == "" {
		namespace = "default"
	}
	return &Framework{
		typename:  typename,
		Namespace: namespace,
		mgr:       mgr,
		client:    mgr.GetClient(),
		stopCh:    make(chan struct{}),
	}
}

// NewContext return new context
func (f *Framework) NewContext() *Context {
	return &Context{
		F:       f,
		timeout: 120,
	}
}

// WithCR inject CR
func (c *Context) WithCR(cr metav1.Object) *Context {
	c.CR = cr
	c.CR.SetNamespace(c.F.Namespace)
	return c
}

// WithTimeout set timeout
func (c *Context) WithTimeout(t time.Duration) *Context {
	c.timeout = t
	return c
}

// GetScheme get scheme
func (f *Framework) GetScheme() *runtime.Scheme {
	return f.mgr.GetScheme()
}

// Start setup
func (f *Framework) Start() {
	// Start the manager so the watcher/cache are properly setup and started.
	go f.mgr.Start(f.stopCh)
}

// Stop setup
func (f *Framework) Stop() {
	gi.By("Tearing down the Framework")
	// Stop the manager.
	defer close(f.stopCh)
}

// DeleteCR delete CR
func (c *Context) DeleteCR() {
	err := c.F.client.Delete(context.Background(), c.CR.(runtime.Object))
	g.Expect(err).NotTo(g.HaveOccurred(), "failed to delete CR %s: %v", c.CR.GetName(), err)
}

// UpdateCR update CR
func (c *Context) UpdateCR() {
	err := c.F.client.Update(context.Background(), c.CR.(runtime.Object))
	g.Expect(err).NotTo(g.HaveOccurred(), "failed to update CR %s: %v", c.CR.GetName(), err)
}

// RefreshCR syncs to latest CR in api server
func (c *Context) RefreshCR() {
	err := c.F.client.Get(context.Background(), types.NamespacedName{Name: c.CR.GetName(), Namespace: c.CR.GetNamespace()}, c.CR.(runtime.Object))
	g.Expect(err).NotTo(g.HaveOccurred(), "failed to refresh CR %s: %v", c.CR.GetName(), err)
}

// CheckCR syncs to latest CR in api server
func (c *Context) CheckCR(validationFuncs ...func(interface{}) bool) {

	name := c.CR.GetName()
	f := c.F
	gi.By("Checking the CR for " + f.typename + " name: " + name)
	err := wait.Poll(5000*time.Millisecond, c.timeout*time.Second, func() (bool, error) {
		err := f.client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: c.CR.GetNamespace()}, c.CR.(runtime.Object))
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}

		for _, fn := range validationFuncs {
			if !fn(c.CR) {
				return false, nil
			}
		}

		return true, nil
	})
	if err != nil {
		g.Expect(err).NotTo(g.HaveOccurred(), "failed to check the CustomResource: %s, %v", name, err)
	}
}

// CreateCR create CR
func (c *Context) CreateCR() {
	err := c.F.client.Create(context.Background(), c.CR.(runtime.Object))
	g.Expect(err).NotTo(g.HaveOccurred(), "failed to create CR %s: %v", c.CR.GetName(), err)
}

// CheckStatefulSet check sts
func (c *Context) CheckStatefulSet(
	name string,
	replicas int32,
	generation int64) {
	f := c.F
	gi.By("Checking the Statefulset for " + f.typename + " name: " + name)
	err := wait.Poll(5000*time.Millisecond, c.timeout*time.Second, func() (done bool, err error) {
		set := &appsv1.StatefulSet{}
		err = f.client.Get(context.TODO(), types.NamespacedName{Name: name, Namespace: f.Namespace}, set)
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("failed to get StatefulSet %s: %v", name, err)
		}

		if (set.Spec.Replicas != nil && replicas != *set.Spec.Replicas) ||
			replicas != set.Status.Replicas ||
			replicas != set.Status.ReadyReplicas {
			return false, nil
		}

		if set.Status.ObservedGeneration < generation {
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		g.Expect(err).NotTo(g.HaveOccurred(), "failed to check the statefulset: %s, %v", name, err)
	}
}

// DeleteStatefulSet delete sts
func (c *Context) DeleteStatefulSet(name string) error {
	set := &appsv1.StatefulSet{}
	if err := c.F.client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: c.F.Namespace}, set); err != nil {
		return err
	}
	return c.F.client.Delete(context.Background(), set)
}

// CheckService check service
func (c *Context) CheckService(name string, portMap map[string]int32) {
	f := c.F

	gi.By("Checking the Service for " + f.typename + " name: " + name)
	err := wait.Poll(5000*time.Millisecond, c.timeout*time.Second, func() (bool, error) {
		// Check the presence of the given service.
		svc := &corev1.Service{}
		err := f.client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: f.Namespace}, svc)
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}

		if len(svc.Spec.Ports) != len(portMap) {
			g.Expect(err).NotTo(g.HaveOccurred(), "failed to check the service: %s, %v", name, err)
		}

		for _, port := range svc.Spec.Ports {
			if value, ok := portMap[port.Name]; !ok {
				return false, nil
			} else if value != port.Port {
				return false, nil
			}
		}

		return true, nil
	})
	if err != nil {
		g.Expect(err).NotTo(g.HaveOccurred(), "failed to check the service: %s, %v", name, err)
	}
}

// CheckConfigMap check config map
func (c *Context) CheckConfigMap(name string, validationFuncs ...func(map[string]string) bool) {
	f := c.F
	gi.By("Checking the Configmap for " + f.typename + " name: " + name)
	err := wait.Poll(5000*time.Millisecond, c.timeout*time.Second, func() (bool, error) {
		cmap := &corev1.ConfigMap{}
		err := f.client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: f.Namespace}, cmap)
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}

		for _, fn := range validationFuncs {
			if !fn(cmap.Data) {
				return false, nil
			}
		}

		return true, nil
	})
	if err != nil {
		g.Expect(err).NotTo(g.HaveOccurred(), "failed to check the statefulset: %s, %v", name, err)
	}
}

// LoadFromFile load from file
func (f *Framework) LoadFromFile(path string, obj interface{}) error {
	file, err := OpenFile(path)
	if err != nil {
		return nil
	}
	defer file.Close()

	err = yaml.NewYAMLOrJSONDecoder(file, 128).Decode(obj)
	return err
}
