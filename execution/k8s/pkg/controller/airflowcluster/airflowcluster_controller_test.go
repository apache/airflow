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

package airflowcluster

import (
	"testing"
	"time"

	"github.com/onsi/gomega"
	"golang.org/x/net/context"
	airflowv1alpha1 "k8s.io/airflow-operator/pkg/apis/airflow/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var c client.Client

var expectedRequest = reconcile.Request{NamespacedName: types.NamespacedName{Name: "foo", Namespace: "default"}}
var rediskey = types.NamespacedName{Name: "foo-redis", Namespace: "default"}
var uikey = types.NamespacedName{Name: "foo-airflowui", Namespace: "default"}
var workerkey = types.NamespacedName{Name: "foo-worker", Namespace: "default"}
var flowerkey = types.NamespacedName{Name: "foo-flower", Namespace: "default"}
var schedulerkey = types.NamespacedName{Name: "foo-scheduler", Namespace: "default"}

const timeout = time.Second * 5

func TestReconcile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	base := &airflowv1alpha1.AirflowBase{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "default"},
		Spec: airflowv1alpha1.AirflowBaseSpec{
			MySQL: &airflowv1alpha1.MySQLSpec{
				Operator: false,
			},
			Storage: &airflowv1alpha1.NFSStoreSpec{
				Version: "",
			},
		},
	}

	cluster := &airflowv1alpha1.AirflowCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "default"},
		Spec: airflowv1alpha1.AirflowClusterSpec{
			Executor:  "Celery",
			Redis:     &airflowv1alpha1.RedisSpec{Operator: false},
			Scheduler: &airflowv1alpha1.SchedulerSpec{Version: "1.10.2"},
			UI:        &airflowv1alpha1.AirflowUISpec{Replicas: 1, Version: "1.10.2"},
			Worker:    &airflowv1alpha1.WorkerSpec{Replicas: 2, Version: "1.10.2"},
			Flower:    &airflowv1alpha1.FlowerSpec{Replicas: 1, Version: "1.10.2"},
			DAGs: &airflowv1alpha1.DagSpec{
				DagSubdir: "airflow/example_dags/",
				Git: &airflowv1alpha1.GitSpec{
					Repo: "https://github.com/apache/incubator-airflow/",
					Once: true,
				},
			},
			AirflowBaseRef: &corev1.LocalObjectReference{Name: "foo"},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(cfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	c = mgr.GetClient()

	r := newReconciler(mgr)
	recFn, requests := SetupTestReconcile(r)
	g.Expect(r.Controller(recFn)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	// Create the AirflowCluster object and expect the Reconcile and Deployment to be created
	err = c.Create(context.TODO(), base)
	err = c.Create(context.TODO(), cluster)
	// The cluster object may not be a valid object because it might be missing some required fields.
	// Please modify the cluster object by adding required fields and then remove the following if statement.
	if apierrors.IsInvalid(err) {
		t.Logf("failed to create object, got an invalid object error: %v", err)
		return
	}
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer c.Delete(context.TODO(), cluster)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))

	redis := &appsv1.StatefulSet{}
	ui := &appsv1.StatefulSet{}
	worker := &appsv1.StatefulSet{}
	flower := &appsv1.StatefulSet{}
	scheduler := &appsv1.StatefulSet{}
	g.Eventually(func() error { return c.Get(context.TODO(), rediskey, redis) }, timeout).Should(gomega.Succeed())
	g.Eventually(func() error { return c.Get(context.TODO(), uikey, ui) }, timeout).Should(gomega.Succeed())
	g.Eventually(func() error { return c.Get(context.TODO(), workerkey, worker) }, timeout).Should(gomega.Succeed())
	g.Eventually(func() error { return c.Get(context.TODO(), schedulerkey, scheduler) }, timeout).Should(gomega.Succeed())
	g.Eventually(func() error { return c.Get(context.TODO(), flowerkey, flower) }, timeout).Should(gomega.Succeed())

	// Delete the Deployment and expect Reconcile to be called for Deployment deletion
	g.Expect(c.Delete(context.TODO(), scheduler)).NotTo(gomega.HaveOccurred())
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))
	g.Eventually(func() error { return c.Get(context.TODO(), schedulerkey, scheduler) }, timeout).
		Should(gomega.Succeed())

	// Manually delete Deployment since GC isn't enabled in the test control plane
	g.Expect(c.Delete(context.TODO(), redis)).To(gomega.Succeed())
	g.Expect(c.Delete(context.TODO(), ui)).To(gomega.Succeed())
	g.Expect(c.Delete(context.TODO(), worker)).To(gomega.Succeed())
	g.Expect(c.Delete(context.TODO(), flower)).To(gomega.Succeed())
	g.Expect(c.Delete(context.TODO(), scheduler)).To(gomega.Succeed())
}
