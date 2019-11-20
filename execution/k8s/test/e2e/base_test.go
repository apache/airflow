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

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/airflow-operator/pkg/apis/airflow/v1alpha1"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"sigs.k8s.io/controller-reconciler/pkg/test"
)

const (
	CRName    = "AirflowBase"
	SampleDir = "../../hack/sample/"
)

var f *test.Framework
var ctx *test.Context

func airflowBase(file string) *v1alpha1.AirflowBase {
	cr := &v1alpha1.AirflowBase{}
	if err := f.LoadFromFile(file, cr); err != nil {
		return nil
	}
	return cr
}

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, CRName+" Suite")
}

var _ = BeforeSuite(func() {
	f = test.New(CRName)
	err := v1alpha1.SchemeBuilder.AddToScheme(f.GetScheme())
	Expect(err).NotTo(HaveOccurred(), "failed to initialize the Framework: %v", err)
	f.Start()
})

var _ = AfterSuite(func() {
	if ctx != nil {
		ctx.DeleteCR()
	}
	if f != nil {
		f.Stop()
	}
})

func isBaseReady(cr interface{}) bool {
	stts := cr.(*v1alpha1.AirflowBase).Status
	return stts.IsReady()
}

var _ = Describe(CRName+" controller tests", func() {
	AfterEach(func() {
		ctx.DeleteCR()
		ctx = nil
	})

	It("creating a "+CRName+" with mysql", func() {
		ctx = f.NewContext().WithCR(airflowBase(SampleDir + "mysql-celery/base.yaml"))
		cr := ctx.CR.(*v1alpha1.AirflowBase)
		By("creating a new " + CRName + ": " + cr.Name)
		ctx.CreateCR()
		ctx.WithTimeout(200).CheckStatefulSet(cr.Name+"-mysql", 1, 1)
		ctx.WithTimeout(10).CheckService(cr.Name+"-sql", map[string]int32{"mysql": 3306})
		//ctx.WithTimeout(10).CheckSecret(name)
		ctx.WithTimeout(200).CheckStatefulSet(cr.Name+"-nfs", 1, 1)
		ctx.WithTimeout(200).CheckCR(isBaseReady)
	})

	It("creating a "+CRName+" with postgres", func() {
		ctx = f.NewContext().WithCR(airflowBase(SampleDir + "postgres-celery/base.yaml"))
		cr := ctx.CR.(*v1alpha1.AirflowBase)
		By("creating a new " + CRName + ": " + cr.Name)
		ctx.CreateCR()
		ctx.WithTimeout(200).CheckStatefulSet(cr.Name+"-postgres", 1, 1)
		ctx.WithTimeout(10).CheckService(cr.Name+"-sql", map[string]int32{"postgres": 5432})
		//ctx.WithTimeout(10).CheckSecret(name)
		ctx.WithTimeout(200).CheckStatefulSet(cr.Name+"-nfs", 1, 1)
		ctx.WithTimeout(200).CheckCR(isBaseReady)
	})
})
