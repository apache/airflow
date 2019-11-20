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

package reconciler_test

import (
	//"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
)

type TemplateValues struct {
	Name      string
	Namespace string
	Selector  map[string]string
	Labels    map[string]string
	Image     string
	Version   string
	Replicas  int
}

type FooValues struct {
	Name      string
	Namespace string
}

var _ = Describe("Resource", func() {
	var val TemplateValues
	var fooval FooValues

	BeforeEach(func() {
	})

	Describe("ObjectFromFile", func() {
		It("Object loading from file returns error with missing template variable", func(done Done) {
			o, e := k8s.ObjectFromFile("testdata/sts.yaml", fooval, &appsv1.StatefulSetList{})
			//fmt.Printf("%v\n", e)
			Expect(e).NotTo(BeNil())
			Expect(o).To(BeNil())
			close(done)
		})
		It("Object loading from file returns error with empty template field for maps", func(done Done) {
			o, e := k8s.ObjectFromFile("testdata/sts.yaml", val, &appsv1.StatefulSetList{})
			//fmt.Printf("%v\n", e)
			Expect(e).NotTo(BeNil())
			Expect(o).To(BeNil())
			close(done)
		})
		It("Object loading from file returns error with missing template file", func(done Done) {
			o, e := k8s.ObjectFromFile("test/sts.yaml", val, &appsv1.StatefulSetList{})
			//fmt.Printf("%v\n", e)
			Expect(e).NotTo(BeNil())
			Expect(o).To(BeNil())
			close(done)
		})
		It("Object loading from file is successful", func(done Done) {
			val = TemplateValues{
				Name:      "myapp",
				Namespace: "default",
				Selector: map[string]string{
					"k1": "v1",
					"k2": "v2",
					"k3": "v3",
				},
				Labels: map[string]string{
					"k1": "v1",
					"k2": "v2",
					"k3": "v3",
					"k4": "v4",
				},
				Image:    "gcr.io/project/image",
				Version:  "v1.0",
				Replicas: 1,
			}
			o, e := k8s.ObjectFromFile("testdata/sts.yaml", val, &appsv1.StatefulSetList{})
			//fmt.Printf("%v\n", e)
			Expect(e).To(BeNil())
			Expect(o).NotTo(BeNil())
			obj := o.Obj.(*k8s.Object).Obj
			sts, ok := obj.(*appsv1.StatefulSet)
			Expect(ok).To(BeTrue())
			Expect(obj.GetName()).To(Equal("myapp"))
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(sts.ObjectMeta.Name).To(Equal("myapp"))
			close(done)
		})
		It("Object loading from file fails for unknown resource", func(done Done) {
			val = TemplateValues{
				Name:      "myapp",
				Namespace: "default",
				Selector: map[string]string{
					"k1": "v1",
				},
				Labels: map[string]string{
					"k1": "v1",
					"k2": "v2",
				},
			}
			o, e := k8s.ObjectFromFile("testdata/unknown_rsrc.yaml", val, &appsv1.StatefulSetList{})
			//fmt.Printf("%v\n", e)
			Expect(e).NotTo(BeNil())
			Expect(o).To(BeNil())
			close(done)
		})
	})

	Describe("ObjFromString", func() {
		svcspec := `
apiVersion: v1
kind: Service
metadata:
  name: {{.Name}}
  namespace: {{.Namespace}}
  labels:
    {{range $k,$v := .Labels }}
    {{$k}}: {{$v}}
    {{end}}
spec:
  ports:
  - port: 8080
    name: http
  selector:
    {{range $k,$v := .Selector }}
    {{$k}}: {{$v}}
    {{end}}
  type: ClusterIP
`

		It("Pod loading from string fails for incorrectly formatted spec", func(done Done) {
			o, e := k8s.ObjectFromString("svc", "bad pod spec", fooval, &corev1.ServiceList{})
			//fmt.Printf("%v\n", e)
			Expect(e).NotTo(BeNil())
			Expect(o).To(BeNil())
			close(done)
		})
		It("Service loading from string works for correctly formatted spec", func(done Done) {
			val = TemplateValues{
				Name:      "mysvc",
				Namespace: "default",
				Selector: map[string]string{
					"k1": "v1",
				},
				Labels: map[string]string{
					"k1": "v1",
					"k2": "v2",
				},
			}
			o, e := k8s.ObjectFromString("svc", svcspec, val, &corev1.ServiceList{})
			//fmt.Printf("%v\n", e)
			Expect(e).To(BeNil())
			Expect(o).NotTo(BeNil())
			obj := o.Obj.(*k8s.Object).Obj
			svc, ok := obj.(*corev1.Service)
			Expect(ok).To(BeTrue())
			Expect(obj.GetName()).To(Equal("mysvc"))
			Expect(obj.GetNamespace()).To(Equal("default"))
			Expect(svc.ObjectMeta.Name).To(Equal("mysvc"))
			close(done)
		})
	})
})
