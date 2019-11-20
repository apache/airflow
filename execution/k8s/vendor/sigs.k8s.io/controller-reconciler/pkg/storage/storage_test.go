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

package storage_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-reconciler/pkg/storage"
)

var _ = Describe("Resource", func() {
	gcs := storage.GCS{
		Bucket:   "asdasd",
		ReadOnly: true,
	}
	nfs := corev1.NFSVolumeSource{
		Server: "asdasd",
		Path:   "/asd",
	}
	gcsonly := storage.Spec{GCS: &gcs}
	nfsonly := storage.Spec{NFS: &nfs}
	both := storage.Spec{GCS: &gcs, NFS: &nfs}
	none := storage.Spec{}
	fp := field.NewPath("spec")

	BeforeEach(func() {
	})

	Describe("Validate", func() {
		fmt.Printf(".")
		It("validate expected specs exist", func(done Done) {
			var e field.ErrorList
			e = gcsonly.Validate(fp, "storage", e, false, true, map[string]string{storage.FieldGCS: storage.Expected})
			Expect(len(e)).To(Equal(0))
			e = nfsonly.Validate(fp, "storage", e, false, true, map[string]string{storage.FieldNFS: storage.Expected})
			Expect(len(e)).To(Equal(0))
			e = both.Validate(fp, "storage", e, false, false, map[string]string{storage.FieldNFS: storage.Expected, storage.FieldGCS: storage.Expected})
			Expect(len(e)).To(Equal(0))
			close(done)
		})
		It("validate missing expected specs", func(done Done) {
			var e field.ErrorList
			e = none.Validate(fp, "storage", e, false, false, map[string]string{storage.FieldNFS: storage.Expected})
			//fmt.Printf("%s", e.ToAggregate().Error())
			Expect(len(e)).To(Equal(2))

			e = gcsonly.Validate(fp, "storage", e, false, true, map[string]string{storage.FieldNFS: storage.Expected})
			//fmt.Printf("%s", e.ToAggregate().Error())
			Expect(len(e)).To(Equal(5))

			e = nfsonly.Validate(fp, "storage", e, false, true, map[string]string{storage.FieldGCS: storage.Expected})
			//fmt.Printf("%s", e.ToAggregate().Error())
			Expect(len(e)).To(Equal(8))

			close(done)
		})
		It("validate only one exist", func(done Done) {
			var e field.ErrorList
			e = nfsonly.Validate(fp, "storage", e, false, true, map[string]string{storage.FieldNFS: storage.Optional, storage.FieldGCS: storage.Optional})
			Expect(len(e)).To(Equal(0))

			e = both.Validate(fp, "storage", e, false, true, map[string]string{storage.FieldNFS: storage.Optional, storage.FieldGCS: storage.Optional})
			//fmt.Printf("%s", e.ToAggregate().Error())
			Expect(len(e)).To(Equal(1))

			e = none.Validate(fp, "storage", e, false, true, map[string]string{storage.FieldNFS: storage.Optional, storage.FieldGCS: storage.Optional})
			fmt.Printf("%s", e.ToAggregate().Error())
			Expect(len(e)).To(Equal(2))

			close(done)
		})
		It("validate no storage spec errors", func(done Done) {
			var e field.ErrorList
			e = none.Validate(fp, "storage", e, true, false, map[string]string{storage.FieldNFS: storage.Optional, storage.FieldGCS: storage.Optional})
			Expect(len(e)).To(Equal(0))

			close(done)
		})
	})
})
