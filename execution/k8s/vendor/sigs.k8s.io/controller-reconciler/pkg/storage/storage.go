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

package storage

import (
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Validate validates the storage spec
func (s *Spec) Validate(fp *field.Path, sfield string, errs field.ErrorList, noneok bool, one bool, smap map[string]string) field.ErrorList {
	fp = fp.Child(sfield)
	if s == nil {
		if !noneok {
			errs = append(errs, field.Required(fp, "Required storage spec missing."))
		}
		return errs
	}

	found := 0
	if val, ok := smap[FieldGCS]; ok {
		if s.GCS != nil {
			found++
		} else if val == Expected {
			errs = append(errs, field.Required(fp.Child("gcs"), "gcs spec missing"))
		}
	} else if s.GCS != nil {
		errs = append(errs, field.Invalid(fp.Child("gcs"), "", "gcs spec not supported"))
	}

	if val, ok := smap[FieldNFS]; ok {
		if s.NFS != nil {
			found++
		} else if val == Expected {
			errs = append(errs, field.Required(fp.Child("nfs"), "nfs spec missing"))
		}
	} else if s.NFS != nil {
		errs = append(errs, field.Invalid(fp.Child("nfs"), "", "nfs spec not supported"))
	}

	if val, ok := smap[FieldFS]; ok {
		if s.FS != nil {
			found++
		} else if val == Expected {
			errs = append(errs, field.Required(fp.Child("fs"), "fs spec missing"))
		}
	} else if s.FS != nil {
		errs = append(errs, field.Invalid(fp.Child("fs"), "", "fs spec not supported"))
	}

	if found == 0 {
		if !noneok {
			errs = append(errs, field.Invalid(fp, "", "No storage specs present."))
		}
	} else if found > 1 && one {
		errs = append(errs, field.Invalid(fp, "", "Expecting only one storage. Multiple present."))
	}
	return errs
}
