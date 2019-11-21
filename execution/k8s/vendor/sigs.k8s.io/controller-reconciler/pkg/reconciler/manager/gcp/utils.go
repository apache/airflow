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

package gcp

import (
	"cloud.google.com/go/compute/metadata"
	"flag"
	"google.golang.org/api/googleapi"
	"net/http"
	"regexp"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"strings"
)

var (
	projectID, zone string
)

func init() {
	// TODO: Fix this to allow double vendoring this library but still register flags on behalf of users
	flag.StringVar(&projectID, "gcpproject", "",
		"GCP Project ID. Required for outside cluster.")
	flag.StringVar(&zone, "gcpzone", "",
		"GCP Zone. Required for outside cluster.")
}

// CompliantLabelString - convert to GCP compliant string
// https://cloud.google.com/resource-manager/docs/creating-managing-labels
func CompliantLabelString(s string) string {
	s = strings.ToLower(s)
	reg, _ := regexp.Compile("[^a-z0-9_-]+")
	s = reg.ReplaceAllString(s, "-")
	return s
}

// CompliantLabelMap - convert to GCP compliant map
// https://cloud.google.com/resource-manager/docs/creating-managing-labels
func CompliantLabelMap(in map[string]string) map[string]string {
	out := make(map[string]string)
	for k, v := range in {
		out[CompliantLabelString(k)] = CompliantLabelString(v)
	}
	return out
}

// IsNotAuthorized - true if not-authorized error is returned
func IsNotAuthorized(err error) bool {
	return isGoogleErrorWithCode(err, http.StatusForbidden)
}

// IsNotFound - true if not-found error is returned
func IsNotFound(err error) bool {
	return isGoogleErrorWithCode(err, http.StatusNotFound)
}

func isGoogleErrorWithCode(err error, code int) bool {
	if err == nil {
		return false
	}
	if ge, ok := err.(*googleapi.Error); ok {
		return ge.Code == code
	}
	return false
}

// GetProjectFromMetadata - get metadata
func GetProjectFromMetadata() (string, error) {
	if projectID != "" {
		return projectID, nil
	}
	return metadata.ProjectID()
}

// GetZoneFromMetadata - get metadata
func GetZoneFromMetadata() (string, error) {
	if zone != "" {
		return zone, nil
	}
	return metadata.Zone()
}

// GetFilterStringFromLabels returns filter string
func GetFilterStringFromLabels(map[string]string) string {
	return ""
}

// Objects internal
type Objects struct {
	err     error
	context interface{}
	bag     []reconciler.Object
	labels  map[string]string
}

// NewObjects returns nag
func NewObjects() *Objects {
	return &Objects{
		bag: []reconciler.Object{},
	}
}

//WithContext injects context for mutators
func (b *Objects) WithContext(context interface{}) *Objects {
	b.context = context
	return b
}

//WithLabels injects labels
func (b *Objects) WithLabels(labels map[string]string) *Objects {
	b.labels = labels
	return b
}

//Build - process
func (b *Objects) Build() ([]reconciler.Object, error) {
	return b.bag, b.err
}

// Add - add obj
func (b *Objects) Add(o *reconciler.Object, err error) *Objects {
	if err != nil {
		b.err = err
	} else {
		o.Obj.SetLabels(b.labels)
		b.bag = append(b.bag, *o)
	}
	return b
}
