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

package common

import (
	"bytes"
	"fmt"
	alpha1 "k8s.io/airflow-operator/pkg/apis/airflow/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"math/rand"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
	"time"
)

// Constants
const (
	PasswordCharNumSpace = "abcdefghijklmnopqrstuvwxyz0123456789"
	PasswordCharSpace    = "abcdefghijklmnopqrstuvwxyz"

	LabelAirflowCR                   = "airflow-cr"
	ValueAirflowCRBase               = "airflow-base"
	ValueAirflowCRCluster            = "airflow-cluster"
	LabelAirflowCRName               = "airflow-cr-name"
	LabelAirflowComponent            = "airflow-component"
	ValueAirflowComponentMemoryStore = "redis"
	ValueAirflowComponentMySQL       = "mysql"
	ValueAirflowComponentPostgres    = "postgres"
	ValueAirflowComponentSQLProxy    = "sqlproxy"
	ValueAirflowComponentBase        = "base"
	ValueAirflowComponentCluster     = "cluster"
	ValueAirflowComponentSQL         = "sql"
	ValueAirflowComponentUI          = "airflowui"
	ValueAirflowComponentNFS         = "nfs"
	ValueAirflowComponentRedis       = "redis"
	ValueAirflowComponentScheduler   = "scheduler"
	ValueAirflowComponentWorker      = "worker"
	ValueAirflowComponentFlower      = "flower"
	ValueSQLProxyTypeMySQL           = "mysql"
	ValueSQLProxyTypePostgres        = "postgres"
	LabelApp                         = "app"

	KindAirflowBase    = "AirflowBase"
	KindAirflowCluster = "AirflowCluster"

	PodManagementPolicyParallel = "Parallel"

	TemplatePath = "templates/"
)

var (
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func optionsToString(options map[string]string, prefix string) string {
	var buf bytes.Buffer
	for k, v := range options {
		buf.WriteString(fmt.Sprintf("%s%s %s ", prefix, k, v))
	}
	return buf.String()
}

// RandomAlphanumericString generates a random password of some fixed length.
func RandomAlphanumericString(strlen int) []byte {
	result := make([]byte, strlen)
	for i := range result {
		result[i] = PasswordCharNumSpace[random.Intn(len(PasswordCharNumSpace))]
	}
	result[0] = PasswordCharSpace[random.Intn(len(PasswordCharSpace))]
	return result
}

// RsrcName - create name
func RsrcName(name string, component string, suffix string) string {
	return name + "-" + component + suffix
}

// TemplateValue replacer
type TemplateValue struct {
	Name        string
	Namespace   string
	SecretName  string
	SvcName     string
	Base        *alpha1.AirflowBase
	Cluster     *alpha1.AirflowCluster
	Labels      reconciler.KVMap
	Selector    reconciler.KVMap
	Ports       map[string]string
	Secret      map[string]string
	PDBMinAvail string
	Expected    []reconciler.Object
	SQLConn     string
}

// differs returns true if the resource needs to be updated
func differs(expected reconciler.Object, observed reconciler.Object) bool {
	switch expected.Obj.(*k8s.Object).Obj.(type) {
	case *corev1.ServiceAccount:
		// Dont update a SA
		return false
	case *corev1.Secret:
		// Dont update a secret
		return false
	}
	return true
}
