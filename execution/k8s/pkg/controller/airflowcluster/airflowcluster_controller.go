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
	"context"
	"encoding/base64"
	app "github.com/kubernetes-sigs/application/pkg/apis/app/v1beta1"
	alpha1 "k8s.io/airflow-operator/pkg/apis/airflow/v1alpha1"
	"k8s.io/airflow-operator/pkg/controller/application"
	"k8s.io/airflow-operator/pkg/controller/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-reconciler/pkg/finalizer"
	gr "sigs.k8s.io/controller-reconciler/pkg/genericreconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/gcp"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/gcp/redis"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
	"sigs.k8s.io/controller-reconciler/pkg/status"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	afk             = "AIRFLOW__KUBERNETES__"
	afc             = "AIRFLOW__CORE__"
	gitSyncDestDir  = "gitdags"
	gCSSyncDestDir  = "dags"
	airflowHome     = "/usr/local/airflow"
	airflowDagsBase = airflowHome + "/dags/"
)

// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=airflow.k8s.io,resources=airflowbases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=airflow.k8s.io,resources=airflowclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=app.k8s.io,resources=applications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch;create;update;patch;delete

// Add creates a new AirflowBase Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	r := newReconciler(mgr)
	return r.Controller(nil)
}

func newReconciler(mgr manager.Manager) *gr.Reconciler {
	return gr.
		WithManager(mgr).
		WithResourceManager(redis.Getter(context.TODO())).
		For(&alpha1.AirflowCluster{}, alpha1.SchemeGroupVersion).
		Using(&UI{}).
		Using(&Redis{}).
		Using(&MemoryStore{}).
		Using(&Flower{}).
		Using(&Scheduler{}).
		Using(&Worker{}).
		Using(&Cluster{}).
		WithErrorHandler(handleError).
		WithValidator(validate).
		WithDefaulter(applyDefaults).
		RegisterSchemeBuilder(app.SchemeBuilder).
		Build()
}

func handleError(resource interface{}, err error, kind string) {
	ac := resource.(*alpha1.AirflowCluster)
	if err != nil {
		ac.Status.SetError("ErrorSeen", err.Error())
	} else {
		ac.Status.ClearError()
	}
}

func validate(resource interface{}) error {
	ac := resource.(*alpha1.AirflowCluster)
	return ac.Validate()
}

func applyDefaults(resource interface{}) {
	ac := resource.(*alpha1.AirflowCluster)
	ac.ApplyDefaults()
}

// Cluster - interface to handle airflowbase
type Cluster struct{}

// Redis - interface to handle redis
type Redis struct{}

// Flower - interface to handle flower
type Flower struct{}

// Scheduler - interface to handle scheduler
type Scheduler struct{}

// Worker - interface to handle worker
type Worker struct{}

// UI - interface to handle ui
type UI struct{}

// MemoryStore - interface to handle memorystore
type MemoryStore struct{}

// --------------- common functions -------------------------

func envFromSecret(name string, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: name,
			},
			Key: key,
		},
	}
}

// IsPostgres return true for postgres
func IsPostgres(s *alpha1.AirflowBaseSpec) bool {
	postgres := false
	if s.Postgres != nil {
		postgres = true
	}
	if s.SQLProxy != nil && s.SQLProxy.Type == common.ValueSQLProxyTypePostgres {
		postgres = true
	}
	return postgres
}

func updateSts(o *reconciler.Object, v interface{}) (*appsv1.StatefulSet, *common.TemplateValue) {
	r := v.(*common.TemplateValue)
	sts := o.Obj.(*k8s.Object).Obj.(*appsv1.StatefulSet)
	sts.Spec.Template.Spec.Containers[0].Env = getAirflowEnv(r.Cluster, sts.Name, r.Base)
	addAirflowContainers(r.Cluster, sts)
	return sts, r
}

func templateValue(r *alpha1.AirflowCluster, dependent []reconciler.Object, component string, label, selector, ports map[string]string) *common.TemplateValue {
	b := k8s.GetItem(dependent, &alpha1.AirflowBase{}, r.Spec.AirflowBaseRef.Name, r.Namespace)
	base := b.(*alpha1.AirflowBase)
	return &common.TemplateValue{
		Name:       common.RsrcName(r.Name, component, ""),
		Namespace:  r.Namespace,
		SecretName: common.RsrcName(r.Name, component, ""),
		SvcName:    common.RsrcName(r.Name, component, ""),
		Cluster:    r,
		Base:       base,
		Labels:     label,
		Selector:   selector,
		Ports:      ports,
	}
}

func addAirflowContainers(r *alpha1.AirflowCluster, ss *appsv1.StatefulSet) {
	if r.Spec.DAGs != nil {
		init, dc := dagContainer(r.Spec.DAGs, "dags-data")
		if init {
			ss.Spec.Template.Spec.InitContainers = append(ss.Spec.Template.Spec.InitContainers, dc)
		} else {
			ss.Spec.Template.Spec.Containers = append(ss.Spec.Template.Spec.Containers, dc)
		}
	}
}

func addMySQLUserDBContainer(r *alpha1.AirflowCluster, ss *appsv1.StatefulSet) {
	sqlRootSecret := common.RsrcName(r.Spec.AirflowBaseRef.Name, common.ValueAirflowComponentSQL, "")
	sqlSvcName := common.RsrcName(r.Spec.AirflowBaseRef.Name, common.ValueAirflowComponentSQL, "")
	sqlSecret := common.RsrcName(r.Name, common.ValueAirflowComponentUI, "")
	env := []corev1.EnvVar{
		{Name: "SQL_ROOT_PASSWORD", ValueFrom: envFromSecret(sqlRootSecret, "rootpassword")},
		{Name: "SQL_DB", Value: r.Spec.Scheduler.DBName},
		{Name: "SQL_USER", Value: r.Spec.Scheduler.DBUser},
		{Name: "SQL_PASSWORD", ValueFrom: envFromSecret(sqlSecret, "password")},
		{Name: "SQL_HOST", Value: sqlSvcName},
		{Name: "DB_TYPE", Value: "mysql"},
	}
	containers := []corev1.Container{
		{
			Name:    "mysql-dbcreate",
			Image:   alpha1.DefaultMySQLImage + ":" + alpha1.DefaultMySQLVersion,
			Env:     env,
			Command: []string{"/bin/bash"},
			//SET GLOBAL explicit_defaults_for_timestamp=ON;
			Args: []string{"-c", `
mysql -uroot -h$(SQL_HOST) -p$(SQL_ROOT_PASSWORD) << EOSQL
CREATE DATABASE IF NOT EXISTS $(SQL_DB);
USE $(SQL_DB);
CREATE USER IF NOT EXISTS '$(SQL_USER)'@'%' IDENTIFIED BY '$(SQL_PASSWORD)';
GRANT ALL ON $(SQL_DB).* TO '$(SQL_USER)'@'%' ;
FLUSH PRIVILEGES;
SHOW GRANTS FOR $(SQL_USER);
EOSQL
`},
		},
	}
	ss.Spec.Template.Spec.InitContainers = append(containers, ss.Spec.Template.Spec.InitContainers...)
}

func addPostgresUserDBContainer(r *alpha1.AirflowCluster, ss *appsv1.StatefulSet) {
	sqlRootSecret := common.RsrcName(r.Spec.AirflowBaseRef.Name, common.ValueAirflowComponentSQL, "")
	sqlSvcName := common.RsrcName(r.Spec.AirflowBaseRef.Name, common.ValueAirflowComponentSQL, "")
	sqlSecret := common.RsrcName(r.Name, common.ValueAirflowComponentUI, "")
	env := []corev1.EnvVar{
		{Name: "SQL_ROOT_PASSWORD", ValueFrom: envFromSecret(sqlRootSecret, "rootpassword")},
		{Name: "SQL_DB", Value: r.Spec.Scheduler.DBName},
		{Name: "SQL_USER", Value: r.Spec.Scheduler.DBUser},
		{Name: "SQL_PASSWORD", ValueFrom: envFromSecret(sqlSecret, "password")},
		{Name: "SQL_HOST", Value: sqlSvcName},
		{Name: "DB_TYPE", Value: "postgres"},
	}
	containers := []corev1.Container{
		{
			Name:    "postgres-dbcreate",
			Image:   alpha1.DefaultPostgresImage + ":" + alpha1.DefaultPostgresVersion,
			Env:     env,
			Command: []string{"/bin/bash"},
			Args: []string{"-c", `
PGPASSWORD=$(SQL_ROOT_PASSWORD) psql -h $SQL_HOST -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = '$(SQL_DB)'" | grep -q 1 || (PGPASSWORD=$(SQL_ROOT_PASSWORD) psql -h $SQL_HOST -U postgres -c "CREATE DATABASE $(SQL_DB)" &&
PGPASSWORD=$(SQL_ROOT_PASSWORD) psql -h $SQL_HOST -U postgres -c "CREATE USER $(SQL_USER) WITH ENCRYPTED PASSWORD '$(SQL_PASSWORD)'; GRANT ALL PRIVILEGES ON DATABASE $(SQL_DB) TO $(SQL_USER)")
`},
		},
	}
	ss.Spec.Template.Spec.InitContainers = append(containers, ss.Spec.Template.Spec.InitContainers...)
}

func dependantResources(i interface{}) []reconciler.Object {
	r := i.(*alpha1.AirflowCluster)
	rsrc := []reconciler.Object{}
	rsrc = append(rsrc, k8s.ReferredItem(&alpha1.AirflowBase{}, r.Spec.AirflowBaseRef.Name, r.Namespace))
	return rsrc
}

func getAirflowPrometheusEnv(r *alpha1.AirflowCluster, base *alpha1.AirflowBase) []corev1.EnvVar {
	sqlSvcName := common.RsrcName(r.Spec.AirflowBaseRef.Name, common.ValueAirflowComponentSQL, "")
	sqlSecret := common.RsrcName(r.Name, common.ValueAirflowComponentUI, "")
	ap := "AIRFLOW_PROMETHEUS_"
	apd := ap + "DATABASE_"
	backend := "mysql"
	port := "3306"
	if IsPostgres(&base.Spec) {
		backend = "postgres"
		port = "5432"
	}
	env := []corev1.EnvVar{
		{Name: ap + "LISTEN_ADDR", Value: ":9112"},
		{Name: apd + "BACKEND", Value: backend},
		{Name: apd + "HOST", Value: sqlSvcName},
		{Name: apd + "PORT", Value: port},
		{Name: apd + "USER", Value: r.Spec.Scheduler.DBUser},
		{Name: apd + "PASSWORD", ValueFrom: envFromSecret(sqlSecret, "password")},
		{Name: apd + "NAME", Value: r.Spec.Scheduler.DBName},
	}
	return env
}

func getAirflowEnv(r *alpha1.AirflowCluster, saName string, base *alpha1.AirflowBase) []corev1.EnvVar {
	sp := r.Spec
	sqlSvcName := common.RsrcName(sp.AirflowBaseRef.Name, common.ValueAirflowComponentSQL, "")
	sqlSecret := common.RsrcName(r.Name, common.ValueAirflowComponentUI, "")
	schedulerConfigmap := common.RsrcName(r.Name, common.ValueAirflowComponentScheduler, "")
	redisSecret := ""
	redisSvcName := ""
	if sp.MemoryStore == nil {
		redisSecret = common.RsrcName(r.Name, common.ValueAirflowComponentRedis, "")
		redisSvcName = redisSecret
	}
	dagFolder := airflowDagsBase
	if sp.DAGs != nil {
		if sp.DAGs.Git != nil {
			dagFolder = airflowDagsBase + gitSyncDestDir + "/" + sp.DAGs.DagSubdir
		} else if sp.DAGs.GCS != nil {
			dagFolder = airflowDagsBase + gCSSyncDestDir + "/" + sp.DAGs.DagSubdir
		}
	}
	dbType := "mysql"
	if IsPostgres(&base.Spec) {
		dbType = "postgres"
	}
	env := []corev1.EnvVar{
		{Name: "EXECUTOR", Value: sp.Executor},
		{Name: "SQL_PASSWORD", ValueFrom: envFromSecret(sqlSecret, "password")},
		{Name: afc + "DAGS_FOLDER", Value: dagFolder},
		{Name: "SQL_HOST", Value: sqlSvcName},
		{Name: "SQL_USER", Value: sp.Scheduler.DBUser},
		{Name: "SQL_DB", Value: sp.Scheduler.DBName},
		{Name: "DB_TYPE", Value: dbType},
	}
	if sp.Executor == alpha1.ExecutorK8s {
		env = append(env, []corev1.EnvVar{
			{Name: afk + "AIRFLOW_CONFIGMAP", Value: schedulerConfigmap},
			{Name: afk + "WORKER_CONTAINER_REPOSITORY", Value: sp.Worker.Image},
			{Name: afk + "WORKER_CONTAINER_TAG", Value: sp.Worker.Version},
			{Name: afk + "WORKER_CONTAINER_IMAGE_PULL_POLICY", Value: "IfNotPresent"},
			{Name: afk + "DELETE_WORKER_PODS", Value: "True"},
			{Name: afk + "NAMESPACE", Value: r.Namespace},
			//{Name: afk+"IMAGE_PULL_SECRETS", Value: s.ImagePullSecrets},
			//{Name: afk+"GCP_SERVICE_ACCOUNT_KEYS", Vaslue:  ??},
		}...)
		if sp.DAGs != nil && sp.DAGs.Git != nil {
			env = append(env, []corev1.EnvVar{
				{Name: afk + "GIT_REPO", Value: sp.DAGs.Git.Repo},
				{Name: afk + "GIT_BRANCH", Value: sp.DAGs.Git.Branch},
				{Name: afk + "GIT_SUBPATH", Value: sp.DAGs.DagSubdir},
				{Name: afk + "GIT_SYNC_DEST", Value: gitSyncDestDir},
				{Name: afk + "WORKER_SERVICE_ACCOUNT_NAME", Value: saName},
				{Name: afk + "GIT_DAGS_FOLDER_MOUNT_POINT", Value: airflowDagsBase},
				// git_sync_root = /git
				// git_sync_dest = repo
			}...)
			if sp.DAGs.Git.CredSecretRef != nil {
				env = append(env, []corev1.EnvVar{
					{Name: "GIT_PASSWORD",
						ValueFrom: envFromSecret(sp.DAGs.Git.CredSecretRef.Name, "password")},
					{Name: "GIT_USER", Value: sp.DAGs.Git.User},
				}...)
			}
		}
		// dags_in_image = False
		// dags_volume_subpath =
		// dags_volume_claim =
	}
	if sp.Executor == alpha1.ExecutorCelery {
		if sp.MemoryStore != nil {
			env = append(env,
				[]corev1.EnvVar{
					{Name: "REDIS_HOST", Value: sp.MemoryStore.Status.Host},
					{Name: "REDIS_PORT", Value: strconv.FormatInt(sp.MemoryStore.Status.Port, 10)},
				}...)
		} else if r.Spec.Redis.RedisHost == "" {
			env = append(env,
				[]corev1.EnvVar{
					{Name: "REDIS_PASSWORD",
						ValueFrom: envFromSecret(redisSecret, "password")},
					{Name: "REDIS_HOST", Value: redisSvcName},
				}...)
		} else {
			env = append(env,
				[]corev1.EnvVar{
					{Name: "REDIS_HOST", Value: r.Spec.Redis.RedisHost},
					{Name: "REDIS_PORT", Value: r.Spec.Redis.RedisPort},
				}...)
			if r.Spec.Redis.RedisPassword == true {
				env = append(env,
					[]corev1.EnvVar{
						{Name: "REDIS_PASSWORD",
							ValueFrom: envFromSecret(redisSecret, "password")},
					}...)
			}
		}
	}

	// Do sorted key scan. To store the keys in slice in sorted order
	var keys []string
	for k := range sp.Config.AirflowEnv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		env = append(env, corev1.EnvVar{Name: k, Value: sp.Config.AirflowEnv[k]})
	}

	for _, k := range sp.Config.AirflowSecretEnv {
		env = append(env, corev1.EnvVar{Name: k.Env, ValueFrom: envFromSecret(k.Secret, k.Field)})
	}

	return env
}

// --------------- Global Cluster component -------------------------

// Observables asd
func (c *Cluster) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&app.ApplicationList{}).
		Get()
}

// DependentResources - return dependant resources
func (c *Cluster) DependentResources(rsrc interface{}) []reconciler.Object {
	return dependantResources(rsrc)
}

// Objects returns the list of resource/name for those resources created by
// the operator for this spec and those resources referenced by this operator.
// Mark resources as owned, referred
func (c *Cluster) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowCluster)

	selectors := make(map[string]string)
	for k, v := range rsrclabels {
		selectors[k] = v
	}
	delete(selectors, gr.LabelUsing)

	ngdata := templateValue(r, dependent, common.ValueAirflowComponentCluster, rsrclabels, selectors, nil)
	ngdata.Expected = aggregated

	return k8s.NewObjects().
		WithValue(ngdata).
		WithTemplate("cluster-application.yaml", &app.ApplicationList{},
			func(o *reconciler.Object, v interface{}) {
				ao := application.NewApplication(o.Obj.(*k8s.Object).Obj)
				o = ao.SetSelector(r.Labels).
					SetComponentGK(aggregated).
					Item()
			}).
		Build()
}

// UpdateStatus use reconciled objects to update component status
func (c *Cluster) UpdateStatus(rsrc interface{}, reconciled []reconciler.Object, err error) time.Duration {
	var period time.Duration
	stts := &rsrc.(*alpha1.AirflowCluster).Status
	ready := stts.ComponentMeta.UpdateStatus(reconciler.ObjectsByType(reconciled, k8s.Type))
	stts.Meta.UpdateStatus(&ready, err)
	return period
}

// ------------------------------ Airflow UI -----------------------------------

// Observables asd
func (s *UI) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&appsv1.StatefulSetList{}).
		For(&corev1.SecretList{}).
		Get()
}

// DependentResources - return dependant resources
func (s *UI) DependentResources(rsrc interface{}) []reconciler.Object {
	return dependantResources(rsrc)
}

// Objects returns the list of resource/name for those resources created by
func (s *UI) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowCluster)
	if r.Spec.UI == nil {
		return []reconciler.Object{}, nil
	}

	if r.Spec.MemoryStore != nil && r.Spec.MemoryStore.Status.Host == "" {
		return []reconciler.Object{}, nil
	}

	ngdata := templateValue(r, dependent, common.ValueAirflowComponentUI, rsrclabels, rsrclabels, map[string]string{"web": "8080"})
	ngdata.Secret = map[string]string{
		"password": base64.StdEncoding.EncodeToString(common.RandomAlphanumericString(16)),
	}

	return k8s.NewObjects().
		WithValue(ngdata).
		WithTemplate("ui-sts.yaml", &appsv1.StatefulSetList{}, s.sts).
		WithTemplate("secret.yaml", &corev1.SecretList{}, reconciler.NoUpdate).
		Build()
}

func (s *UI) sts(o *reconciler.Object, v interface{}) {
	sts, r := updateSts(o, v)
	sts.Spec.Template.Spec.Containers[0].Resources = r.Cluster.Spec.UI.Resources
	if IsPostgres(&r.Base.Spec) {
		addPostgresUserDBContainer(r.Cluster, sts)
	} else {
		addMySQLUserDBContainer(r.Cluster, sts)
	}
}

// ------------------------------ RedisSpec ---------------------------------------

func (s Redis) sts(o *reconciler.Object, v interface{}) {
	r := v.(*common.TemplateValue)
	sts := o.Obj.(*k8s.Object).Obj.(*appsv1.StatefulSet)
	sts.Spec.Template.Spec.Containers[0].Resources = r.Cluster.Spec.Redis.Resources
	if r.Cluster.Spec.Redis.VolumeClaimTemplate != nil {
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{*r.Cluster.Spec.Redis.VolumeClaimTemplate}
	}
}

// Observables asd
func (s *Redis) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&appsv1.StatefulSetList{}).
		For(&corev1.SecretList{}).
		For(&policyv1.PodDisruptionBudgetList{}).
		For(&corev1.ServiceList{}).
		Get()
}

// DependentResources - return dependant resources
func (s *Redis) DependentResources(rsrc interface{}) []reconciler.Object {
	return dependantResources(rsrc)
}

// Objects returns the list of resource/name for those resources created by
// the operator for this spec and those resources referenced by this operator.
// Mark resources as owned, referred
func (s *Redis) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowCluster)
	if r.Spec.Redis == nil || r.Spec.Redis.RedisHost != "" {
		return []reconciler.Object{}, nil
	}
	ngdata := templateValue(r, dependent, common.ValueAirflowComponentRedis, rsrclabels, rsrclabels, map[string]string{"redis": "6379"})
	ngdata.Secret = map[string]string{
		"password": base64.StdEncoding.EncodeToString(common.RandomAlphanumericString(16)),
	}
	ngdata.PDBMinAvail = "100%"

	return k8s.NewObjects().
		WithValue(ngdata).
		WithTemplate("redis-sts.yaml", &appsv1.StatefulSetList{}, s.sts).
		WithTemplate("secret.yaml", &corev1.SecretList{}, reconciler.NoUpdate).
		WithTemplate("pdb.yaml", &policyv1.PodDisruptionBudgetList{}).
		WithTemplate("svc.yaml", &corev1.ServiceList{}).
		Build()
}

// ------------------------------ Scheduler ---------------------------------------

func gcsContainer(s *alpha1.GCSSpec, volName string) (bool, corev1.Container) {
	init := false
	container := corev1.Container{}
	env := []corev1.EnvVar{
		{Name: "GCS_BUCKET", Value: s.Bucket},
	}
	if s.Once {
		init = true
	}
	container = corev1.Container{
		Name:  "gcs-syncd",
		Image: alpha1.GCSsyncImage + ":" + alpha1.GCSsyncVersion,
		Env:   env,
		Args:  []string{"/home/airflow/gcs"},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      volName,
				MountPath: "/home/airflow/gcs",
			},
		},
	}

	return init, container
}

func gitContainer(s *alpha1.GitSpec, volName string) (bool, corev1.Container) {
	init := false
	container := corev1.Container{}
	env := []corev1.EnvVar{
		{Name: "GIT_SYNC_REPO", Value: s.Repo},
		{Name: "GIT_SYNC_DEST", Value: gitSyncDestDir},
		{Name: "GIT_SYNC_BRANCH", Value: s.Branch},
		{Name: "GIT_SYNC_ONE_TIME", Value: strconv.FormatBool(s.Once)},
		{Name: "GIT_SYNC_REV", Value: s.Rev},
	}
	if s.CredSecretRef != nil {
		env = append(env, []corev1.EnvVar{
			{Name: "GIT_SYNC_PASSWORD",
				ValueFrom: envFromSecret(s.CredSecretRef.Name, "password")},
			{Name: "GIT_SYNC_USERNAME", Value: s.User},
		}...)
	}
	if s.Once {
		init = true
	}
	container = corev1.Container{
		Name:    "git-sync",
		Image:   alpha1.GitsyncImage + ":" + alpha1.GitsyncVersion,
		Env:     env,
		Command: []string{"/git-sync"},
		Ports: []corev1.ContainerPort{
			{
				Name:          "gitsync",
				ContainerPort: 2020,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      volName,
				MountPath: "/tmp/git",
			},
		},
	}

	return init, container
}

func dagContainer(s *alpha1.DagSpec, volName string) (bool, corev1.Container) {
	init := false
	container := corev1.Container{}

	if s.Git != nil {
		return gitContainer(s.Git, volName)
	}
	if s.GCS != nil {
		return gcsContainer(s.GCS, volName)
	}

	return init, container
}

func (s *Scheduler) sts(o *reconciler.Object, v interface{}) {
	sts, r := updateSts(o, v)
	if r.Cluster.Spec.Executor == alpha1.ExecutorK8s {
		sts.Spec.Template.Spec.ServiceAccountName = sts.Name
	}
	sts.Spec.Template.Spec.Containers[0].Resources = r.Cluster.Spec.Scheduler.Resources
	sts.Spec.Template.Spec.Containers[1].Env = getAirflowPrometheusEnv(r.Cluster, r.Base)
}

// DependentResources - return dependant resources
func (s *Scheduler) DependentResources(rsrc interface{}) []reconciler.Object {
	r := rsrc.(*alpha1.AirflowCluster)
	resources := dependantResources(rsrc)
	if r.Spec.Executor == alpha1.ExecutorK8s {
		sqlSecret := common.RsrcName(r.Name, common.ValueAirflowComponentUI, "")
		resources = append(resources, k8s.ReferredItem(&corev1.Secret{}, sqlSecret, r.Namespace))
	}
	return resources
}

// Observables - get
func (s *Scheduler) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&appsv1.StatefulSetList{}).
		For(&corev1.ConfigMapList{}).
		For(&corev1.ServiceAccountList{}).
		For(&rbacv1.RoleBindingList{}).
		Get()
}

// Objects returns the list of resource/name for those resources created by
// the operator for this spec and those resources referenced by this operator.
// Mark resources as owned, referred
func (s *Scheduler) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowCluster)
	if r.Spec.Scheduler == nil {
		return []reconciler.Object{}, nil
	}

	if r.Spec.MemoryStore != nil && r.Spec.MemoryStore.Status.Host == "" {
		return []reconciler.Object{}, nil
	}

	b := k8s.GetItem(dependent, &alpha1.AirflowBase{}, r.Spec.AirflowBaseRef.Name, r.Namespace)
	base := b.(*alpha1.AirflowBase)
	bag := k8s.NewObjects()
	if r.Spec.DAGs != nil {
		git := r.Spec.DAGs.Git
		if git != nil && git.CredSecretRef != nil {
			bag.WithReferredItem(&corev1.Secret{}, git.CredSecretRef.Name, r.Namespace)
		}
	}

	ngdata := templateValue(r, dependent, common.ValueAirflowComponentScheduler, rsrclabels, rsrclabels, nil)
	bag.WithValue(ngdata).WithFolder("templates/")

	if r.Spec.Executor == alpha1.ExecutorK8s {
		sqlSvcName := common.RsrcName(r.Spec.AirflowBaseRef.Name, common.ValueAirflowComponentSQL, "")
		sqlSecret := common.RsrcName(r.Name, common.ValueAirflowComponentUI, "")
		se := k8s.GetItem(dependent, &corev1.Secret{}, sqlSecret, r.Namespace)
		secret := se.(*corev1.Secret)

		dbPrefix := "mysql"
		port := "3306"
		if base.Spec.Postgres != nil {
			dbPrefix = "postgresql+psycopg2"
			port = "5432"
		}
		conn := dbPrefix + "://" + r.Spec.Scheduler.DBUser + ":" + string(secret.Data["password"]) + "@" + sqlSvcName + ":" + port + "/" + r.Spec.Scheduler.DBName

		ngdata.SQLConn = conn
		bag.WithTemplate("airflow-configmap.yaml", &corev1.ConfigMapList{})
	}

	return bag.WithTemplate("scheduler-sts.yaml", &appsv1.StatefulSetList{}, s.sts).
		WithTemplate("serviceaccount.yaml", &corev1.ServiceAccountList{}, reconciler.NoUpdate).
		WithTemplate("rolebinding.yaml", &rbacv1.RoleBindingList{}).
		Build()
}

// ------------------------------ Worker ----------------------------------------

func (s *Worker) sts(o *reconciler.Object, v interface{}) {
	sts, r := updateSts(o, v)
	sts.Spec.Template.Spec.Containers[0].Resources = r.Cluster.Spec.Worker.Resources
}

// Observables asd
func (s *Worker) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&appsv1.StatefulSetList{}).
		For(&corev1.ServiceList{}).
		Get()
}

// DependentResources - return dependant resources
func (s *Worker) DependentResources(rsrc interface{}) []reconciler.Object {
	return dependantResources(rsrc)
}

// Objects returns the list of resource/name for those resources created by
// the operator for this spec and those resources referenced by this operator.
// Mark resources as owned, referred
func (s *Worker) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowCluster)
	if r.Spec.Worker == nil {
		return []reconciler.Object{}, nil
	}

	if r.Spec.MemoryStore != nil && r.Spec.MemoryStore.Status.Host == "" {
		return []reconciler.Object{}, nil
	}

	ngdata := templateValue(r, dependent, common.ValueAirflowComponentWorker, rsrclabels, rsrclabels, map[string]string{"wlog": "8793"})

	return k8s.NewObjects().
		WithValue(ngdata).
		WithTemplate("worker-sts.yaml", &appsv1.StatefulSetList{}, s.sts).
		WithTemplate("headlesssvc.yaml", &corev1.ServiceList{}).
		Build()
}

// ------------------------------ Flower ---------------------------------------

// Observables asd
func (s *Flower) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return k8s.NewObservables().
		WithLabels(labels).
		For(&appsv1.StatefulSetList{}).
		Get()
}

// DependentResources - return dependant resources
func (s *Flower) DependentResources(rsrc interface{}) []reconciler.Object {
	return dependantResources(rsrc)
}

// Objects returns the list of resource/name for those resources created by
func (s *Flower) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowCluster)
	if r.Spec.Flower == nil {
		return []reconciler.Object{}, nil
	}
	if r.Spec.MemoryStore != nil && r.Spec.MemoryStore.Status.Host == "" {
		return []reconciler.Object{}, nil
	}
	ngdata := templateValue(r, dependent, common.ValueAirflowComponentFlower, rsrclabels, rsrclabels, map[string]string{"flower": "5555"})

	return k8s.NewObjects().
		WithValue(ngdata).
		WithTemplate("flower-sts.yaml", &appsv1.StatefulSetList{}, s.sts).
		Build()
}

func (s *Flower) sts(o *reconciler.Object, v interface{}) {
	sts, r := updateSts(o, v)
	sts.Spec.Template.Spec.Containers[0].Resources = r.Cluster.Spec.Flower.Resources
}

// ------------------------------ MemoryStore ---------------------------------------

// DependentResources - return dependant resources
func (s *MemoryStore) DependentResources(rsrc interface{}) []reconciler.Object {
	return dependantResources(rsrc)
}

// Observables for memstore
func (s *MemoryStore) Observables(rsrc interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	r := rsrc.(*alpha1.AirflowCluster)
	if r.Spec.MemoryStore == nil {
		return []reconciler.Observable{}
	}
	parent, err := redis.GetParent(r.Spec.MemoryStore.Project, r.Spec.MemoryStore.Region)
	if err != nil {
		return []reconciler.Observable{}
		// TODO assert()
	}
	return []reconciler.Observable{redis.NewObservable(labels, parent)}
}

// Objects - returns resources
func (s *MemoryStore) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error) {
	r := rsrc.(*alpha1.AirflowCluster)
	if r.Spec.MemoryStore == nil {
		return []reconciler.Object{}, nil
	}
	parent, err := redis.GetParent(r.Spec.MemoryStore.Project, r.Spec.MemoryStore.Region)
	if err != nil {
		return []reconciler.Object{}, err
	}
	bag, err := gcp.NewObjects().
		WithLabels(rsrclabels).
		Add(redis.NewObject(parent, r.Name+"-redis")).
		Build()

	if err != nil {
		return []reconciler.Object{}, err
	}
	robj := bag[0].Obj.(*redis.Object).Redis
	robj.AlternativeLocationId = r.Spec.MemoryStore.AlternativeLocationID
	robj.AuthorizedNetwork = r.Spec.MemoryStore.AuthorizedNetwork
	robj.DisplayName = r.Name + "-redis"

	if r.Spec.MemoryStore.NotifyKeyspaceEvents != "" {
		if robj.RedisConfigs == nil {
			robj.RedisConfigs = make(map[string]string)
		}
		robj.RedisConfigs["notify-keyspace-events"] = r.Spec.MemoryStore.NotifyKeyspaceEvents
	}

	if r.Spec.MemoryStore.MaxMemoryPolicy != "" {
		if robj.RedisConfigs == nil {
			robj.RedisConfigs = make(map[string]string)
		}
		robj.RedisConfigs["maxmemory-policy"] = r.Spec.MemoryStore.MaxMemoryPolicy
	}

	robj.RedisVersion = r.Spec.MemoryStore.RedisVersion
	robj.MemorySizeGb = int64(r.Spec.MemoryStore.MemorySizeGb)
	robj.Tier = strings.ToUpper(r.Spec.MemoryStore.Tier)

	return bag, nil
}

// UpdateStatus - update status block
func (s *MemoryStore) UpdateStatus(rsrc interface{}, reconciled []reconciler.Object, err error) time.Duration {
	var period time.Duration
	r := rsrc.(*alpha1.AirflowCluster)
	if r.Spec.MemoryStore == nil {
		return period
	}
	stts := &r.Spec.MemoryStore.Status
	ready := false
	if len(reconciled) != 0 {
		instance := reconciled[0].Obj.(*redis.Object).Redis
		stts.CreateTime = instance.CreateTime
		stts.CurrentLocationID = instance.CurrentLocationId
		stts.Host = instance.Host
		stts.Port = instance.Port
		stts.State = instance.State
		if instance.State != "READY" && instance.State != "MAINTENANCE" {
			period = time.Second * 30
		}
		stts.StatusMessage = instance.StatusMessage
		ready = true
		stts.Meta.UpdateStatus(&ready, err)
	} else {
		period = time.Second * 30
		stts.Meta.UpdateStatus(&ready, err)
	}
	return period
}

// Differs returns true if the resource needs to be updated
func (s *MemoryStore) Differs(expected reconciler.Object, observed reconciler.Object) bool {
	return true //differs(expected, observed)
}

// Finalize - finalizes MemoryStore component when it is deleted
func (s *MemoryStore) Finalize(rsrc interface{}, observed, dependent []reconciler.Object) error {
	r := rsrc.(*alpha1.AirflowCluster)
	if r.Spec.MemoryStore == nil {
		return nil
	}
	obj := r.Spec.MemoryStore
	obj.Status.NotReady("Finalizing", "Finalizing in progress")
	if len(observed) != 0 {
		finalizer.Add(r, finalizer.Cleanup)
		items := observed
		for i := range items {
			items[i].Delete = true
		}
		obj.Status.SetCondition(status.Cleanup, "InProgress", "Items pending deletion")
	} else {
		finalizer.Remove(r, finalizer.Cleanup)
	}
	return nil
}
