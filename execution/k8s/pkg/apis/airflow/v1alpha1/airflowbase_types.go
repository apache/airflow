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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-reconciler/pkg/finalizer"
	"sigs.k8s.io/controller-reconciler/pkg/status"
)

// defaults and constant strings
const (
	DefaultMySQLImage      = "mysql"
	DefaultMySQLVersion    = "5.7"
	DefaultPostgresImage   = "postgres"
	DefaultPostgresVersion = "9.5"
	defaultUIImage         = "gcr.io/airflow-operator/airflow"
	defaultUIVersion       = "1.10.2"
	defaultFlowerVersion   = "1.10.2"
	defaultNFSVersion      = "0.8"
	defaultNFSImage        = "k8s.gcr.io/volume-nfs"
	defaultSQLProxyImage   = "gcr.io/cloud-airflow-public/airflow-sqlproxy"
	defaultSQLProxyVersion = "1.8.0"
	defaultSchedule        = "0 0 0 ? * * *`" // daily@midnight
	defaultDBReplicas      = 1
	defaultOperator        = false
	defaultStorageProvider = "s3"
	providerS3             = "s3"
	StatusReady            = "Ready"
	StatusInProgress       = "InProgress"
	StatusDisabled         = "Disabled"
	DatabaseMySQL          = "MySQL"
	DatabasePostgres       = "Postgres"
	DatabaseSQLProxy       = "SQLProxy"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AirflowBase represents the components required for an Airflow scheduler and worker to
// function. At a minimum they need a SQL service (MySQL or SQLProxy) and Airflow UI.
// In addition for an installation with minimal external dependencies, NFS and Airflow UI
// are also added.
// +k8s:openapi-gen=true
// +kubebuilder:resource:path=airflowbases
type AirflowBase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AirflowBaseSpec   `json:"spec,omitempty"`
	Status AirflowBaseStatus `json:"status,omitempty"`
}

// AirflowBaseStatus defines the observed state of AirflowBase
type AirflowBaseStatus struct {
	status.Meta          `json:",inline"`
	status.ComponentMeta `json:",inline"`
}

// AirflowBaseSpec defines the desired state of AirflowBase
type AirflowBaseSpec struct {
	// Selector for fitting pods to nodes whose labels match the selector.
	// https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Define scheduling constraints for pods.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// Custom annotations to be added to the pods.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// Custom labels to be added to the pods.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Spec for MySQL component.
	// +optional
	MySQL    *MySQLSpec    `json:"mysql,omitempty"`
	SQLProxy *SQLProxySpec `json:"sqlproxy,omitempty"`
	Postgres *PostgresSpec `json:"postgres,omitempty"`
	// Spec for NFS component.
	// +optional
	Storage *NFSStoreSpec `json:"storage,omitempty"`
}

func (s *AirflowBaseSpec) validate(fp *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if s == nil {
		return errs
	}
	if s.MySQL == nil && s.SQLProxy == nil && s.Postgres == nil {
		errs = append(errs, field.Required(fp.Child("database"), "Either MySQL or SQLProxy is required"))
	}
	return errs
}

// PostgresSpec defines the attributes and desired state of Postgres Component
// TODO - minimum spec needed .. for now it is version: ""
// need to consider empty mysql
type PostgresSpec struct {
	// Image defines the Postgres Docker image name
	// +optional
	Image string `json:"image,omitempty"`
	// Version defines the Postgres Docker image version
	// +optional
	Version string `json:"version,omitempty"`
	// Replicas defines the number of running Postgres instances in a cluster
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// VolumeClaimTemplate allows a user to specify volume claim for Postgres Server files
	// +optional
	VolumeClaimTemplate *corev1.PersistentVolumeClaim `json:"volumeClaimTemplate,omitempty"`
	// Flag when True generates PostgresOperator CustomResource to be handled by Postgres Operator
	// If False, a StatefulSet with 1 replica is created (not for production setups)
	// +optional
	Operator bool `json:"operator,omitempty"`
	// Resources is the resource requests and limits for the pods.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// Options command line options for mysql
	Options map[string]string
}

func (s *PostgresSpec) validate(fp *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if s == nil {
		return errs
	}
	if s.Operator == true {
		errs = append(errs, field.Invalid(fp.Child("operator"), "", "Operator is not supported in this version"))
	}

	return errs
}

// MySQLSpec defines the attributes and desired state of MySQL Component
// TODO - minimum spec needed .. for now it is version: ""
// need to consider empty mysql
type MySQLSpec struct {
	// Image defines the MySQL Docker image name
	// +optional
	Image string `json:"image,omitempty"`
	// Version defines the MySQL Docker image version
	// +optional
	Version string `json:"version,omitempty"`
	// Replicas defines the number of running MySQL instances in a cluster
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// VolumeClaimTemplate allows a user to specify volume claim for MySQL Server files
	// +optional
	VolumeClaimTemplate *corev1.PersistentVolumeClaim `json:"volumeClaimTemplate,omitempty"`
	// BackupVolumeClaimTemplate allows a user to specify a volume to temporarily store the
	// data for a backup prior to it being shipped to object storage.
	// +optional
	BackupVolumeClaimTemplate *corev1.PersistentVolumeClaim `json:"backupVolumeClaimTemplate,omitempty"`
	// Flag when True generates MySQLOperator CustomResource to be handled by MySQL Operator
	// If False, a StatefulSet with 1 replica is created (not for production setups)
	// +optional
	Operator bool `json:"operator,omitempty"`
	// Spec defining the Backup Custom Resource to be handled by MySQLOperator
	// Ignored when Operator is False
	// +optional
	Backup *MySQLBackup `json:"backup,omitempty"`
	// Resources is the resource requests and limits for the pods.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// Options command line options for mysql
	Options map[string]string
}

func (s *MySQLSpec) validate(fp *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if s == nil {
		return errs
	}
	if s.Operator == true {
		errs = append(errs, field.Invalid(fp.Child("operator"), "", "Operator is not supported in this version"))
	}
	if s.Backup != nil {
		errs = append(errs, field.Invalid(fp.Child("backup"), "", "Backup is not supported in this version"))
	}

	errs = append(errs, s.Backup.validate(fp.Child("backup"))...)
	return errs
}

// MySQLBackup defines the Backup Custom Resource which is handled by MySQLOperator
type MySQLBackup struct {
	// Schedule is the cron string used to schedule backup
	Schedule string `json:"schedule"`
	// Storage has the s3 compatible storage spec
	Storage StorageSpec `json:"storage"`
}

func (s *MySQLBackup) validate(fp *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if s == nil {
		return errs
	}
	if !validCronString(s.Schedule) {
		errs = append(errs,
			field.Invalid(fp.Child("schedule"),
				s.Schedule,
				"Invalid Schedule cron string"))
	}

	errs = append(errs, s.Storage.validate(fp.Child("storage"))...)

	return errs
}

func validCronString(cron string) bool {
	// TODO : Check cron string
	return true
}

// StorageSpec describes the s3 compatible storage
type StorageSpec struct {
	// Provider is the storage type used for backup and restore
	// e.g. s3, oci-s3-compat, aws-s3, gce-s3, etc.
	StorageProvider string `json:"storageprovider"`
	// SecretRef is a reference to the Kubernetes secret containing the configuration for uploading
	// the backup to authenticated storage.
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
	// Config is generic string based key-value map that defines non-secret configuration values for
	// uploading the backup to storage w.r.t the configured storage provider.
	Config map[string]string `json:"config,omitempty"`
}

func (s *StorageSpec) validate(fp *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if !validStorageProvider(s.StorageProvider) {
		errs = append(errs,
			field.Invalid(fp.Child("storageprovider"),
				s.StorageProvider,
				"Invalid Storage Provider"))
	}
	if s.SecretRef == nil {
		errs = append(errs, field.Required(fp.Child("secretRef"), ""))
	} else if s.SecretRef.Name == "" {
		errs = append(errs, field.Required(fp.Child("secretRef", "name"), ""))
	}

	config := fp.Child("config")
	if s.Config == nil {
		errs = append(errs, field.Required(config, ""))
		return errs
	}

	if s.Config["endpoint"] == "" {
		errs = append(errs, field.Required(config.Key("endpoint"), "no storage config 'endpoint'"))
	}

	if s.Config["region"] == "" {
		errs = append(errs, field.Required(config.Key("region"), "no storage config 'region'"))
	}

	if s.Config["bucket"] == "" {
		errs = append(errs, field.Required(config.Key("bucket"), "no storage config 'bucket'"))
	}

	return errs
}

func validStorageProvider(provider string) bool {
	switch provider {
	case providerS3:
		return true
	}
	return false
}

// AirflowUISpec defines the attributes to deploy Airflow UI component
type AirflowUISpec struct {
	// Image defines the AirflowUI Docker image.
	// +optional
	Image string `json:"image,omitempty"`
	// Version defines the AirflowUI Docker image version.
	// +optional
	Version string `json:"version,omitempty"`
	// Replicas defines the number of running Airflow UI instances in a cluster
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// Resources is the resource requests and limits for the pods.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

func (s *AirflowUISpec) validate(fp *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	//errs = append(errs, s.Resources.validate(fp.Child("resources"))...)
	return errs
}

// NFSStoreSpec defines the attributes to deploy Airflow Storage component
type NFSStoreSpec struct {
	// Image defines the NFS Docker image.
	// +optional
	Image string `json:"image,omitempty"`
	// Version defines the NFS Server Docker image version.
	// +optional
	Version string `json:"version,omitempty"`
	// Resources is the resource requests and limits for the pods.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// Volume allows a user to specify volume claim template to be used for fileserver
	// +optional
	Volume *corev1.PersistentVolumeClaim `json:"volumeClaimTemplate,omitempty"`
}

func (s *NFSStoreSpec) validate(fp *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	// TODO Volume check
	//errs = append(errs, s.Resources.validate(fp.Child("resources"))...)
	return errs
}

// SQLProxySpec defines the attributes to deploy SQL Proxy component
type SQLProxySpec struct {
	// Image defines the SQLProxy Docker image name
	// +optional
	Image string `json:"image,omitempty"`
	// Version defines the SQL Proxy docker image version.
	// +optional
	Version string `json:"version,omitempty"`
	// example: myProject:us-central1:myInstance=tcp:3306
	// Project defines the SQL instance project
	Project string `json:"project"`
	// Region defines the SQL instance region
	Region string `json:"region"`
	// Instance defines the SQL instance name
	Instance string `json:"instance"`
	// Type defines the SQL instance type
	Type string `json:"type"`
	// Resources is the resource requests and limits for the pods.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

func (s *SQLProxySpec) validate(fp *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if s == nil {
		return errs
	}
	if s.Project == "" {
		errs = append(errs, field.Required(fp.Child("project"), "Missing cloudSQL Project"))
	}
	if s.Region == "" {
		errs = append(errs, field.Required(fp.Child("region"), "Missing cloudSQL Region"))
	}
	if s.Instance == "" {
		errs = append(errs, field.Required(fp.Child("instance"), "Missing cloudSQL Instance"))
	}
	return errs
}

// Resources aggregates resource requests and limits. Note that requests, if specified, must be less
// than or equal to limits.
type Resources struct {
	// The amount of CPU, Memory, and Disk requested for pods.
	// +optional
	Requests ResourceRequests `json:"requests,omitempty"`
	// The limit of CPU and Memory that pods may use.
	// +optional
	Limits ResourceLimits `json:"limits,omitempty"`
}

func (s *Resources) validate(fp *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	return errs
}

// ResourceRequests is used to describe the resource requests for a Redis pod.
type ResourceRequests struct {
	// Cpu is the amount of CPU requested for a pod.
	// +optional
	CPU string `json:"cpu,omitempty"`
	// Memory is the amount of RAM requested for a Pod.
	// +optional
	Memory string `json:"memory,omitempty"`
	// Disk is the amount of Disk requested for a pod.
	// +optional
	Disk string `json:"disk,omitempty"`
	// DiskStorageClass is the storage class for Disk.
	// Disk must be present or this field is invalid.
	// +optional
	DiskStorageClass string `json:"diskStorageClass,omitempty"`
}

// ResourceLimits is used to describe the resources limits for a Redis pod.
// When limits are exceeded, the Pod will be terminated.
type ResourceLimits struct {
	// Cpu is the CPU limit for a pod.
	// +optional
	CPU string `json:"cpu,omitempty"`
	// Memory is the RAM limit for a pod.
	// +optional
	Memory string `json:"memory,omitempty"`
}

// Helper functions for the resources

// ApplyDefaults the AirflowBase
func (b *AirflowBase) ApplyDefaults() {
	if b.Spec.MySQL != nil {
		if b.Spec.MySQL.Replicas == 0 {
			b.Spec.MySQL.Replicas = defaultDBReplicas
		}
		if b.Spec.MySQL.Image == "" {
			b.Spec.MySQL.Image = DefaultMySQLImage
		}
		if b.Spec.MySQL.Version == "" {
			b.Spec.MySQL.Version = DefaultMySQLVersion
		}
		if b.Spec.MySQL.Backup != nil {
			if b.Spec.MySQL.Backup.Storage.StorageProvider == "" {
				b.Spec.MySQL.Backup.Storage.StorageProvider = defaultStorageProvider
			}
			if b.Spec.MySQL.Backup.Schedule == "" {
				b.Spec.MySQL.Backup.Schedule = defaultSchedule
			}
			if b.Spec.MySQL.Backup.Storage.StorageProvider == "" {
				b.Spec.MySQL.Backup.Storage.StorageProvider = defaultStorageProvider
			}
		}
	}
	if b.Spec.Postgres != nil {
		if b.Spec.Postgres.Replicas == 0 {
			b.Spec.Postgres.Replicas = defaultDBReplicas
		}
		if b.Spec.Postgres.Image == "" {
			b.Spec.Postgres.Image = DefaultPostgresImage
		}
		if b.Spec.Postgres.Version == "" {
			b.Spec.Postgres.Version = DefaultPostgresVersion
		}
	}
	if b.Spec.Storage != nil {
		if b.Spec.Storage.Image == "" {
			b.Spec.Storage.Image = defaultNFSImage
		}
		if b.Spec.Storage.Version == "" {
			b.Spec.Storage.Version = defaultNFSVersion
		}
	}
	if b.Spec.SQLProxy != nil {
		if b.Spec.SQLProxy.Image == "" {
			b.Spec.SQLProxy.Image = defaultSQLProxyImage
		}
		if b.Spec.SQLProxy.Version == "" {
			b.Spec.SQLProxy.Version = defaultSQLProxyVersion
		}
	}
	b.Status.ComponentList = status.ComponentList{}
	finalizer.EnsureStandard(b)
}

// HandleError records status or error in status
func (b *AirflowBase) HandleError(err error) {
	if err != nil {
		b.Status.SetError("ErrorSeen", err.Error())
	} else {
		b.Status.ClearError()
	}
}

// Validate the AirflowBase
func (b *AirflowBase) Validate() error {
	errs := field.ErrorList{}
	spec := field.NewPath("spec")

	errs = append(errs, b.Spec.validate(spec)...)
	errs = append(errs, b.Spec.MySQL.validate(spec.Child("mysql"))...)
	errs = append(errs, b.Spec.Storage.validate(spec.Child("storage"))...)
	errs = append(errs, b.Spec.SQLProxy.validate(spec.Child("sqlproxy"))...)

	if b.Spec.MySQL == nil && b.Spec.Postgres == nil && b.Spec.SQLProxy == nil {
		errs = append(errs, field.Required(spec, "Either MySQL or Postgres or SQLProxy is required"))
	}

	count := 0
	if b.Spec.Postgres != nil {
		count++
	}
	if b.Spec.MySQL != nil {
		count++
	}
	if b.Spec.SQLProxy != nil {
		count++
	}
	if count != 1 {
		errs = append(errs, field.Invalid(spec, "", "Only One of MySQL,Postgres,SQLProxy can be declared"))
	}

	return errs.ToAggregate()
}

// OwnerRef returns owner ref object with the component's resource as owner
func (b *AirflowBase) OwnerRef() *metav1.OwnerReference {
	return metav1.NewControllerRef(b, schema.GroupVersionKind{
		Group:   SchemeGroupVersion.Group,
		Version: SchemeGroupVersion.Version,
		Kind:    "AirflowBase",
	})
}

// NewAirflowBase return a defaults filled AirflowBase object
func NewAirflowBase(name, namespace string, database string, storage bool) *AirflowBase {
	b := AirflowBase{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"test": name,
			},
			Namespace: namespace,
		},
	}
	b.Spec = AirflowBaseSpec{}
	switch database {
	case DatabasePostgres:
		b.Spec.Postgres = &PostgresSpec{}
	case DatabaseSQLProxy:
		b.Spec.SQLProxy = &SQLProxySpec{}
	case DatabaseMySQL:
		fallthrough
	default:
		b.Spec.MySQL = &MySQLSpec{}
	}
	if storage {
		b.Spec.Storage = &NFSStoreSpec{}
	}
	b.ApplyDefaults()
	return &b
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AirflowBaseList contains a list of AirflowBase
type AirflowBaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AirflowBase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AirflowBase{}, &AirflowBaseList{})
}
