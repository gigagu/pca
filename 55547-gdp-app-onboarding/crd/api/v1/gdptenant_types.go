/*
Copyright 2025.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// GDPTenantSpec defines the desired state of GDPTenant
type GDPTenantSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// foo is an example field of GDPTenant. Edit gdptenant_types.go to remove/update
	// +optional
	Product         string   `json:"product"`
	ResilienceLevel string   `json:"resilience-level"`
	Target          []string `json:"target"`
	ItamName        string   `json:"itam-name"`
	Sbia            int      `json:"sbia"`
	Ibs             bool     `json:"ibs"`
	Lob             string   `json:"lob"`
	ItoUnit         string   `json:"ito-unit"`
	Itam            int      `json:"itam"`
	BC              string   `json:"bc"`
	Tenant          string   `json:"tenant"`
	TenantShortName string   `json:"tenant-short-name"`
	TenantDesc      string   `json:"tenant-desc"`

	TenantNoninteractiveOwner  []string       `json:"tenant-noninteractive-owner"`
	TenantNoninteractiveViewer []string       `json:"tenant-noninteractive-viewer"`
	TenantInteractiveOwner     []string       `json:"tenant-interactive-owner"`
	TenantInteractiveViewer    []string       `json:"tenant-interactive-viewer"`
	Namespace                  string         `json:"namespace"`
	HCVAccount                 string         `json:"hcv-secret-account,omitempty"`
	JobQueue                   JobQueue       `json:"jobs-queue,omitempty"`
	ResourceQuotas             ResourceQuotas `json:"resource-quotas"`
	// LimitRanges    LimitRanges    `json:"limit-ranges,omitempty"`
	ObjectStoreBuckets []ObjectStoreBuckets `json:"object-store-buckets,omitempty"`
	RangerPolicies     []RangerPolicy       `json:"ranger-policies,omitempty"`
}

type RangerPolicy struct {
	SchemaName   string   `json:"schema_name"`
	SchemaPath   string   `json:"schema_path"`
	SchemaAdmin  []string `json:"schema_admin"`
	SchemaViewer []string `json:"schema_viewer"`
}

type JobQueue struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

type ObjectStoreBuckets struct {
	Name       string `json:"name"`
	BucketSize string `json:"bucket-size"`
	Location   string `json:"location, omitempty", default:"ap-southeast-1"`
}

type ResourceQuotas struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
	// RequestsStorageLocal    string `json:"requests-storage-local"`
	// RequestsStorageNas      string `json:"requests-storage-nas"`
	// LimitsCPU               string `json:"limits-cpu"`
	// LimitsMemory            string `json:"limits-memory"`
	// Configmaps              int    `json:"configmaps"`
	// Persistancevolumeclaims int    `json:"persistancevolumeclaims"`
	// Pods                    int    `json:"pods"`
	// Replicationcontrollers  int    `json:"replicationcontrollers"`
	// Secrets                 int    `json:"secrets"`
	// Services                int    `json:"services"`
	// ServiceLoadbalancers    int    `json:"service-loadbalancers"`
	// ServiceNodeports        int    `json:"service-nodeports"`
	// Ingress                 int    `json:"ingress"`
}

// GDPTenantStatus defines the observed state of GDPTenant.
type GDPTenantStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the GDPTenant resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	Yunikorn   string             `json:"yunikorn,omitempty"`
	Trino      []string           `json:"trino,omitempty"`
	Ranger     []string           `json:"ranger,omitempty"`
	Namespace  string             `json:"namespace,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// GDPTenant is the Schema for the gdptenants API
// +kubebuilder:resource:scope=Cluster
type GDPTenant struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of GDPTenant
	// +required
	Spec GDPTenantSpec `json:"spec"`

	// status defines the observed state of GDPTenant
	// +optional
	Status GDPTenantStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// GDPTenantList contains a list of GDPTenant
type GDPTenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GDPTenant `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GDPTenant{}, &GDPTenantList{})
}
