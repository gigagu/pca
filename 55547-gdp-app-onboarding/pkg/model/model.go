package model

type AppManifest struct {
	MetaData struct {
		ManifestSchema  string `json:"manifest-schema"`
		ManifestVersion string `json:"manifest-version"`
	} `json:"meta-data"`
	PlatformConfig []PlatformConfig `json:"platform-config"`
}

type PlatformConfig struct {
	Cluster                    string         `json:"cluster",omitempty`
	Product                    string         `json:"product",omitempty`
	ResilienceLevel            string         `json:"resilience-level",omitempty`
	Target                     []string       `json:"target",omitempty`
	ItamName                   string         `json:"itam-name",omitempty`
	Sbia                       int            `json:"sbia",omitempty`
	Ibs                        bool           `json:"ibs",omitempty`
	Lob                        string         `json:"lob",omitempty`
	ItoUnit                    string         `json:"ito-unit",omitempty`
	Itam                       int            `json:"itam",omitempty`
	BC                         string         `json:"bc",omitempty`
	Tenant                     string         `json:"tenant",omitempty`
	TenantShortName            string         `json:"tenant-short-name,omitempty"`
	TenantDesc                 string         `json:"tenant-desc",omitempty`
	TenantNoninteractiveOwner  []string       `json:"tenant-noninteractive-owner",omitempty`
	TenantNoninteractiveViewer []string       `json:"tenant-noninteractive-viewer",omitempty`
	TenantInteractiveOwner     []string       `json:"tenant-interactive-owner",omitempty`
	TenantInteractiveViewer    []string       `json:"tenant-interactive-viewer",omitempty`
	Namespace                  string         `json:"namespace",omitempty"`
	SecretPath                 string         `json:"hcv-secret-path",omitempty`
	JobQueue                   JobQueue       `json:"jobs-queue,omitempty"`
	ResourceQuotas             ResourceQuotas `json:"resource-quotas",omitempty`
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

type ObjectStoreBuckets struct {
	Name       string `json:"name"`
	BucketSize string `json:"bucket-size"`
	Location   string `json:"location, omitempty", default:"ap-southeast-1"`
}
type JobQueue struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
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

// type LimitRanges struct {
// 	Items []LimitRangeItem `json:"items,omitempty"`
// }

// // LimitRangeItem defines a min/max usage limit for any resource that matches on kind.
// type LimitRangeItem struct {
// 	// Type of resource that this limit applies to.
// 	Type corev1.LimitType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=LimitType"`
// 	// Max usage constraints on this kind by resource name.
// 	// +optional
// 	Max corev1.ResourceList `json:"max,omitempty" protobuf:"bytes,2,rep,name=max,casttype=ResourceList,castkey=ResourceName"`
// 	// Min usage constraints on this kind by resource name.
// 	// +optional
// 	Min corev1.ResourceList `json:"min,omitempty" protobuf:"bytes,3,rep,name=min,casttype=ResourceList,castkey=ResourceName"`
// 	// Default resource requirement limit value by resource name if resource limit is omitted.
// 	// +optional
// 	Default corev1.ResourceList `json:"default,omitempty" protobuf:"bytes,4,rep,name=default,casttype=ResourceList,castkey=ResourceName"`
// 	// DefaultRequest is the default resource requirement request value by resource name if resource request is omitted.
// 	// +optional
// 	DefaultRequest corev1.ResourceList `json:"default-request,omitempty" protobuf:"bytes,5,rep,name=defaultRequest,casttype=ResourceList,castkey=ResourceName"`
// 	// MaxLimitRequestRatio if specified, the named resource must have a request and limit that are both non-zero where limit divided by request is less than or equal to the enumerated value; this represents the max burst for the named resource.
// 	// +optional
// 	MaxLimitRequestRatio corev1.ResourceList `json:"max-limitrequest-ratio,omitempty" protobuf:"bytes,6,rep,name=maxLimitRequestRatio,casttype=ResourceList,castkey=ResourceName"`
// }
