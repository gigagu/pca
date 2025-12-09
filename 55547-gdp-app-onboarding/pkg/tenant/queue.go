package tenant

type SchedulerConfig struct {
	Partitions []PartitionConfig `yaml: partitions,omitempty json: partitions,omitempty`
	Checksum   string            `yaml:",omitempty" json:",omitempty"`
}

type PartitionConfig struct {
	Name           string                    `yaml: name,omitempty json: name,omitempty`
	Queues         []TemQueue                `yaml: queues,omitempty json: queues,omitempty`
	PlacementRules []PlacementRule           `yaml:placementrules",omitempty" json:placementrules",omitempty"`
	Limits         []Limit                   `yaml:limits",omitempty" json:limits",omitempty"`
	Preemption     PartitionPreemptionConfig `yaml:preemption",omitempty" json:preemption",omitempty"`
	NodeSortPolicy NodeSortingPolicy         `yaml:nodesortpolicy",omitempty" json:",omitempty"`
}

type NodeSortingPolicy struct {
	Type            string             `yaml:type,omitempty" json:type,omitempty"`
	ResourceWeights map[string]float64 `yaml:",omitempty" json:",omitempty"`
}

// The partition preemption configuration
type PartitionPreemptionConfig struct {
	Enabled *bool `yaml:enabled",omitempty" json:enabled",omitempty"`
}

// The queue object for each queue:
// - the name of the queue
// - a resources object to specify resource limits on the queue
// - the maximum number of applications that can run in the queue
// - a set of properties, exact definition of what can be set is not part of the yaml
// - ACL for submit and or admin access
// - a list of sub or child queues
// - a list of users specifying limits on a queue
type TemQueue struct {
	Name            string            `yaml: name,omitempty json: name,omitempty`
	Parent          bool              `yaml:parent",omitempty" json:parent",omitempty"`
	MaxApplications uint64            `yaml:maxapplications",omitempty" json:maxapplications",omitempty"`
	Properties      map[string]string `yaml:properties",omitempty" json:properties",omitempty"`
	AdminACL        string            `yaml:adminacl",omitempty" json:adminacl",omitempty"`
	SubmitACL       string            `yaml:submitacl",omitempty" json:submitacl",omitempty"`
	ChildTemplate   ChildTemplate     `yaml:childtemplate",omitempty" json:childtemplate",omitempty"`
	Queues          []QueueConfig     `yaml:queues",omitempty" json:queues",omitempty"`
}

type QueueConfig struct {
	Name            string            `yaml: name,omitempty json: name,omitempty`
	Parent          bool              `yaml:parent",omitempty" json:parent",omitempty"`
	Resources       Resources         `yaml:resources",omitempty" json:resources",omitempty"`
	MaxApplications uint64            `yaml:maxapplications",omitempty" json:maxapplications",omitempty"`
	Properties      map[string]string `yaml:properties",omitempty" json:properties",omitempty"`
	AdminACL        string            `yaml:adminacl",omitempty" json:adminacl",omitempty"`
	SubmitACL       string            `yaml:submitacl",omitempty" json:submitacl",omitempty"`
	ChildTemplate   ChildTemplate     `yaml:childtemplate",omitempty" json:childtemplate",omitempty"`
	Queues          []QueueConfig     `yaml:queues",omitempty" json:queues",omitempty"`
	Limits          []Limit           `yaml:limits",omitempty" json:limits",omitempty"`
}

type ChildTemplate struct {
	MaxApplications uint64            `yaml:maxapplications",omitempty" json:maxapplications",omitempty"`
	Properties      map[string]string `yaml:properties",omitempty" json:properties",omitempty"`
	Resources       Resources         `yaml:resources",omitempty" json:resources",omitempty"`
}

// The resource limits to set on the queue. The definition allows for an unlimited number of types to be used.
// The mapping to "known" resources is not handled here.
// - guaranteed resources
// - max resources
type Resources struct {
	Guaranteed map[string]string `yaml:guaranteed,omitempty" json:guaranteed,omitempty"`
	Max        map[string]string `yaml:max,omitempty" json:max,omitempty"`
}

// The queue placement rule definition
// - the name of the rule
// - create flag: can the rule create a queue
// - user and group filter to be applied on the callers
// - rule link to allow setting a rule to generate the parent
// - value a generic value interpreted depending on the rule type (i.e queue name for the "fixed" rule
// or the application label name for the "tag" rule)
type PlacementRule struct {
	Name   string         `yaml: name,omitempty json: name,omitempty`
	Create bool           `yaml: create,omitempty json: create,omitempty`
	Filter Filter         `yaml: filter,omitempty json: filter,omitempty`
	Parent *PlacementRule `yaml: parent,omitempty json: parent,omitempty`
	Value  string         `yaml: value,omitempty json: value,omitempty"`
}

// The user and group filter for a rule.
// - type of filter (allow or deny filter, empty means allow)
// - list of users to filter (maybe empty)
// - list of groups to filter (maybe empty)
// if the list of users or groups is exactly 1 long it is interpreted as a regular expression
type Filter struct {
	Type   string   `yaml: type,omitempty json: type,omitempty`
	Users  []string `yaml: users,omitempty json: users,omitempty`
	Groups []string `yaml: groups,omitempty json: groups,omitempty`
}
type Limits struct {
	Limit []Limit
}

// The limit object to specify user and or group limits at different levels in the partition or queues
// Different limits for the same user or group may be defined at different levels in the hierarchy
// - limit description (optional)
// - list of users (maybe empty)
// - list of groups (maybe empty)
// - maximum resources as a resource object to allow for the user or group
// - maximum number of applications the user or group can have running
type Limit struct {
	Limit           string            `yaml:limit",omitempty" json:limit",omitempty"`
	Users           []string          `yaml:users",omitempty" json:users",omitempty"`
	Groups          []string          `yaml:groups",omitempty" json:groups",omitempty"`
	MaxResources    map[string]string `yaml:maxresources",omitempty" json:maxresources",omitempty"`
	MaxApplications uint64            `yaml:maxapplications",omitempty" json:maxapplications",omitempty"`
}
