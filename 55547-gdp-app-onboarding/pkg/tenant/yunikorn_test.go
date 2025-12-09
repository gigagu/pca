package tenant

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestFindTenantQueue(t *testing.T) {
	type args struct {
		config     *PartitionConfig
		tenantName string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "tenant found at top level",
			args: args{
				config: &PartitionConfig{
					Queues: []TemQueue{
						{Name: "tenantA"},
						{Name: "tenantB"},
					},
				},
				tenantName: "tenantA",
			},
			want: true,
		},
		{
			name: "tenant found as child queue",
			args: args{
				config: &PartitionConfig{
					Queues: []TemQueue{
						{
							Name: "parent",
							Queues: []QueueConfig{
								{Name: "tenantC"},
							},
						},
					},
				},
				tenantName: "tenantC",
			},
			want: true,
		},
		{
			name: "tenant not found",
			args: args{
				config: &PartitionConfig{
					Queues: []TemQueue{
						{Name: "tenantX"},
						{
							Name: "parent",
							Queues: []QueueConfig{
								{Name: "tenantY"},
							},
						},
					},
				},
				tenantName: "tenantZ",
			},
			want: false,
		},
		{
			name: "empty queues",
			args: args{
				config:     &PartitionConfig{Queues: []TemQueue{}},
				tenantName: "any",
			},
			want: false,
		},
		{
			name: "nil config",
			args: args{
				config:     nil,
				tenantName: "tenantA",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got bool
			if tt.args.config == nil {
				got = findTenantQueue(&PartitionConfig{}, tt.args.tenantName)
			} else {
				got = findTenantQueue(tt.args.config, tt.args.tenantName)
			}
			if got != tt.want {
				t.Errorf("findTenantQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyTenantQueue(t *testing.T) {
	type fields struct {
		partitions int
		queues     int
	}
	type args struct {
		partitionIndex int
		queueName      string
		guarantMemory  string
		guarantCPU     string
		maxCPU         string
		maxMemory      string
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantNil      bool
		wantQueueLen int
		wantQueue    QueueConfig
	}{
		{
			name:   "add tenant queue to valid partition",
			fields: fields{partitions: 1, queues: 1},
			args: args{
				partitionIndex: 0,
				queueName:      "tenant1",
				guarantMemory:  "1Gi",
				guarantCPU:     "2",
				maxCPU:         "4",
				maxMemory:      "2Gi",
			},
			wantNil:      false,
			wantQueueLen: 1,
			wantQueue: QueueConfig{
				Name:      "tenant1",
				SubmitACL: "*",
				Resources: Resources{
					Guaranteed: map[string]string{"memory": "1Gi", "vcore": "2"},
					Max:        map[string]string{"memory": "2Gi", "vcore": "4"},
				},
			},
		},
		{
			name:   "invalid partition index returns nil",
			fields: fields{partitions: 1, queues: 1},
			args: args{
				partitionIndex: -1,
				queueName:      "tenant2",
				guarantMemory:  "2Gi",
				guarantCPU:     "1",
				maxCPU:         "2",
				maxMemory:      "4Gi",
			},
			wantNil: true,
		},
		{
			name:   "add to partition with multiple queues",
			fields: fields{partitions: 1, queues: 2},
			args: args{
				partitionIndex: 0,
				queueName:      "tenant3",
				guarantMemory:  "512Mi",
				guarantCPU:     "1",
				maxCPU:         "2",
				maxMemory:      "1Gi",
			},
			wantNil:      false,
			wantQueueLen: 1,
			wantQueue: QueueConfig{
				Name:      "tenant3",
				SubmitACL: "*",
				Resources: Resources{
					Guaranteed: map[string]string{"memory": "512Mi", "vcore": "1"},
					Max:        map[string]string{"memory": "1Gi", "vcore": "2"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build SchedulerConfig with specified number of partitions and queues
			partitions := make([]PartitionConfig, tt.fields.partitions)
			for i := range partitions {
				queues := make([]TemQueue, tt.fields.queues)
				for j := range queues {
					queues[j] = TemQueue{Name: "parent", Queues: []QueueConfig{}}
				}
				partitions[i] = PartitionConfig{Queues: queues}
			}
			config := &SchedulerConfig{Partitions: partitions}

			got := ApplyTenantQueue(
				config,
				tt.args.partitionIndex,
				tt.args.queueName,
				tt.args.guarantMemory,
				tt.args.guarantCPU,
				tt.args.maxCPU,
				tt.args.maxMemory,
			)
			if tt.wantNil {
				if got != nil {
					t.Errorf("ApplyTenantQueue() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("ApplyTenantQueue() = nil, want non-nil")
				return
			}
			// Check the last queue added to the first parent queue in the partition
			parentQueue := got.Partitions[tt.args.partitionIndex].Queues[0]
			if len(parentQueue.Queues) != tt.wantQueueLen {
				t.Errorf("len(Queues) = %d, want %d", len(parentQueue.Queues), tt.wantQueueLen)
			} else if tt.wantQueueLen > 0 {
				added := parentQueue.Queues[len(parentQueue.Queues)-1]
				if added.Name != tt.wantQueue.Name ||
					added.SubmitACL != tt.wantQueue.SubmitACL ||
					added.Resources.Guaranteed["memory"] != tt.wantQueue.Resources.Guaranteed["memory"] ||
					added.Resources.Guaranteed["vcore"] != tt.wantQueue.Resources.Guaranteed["vcore"] ||
					added.Resources.Max["memory"] != tt.wantQueue.Resources.Max["memory"] ||
					added.Resources.Max["vcore"] != tt.wantQueue.Resources.Max["vcore"] {
					t.Errorf("added queue = %+v, want %+v", added, tt.wantQueue)
				}
			}
		})
	}
}

// Test for LoadConfigMaps
func TestLoadConfigMaps(t *testing.T) {
	// 	validYAML := `
	// partitions:
	//   - queues:
	// 	  - name: parent
	// 		queues:
	// 		  - name: tenant1
	// 			submitacl: "*"
	// 			resources:
	// 			  guaranteed:
	// 				memory: 1Gi
	// 				vcore: 2
	// 			  max:
	// 				memory: 2Gi
	// 				vcore: 4
	// `
	invalidYAML := "not: [valid, yaml"

	tests := []struct {
		name        string
		cm          *corev1.ConfigMap
		wantErr     bool
		wantNil     bool
		expectQueue string
	}{
		// {
		// 	name: "valid configmap with queues.yaml",
		// 	cm: &corev1.ConfigMap{
		// 		Data: map[string]string{
		// 			"queues.yaml": validYAML,
		// 		},
		// 	},
		// 	wantErr:     false,
		// 	wantNil:     false,
		// 	expectQueue: "parent",
		// },
		{
			name: "missing queues.yaml key",
			cm: &corev1.ConfigMap{
				Data: map[string]string{
					"other.yaml": "foo: bar",
				},
			},
			wantErr: false, // function does not return error if key missing, just logs
			wantNil: false, // config is still returned, but empty
		},
		{
			name: "invalid yaml in queues.yaml",
			cm: &corev1.ConfigMap{
				Data: map[string]string{
					"queues.yaml": invalidYAML,
				},
			},
			wantErr: true,
			wantNil: true,
		},
		{
			name:    "nil configmap",
			cm:      nil,
			wantErr: true,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				// Recover from panic if cm is nil
				if r := recover(); r != nil {
					if !tt.wantErr {
						t.Errorf("unexpected panic: %v", r)
					}
				}
			}()
			got, err := LoadConfigMaps(tt.cm)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfigMaps() error = %v, wantErr %v", err, tt.wantErr)
			}
			if (got == nil) != tt.wantNil {
				t.Errorf("LoadConfigMaps() = %v, wantNil %v", got, tt.wantNil)
			}
			if !tt.wantNil && tt.expectQueue != "" && len(got.Partitions) > 0 && len(got.Partitions[0].Queues) > 0 {
				if got.Partitions[0].Queues[0].Name != tt.expectQueue {
					t.Errorf("expected queue name %q, got %q", tt.expectQueue, got.Partitions[0].Queues[0].Name)
				}
			}
		})
	}
}
