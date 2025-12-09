package tenant

import (
	"fmt"

	// "github.com/apache/yunikorn-core/pkg/common/configs"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog"
)

func LoadConfigMaps(cm *corev1.ConfigMap) (*SchedulerConfig, error) {
	queueConfigYAML, ok := cm.Data["queues.yaml"]
	if !ok {
		klog.Infof("cannot find queues.yaml in ConfigMap: %+v", cm.Name)
	}

	var config SchedulerConfig
	if err := yaml.Unmarshal([]byte(queueConfigYAML), &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal queues.yaml: %w", err)
	}
	// klog.Infof("yunikorn configmap unmarshal to struct: %+v", config)

	return &config, nil
}

func findTenantQueue(config *PartitionConfig, tenantName string) bool {
	for _, queue := range config.Queues {
		if queue.Name == tenantName {
			return true
		}
		for _, childQueue := range queue.Queues {
			if childQueue.Name == tenantName {
				return true
			}
		}
	}

	return false
}

/*
*
new tenant --> addTenantQueue into Configmap
update tenant --> updateTenantQueue into Configmap
modify configmap --> loopAllTenants and updateTenantQueue into Configmap
*/

func ApplyTenantQueue(config *SchedulerConfig, partitionIndex int, queueName string, cpu string, memory string) *SchedulerConfig {
	if partitionIndex < 0 {
		klog.Errorf("partitionIndex %v", partitionIndex)
		return nil
	}

	if findTenantQueue(&config.Partitions[partitionIndex], queueName) {
		klog.Infof("tenant queue %s already exists", queueName)
		// Support: update queue if queue already exist

		config.Partitions[partitionIndex] = updateTenantQueue(&config.Partitions[partitionIndex], queueName, cpu, memory)
		out, _ := yaml.Marshal(config)
		klog.Infof("After updating tenant queue: %v", string(out))
		return config
	} else {
		tenantQueue := QueueConfig{
			Name:      queueName,
			SubmitACL: "*",
			Resources: Resources{
				Guaranteed: map[string]string{
					"memory": memory,
					"vcore":  cpu,
				},
				Max: map[string]string{
					"memory": memory,
					"vcore":  cpu,
				},
			},
		}
		config.Partitions[partitionIndex].Queues[0].Queues = append(config.Partitions[partitionIndex].Queues[0].Queues, tenantQueue)
	}

	out, _ := yaml.Marshal(config)
	klog.Infof("After adding tenant queue: %v", string(out))

	return config
}

func updateTenantQueue(partition *PartitionConfig, queueName string, cpu, memory string) PartitionConfig {
	for qIndex, queue := range partition.Queues[0].Queues {
		if queue.Name == queueName {
			partition.Queues[0].Queues[qIndex].Resources.Guaranteed["memory"] = memory
			partition.Queues[0].Queues[qIndex].Resources.Guaranteed["vcore"] = cpu
			partition.Queues[0].Queues[qIndex].Resources.Max["memory"] = memory
			partition.Queues[0].Queues[qIndex].Resources.Max["vcore"] = cpu
			// partition.Queues[qIndex].Resources.Guaranteed["memory"] = guarantMemory
			// partition.Queues[qIndex].Resources.Guaranteed["vcore"] = guarantCPU
			// partition.Queues[qIndex].Resources.Max["memory"] = maxMemory
			// partition.Queues[qIndex].Resources.Max["vcore"] = maxCPU
			return *partition
		}
	}
	return *partition
}
