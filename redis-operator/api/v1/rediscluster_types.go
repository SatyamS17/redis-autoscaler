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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RedisClusterSpec defines the desired state of a Redis Cluster with autoscaling capabilities.
type RedisClusterSpec struct {
	// Masters is the number of active master nodes in the cluster (not including standby).
	// +kubebuilder:validation:Minimum=1
	Masters int32 `json:"masters"`

	// MinMasters is the minimum number of masters the cluster can scale down to.
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:default=3
	MinMasters int32 `json:"minMasters"`

	// ReplicasPerMaster is the number of replica nodes per master for high availability.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	ReplicasPerMaster int32 `json:"replicasPerMaster"`

	// RedisVersion specifies the Redis Docker image version to use.
	// +kubebuilder:default="7.2"
	RedisVersion string `json:"redisVersion,omitempty"`

	// AutoScaleEnabled enables or disables the autoscaling feature.
	AutoScaleEnabled bool `json:"autoScaleEnabled"`

	// CpuThreshold is the CPU usage percentage that triggers scale-up (0-100).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	CpuThreshold int32 `json:"cpuThreshold"`

	// CpuThresholdLow is the CPU usage percentage below which scale-down is considered (0-100).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=20
	CpuThresholdLow int32 `json:"cpuThresholdLow,omitempty"`

	// MemoryThreshold is the memory usage percentage that triggers scale-up (0-100).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=70
	MemoryThreshold int32 `json:"memoryThreshold,omitempty"`

	// MemoryThresholdLow is the memory usage percentage below which scale-down is considered (0-100).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=30
	MemoryThresholdLow int32 `json:"memoryThresholdLow,omitempty"`

	// ReshardTimeoutSeconds is the timeout for reshard and drain jobs in seconds.
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=600
	ReshardTimeoutSeconds int32 `json:"reshardTimeoutSeconds,omitempty"`

	// ScaleCooldownSeconds is the minimum time between scaling operations in seconds.
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=60
	ScaleCooldownSeconds int32 `json:"scaleCooldownSeconds,omitempty"`

	// PrometheusURL is the URL to the Prometheus server for metrics queries.
	// +kubebuilder:default="http://prometheus-operated.monitoring.svc:9090"
	PrometheusURL string `json:"prometheusURL,omitempty"`

	// MetricsQueryInterval is how often to query Prometheus for metrics in seconds.
	// +kubebuilder:validation:Minimum=5
	// +kubebuilder:validation:Maximum=300
	// +kubebuilder:default=15
	MetricsQueryInterval int32 `json:"metricsQueryInterval,omitempty"`

	// ExistingCluster indicates this CR is managing an existing Redis cluster.
	// When true, the operator will discover the cluster topology instead of bootstrapping.
	// +optional
	ExistingCluster bool `json:"existingCluster,omitempty"`

	// PodSelector is a label selector to identify Redis pods in an existing cluster.
	// Required when ExistingCluster is true. Example: {"app": "redis", "cluster": "my-cluster"}
	// +optional
	PodSelector map[string]string `json:"podSelector,omitempty"`

	// ServiceName is the name of the headless service for the existing cluster.
	// If not specified, defaults to "<cluster-name>-headless"
	// +optional
	ServiceName string `json:"serviceName,omitempty"`

	// ManageStatefulSet indicates whether the operator should manage the StatefulSet.
	// When false, the operator only manages autoscaling for an externally-managed StatefulSet.
	// +kubebuilder:default=true
	// +optional
	ManageStatefulSet bool `json:"manageStatefulSet,omitempty"`

	// StatefulSetName is the name of the existing StatefulSet to manage.
	// If not specified, defaults to the cluster name.
	// +optional
	StatefulSetName string `json:"statefulSetName,omitempty"`
}

// RedisClusterStatus defines the observed state of a Redis Cluster.
type RedisClusterStatus struct {
	// CurrentMasters is the actual number of active master nodes currently running.
	CurrentMasters int32 `json:"currentMasters"`

	// CurrentReplicas is the actual number of replica nodes currently running.
	CurrentReplicas int32 `json:"currentReplicas"`

	// Initialized indicates whether the cluster has completed bootstrap.
	// +optional
	Initialized bool `json:"initialized,omitempty"`

	// IsResharding indicates a scale-up operation is in progress.
	// +optional
	IsResharding bool `json:"isResharding,omitempty"`

	// IsProvisioningStandby indicates new standby pods are being added to the cluster.
	// +optional
	IsProvisioningStandby bool `json:"isProvisioningStandby,omitempty"`

	// IsDraining indicates a scale-down operation is in progress.
	// +optional
	IsDraining bool `json:"isDraining,omitempty"`

	// LastScaleTime records when the last scaling operation started (for cooldown).
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// StandbyPod is the name of the pod serving as the hot standby (0 hash slots).
	// +optional
	StandbyPod string `json:"standbyPod,omitempty"`

	// OverloadedPod is the pod that triggered the current scale-up operation.
	// +optional
	OverloadedPod string `json:"overloadedPod,omitempty"`

	// PodToDrain is the pod being drained during the current scale-down operation.
	// +optional
	PodToDrain string `json:"podToDrain,omitempty"`

	// DrainDestPod1 is the first destination pod for slots from the drained pod.
	// +optional
	DrainDestPod1 string `json:"drainDestPod1,omitempty"`

	// DrainDestPod2 is the second destination pod for slots from the drained pod.
	// Empty if only one destination is needed.
	// +optional
	DrainDestPod2 string `json:"drainDestPod2,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// RedisCluster is the Schema for the redisclusters API.
type RedisCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisClusterSpec   `json:"spec,omitempty"`
	Status RedisClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster.
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedisCluster{}, &RedisClusterList{})
}

// ValidateSpec validates the RedisCluster spec for logical consistency.
// Returns an error if the spec has invalid configuration.
func (r *RedisCluster) ValidateSpec() error {
	if r.Spec.CpuThreshold <= r.Spec.CpuThresholdLow {
		return fmt.Errorf("cpuThreshold (%d) must be greater than cpuThresholdLow (%d)",
			r.Spec.CpuThreshold, r.Spec.CpuThresholdLow)
	}

	if r.Spec.MemoryThreshold <= r.Spec.MemoryThresholdLow {
		return fmt.Errorf("memoryThreshold (%d) must be greater than memoryThresholdLow (%d)",
			r.Spec.MemoryThreshold, r.Spec.MemoryThresholdLow)
	}

	if r.Spec.Masters < r.Spec.MinMasters {
		return fmt.Errorf("masters (%d) cannot be less than minMasters (%d)",
			r.Spec.Masters, r.Spec.MinMasters)
	}

	// Validate existing cluster configuration
	if r.Spec.ExistingCluster {
		if len(r.Spec.PodSelector) == 0 {
			return fmt.Errorf("podSelector is required when existingCluster is true")
		}
		if r.Spec.ServiceName == "" {
			return fmt.Errorf("serviceName is required when existingCluster is true")
		}
	}

	return nil
}

// SetDefaults sets default values for optional fields that weren't provided.
func (r *RedisCluster) SetDefaults() {
	if r.Spec.RedisVersion == "" {
		r.Spec.RedisVersion = "7.2"
	}
	if r.Spec.MinMasters == 0 {
		r.Spec.MinMasters = 3
	}
	if r.Spec.MemoryThreshold == 0 {
		r.Spec.MemoryThreshold = 70
	}
	if r.Spec.MemoryThresholdLow == 0 {
		r.Spec.MemoryThresholdLow = 30
	}
	if r.Spec.CpuThresholdLow == 0 {
		r.Spec.CpuThresholdLow = 20
	}
	if r.Spec.ReshardTimeoutSeconds == 0 {
		r.Spec.ReshardTimeoutSeconds = 600
	}
	if r.Spec.ScaleCooldownSeconds == 0 {
		r.Spec.ScaleCooldownSeconds = 60
	}
	if r.Spec.PrometheusURL == "" {
		r.Spec.PrometheusURL = "http://prometheus-operated.monitoring.svc:9090"
	}
	if r.Spec.MetricsQueryInterval == 0 {
		r.Spec.MetricsQueryInterval = 15
	}
	if r.Spec.ReplicasPerMaster == 0 {
		r.Spec.ReplicasPerMaster = 1
	}

	// Defaults for existing cluster support
	if r.Spec.ServiceName == "" {
		r.Spec.ServiceName = r.Name + "-headless"
	}
	if r.Spec.StatefulSetName == "" {
		r.Spec.StatefulSetName = r.Name
	}
	// ManageStatefulSet defaults to true (kubebuilder default marker handles this)
}
