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

// RedisClusterSpec defines the desired state of RedisCluster.
type RedisClusterSpec struct {
	// +kubebuilder:validation:Minimum=1
	Masters int32 `json:"masters"`
	// +kubebuilder:default=3
	MinMasters int32 `json:"minMasters"`
	// +kubebuilder:validation:Minimum=0
	ReplicasPerMaster int32  `json:"replicasPerMaster"`
	AutoScaleEnabled  bool   `json:"autoScaleEnabled"`
	CpuThreshold      int32  `json:"cpuThreshold"`
	CpuThresholdLow   int32  `json:"cpuThresholdLow,omitempty"`
	RedisVersion      string `json:"redisVersion,omitempty"`
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=300
	ReshardTimeoutSeconds int32 `json:"reshardTimeoutSeconds,omitempty"`
}

// RedisClusterStatus defines the observed state of RedisCluster.
type RedisClusterStatus struct {
	CurrentMasters  int32 `json:"currentMasters"`
	CurrentReplicas int32 `json:"currentReplicas"`
	AvgCPUUsage     int32 `json:"avgCpuUsage"`
	// +optional
	Initialized bool `json:"initialized,omitempty"`
	// +optional
	IsResharding bool `json:"isResharding,omitempty"`
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`
	// IsDraining locks the autoscaler while a pod is being drained for scale-down.
	IsDraining bool `json:"isDraining,omitempty"`
	// PodToDrain stores the name of the pod selected for draining.
	PodToDrain string `json:"podToDrain,omitempty"`
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
