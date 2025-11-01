package redis

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CreateReshardJob creates a temporary Job to rebalance hash slots after scaling.
func CreateReshardJob(ctx context.Context, c client.Client, namespace string, clusterName string, redisPassword string) error {
	jobName := clusterName + "-reshard-job"

	// If an existing job is still around, skip creating a new one
	var existing batchv1.Job
	if err := c.Get(ctx, client.ObjectKey{Name: jobName, Namespace: namespace}, &existing); err == nil {
		fmt.Println("Reshard job already exists, skipping")
		return nil
	}

	// Build internal DNS target for the rebalance command
	target := fmt.Sprintf("%s-0.%s-headless.%s.svc.cluster.local:6379", clusterName, clusterName, namespace)

	// The actual redis-cli command
	cmd := fmt.Sprintf(
		"redis-cli -a '%s' --cluster rebalance %s --cluster-use-empty-masters --cluster-yes",
		redisPassword, target,
	)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "reshard",
							Image:   "bitnamilegacy/redis-cluster:7.2",
							Command: []string{"/bin/bash", "-c", cmd},
						},
					},
				},
			},
		},
	}

	if err := c.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create reshard job: %w", err)
	}

	fmt.Println("Reshard job created successfully")

	// Wait for completion (simple polling)
	timeout := time.After(3 * time.Minute)
	ticker := time.Tick(10 * time.Second)

	for {
		select {
		case <-timeout:
			return fmt.Errorf("reshard job timed out")
		case <-ticker:
			var j batchv1.Job
			if err := c.Get(ctx, client.ObjectKey{Name: jobName, Namespace: namespace}, &j); err != nil {
				return fmt.Errorf("failed to get job: %w", err)
			}
			if j.Status.Succeeded > 0 {
				fmt.Println("Reshard job completed successfully")
				_ = c.Delete(ctx, &j) // optional cleanup
				return nil
			}
			if j.Status.Failed > 0 {
				return fmt.Errorf("reshard job failed (see logs for %s)", jobName)
			}
		}
	}
}
