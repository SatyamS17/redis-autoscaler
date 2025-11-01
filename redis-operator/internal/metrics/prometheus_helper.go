package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// QueryPrometheus runs a Prometheus query and returns numeric results
func QueryPrometheus(prometheusURL string, query string) (float64, error) {
	client, err := api.NewClient(api.Config{Address: prometheusURL})
	if err != nil {
		return 0, fmt.Errorf("failed to create Prometheus client: %v", err)
	}

	v1api := v1.NewAPI(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, warnings, err := v1api.Query(ctx, query, time.Now())
	if err != nil {
		return 0, fmt.Errorf("Prometheus query failed: %v", err)
	}

	if len(warnings) > 0 {
		fmt.Printf("Prometheus warnings: %v\n", warnings)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return 0, fmt.Errorf("unexpected result type: %T", result)
	}

	if len(vector) == 0 {
		return 0, fmt.Errorf("no data returned")
	}

	return float64(vector[0].Value), nil
}
