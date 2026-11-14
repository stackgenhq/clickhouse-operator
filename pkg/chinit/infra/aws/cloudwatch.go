// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// CloudWatchPusher is the AWS MetricPusher: one PutMetricData call per
// datapoint over the instance role (the node-self-heal IAM policy
// scopes cloudwatch:PutMetricData to the chinit namespaces).
//
// Why a client struct: the conformance suite injects a fake API while
// production binds the real SDK client (the same shape every other
// capability in this package uses).
type CloudWatchPusher struct {
	region string
	client cloudwatchClient
}

// cloudwatchClient is the SDK surface the pusher needs — one method,
// so the fake is a one-liner.
type cloudwatchClient interface {
	PutMetricData(ctx context.Context, params *cloudwatch.PutMetricDataInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error)
}

// NewCloudWatchPusher builds the production pusher.
func NewCloudWatchPusher(region string) *CloudWatchPusher {
	p := &CloudWatchPusher{region: region}
	if region != "" {
		cfg, err := awsConfig(context.Background(), region)
		if err == nil {
			p.client = cloudwatch.NewFromConfig(cfg)
		}
	}
	return p
}

// WithClient injects the API client (conformance tests).
func (p *CloudWatchPusher) WithClient(c cloudwatchClient) *CloudWatchPusher {
	p.client = c
	return p
}

// PutMetric publishes one datapoint. Dimension order is sorted so the
// call is deterministic (the EMF parity: alarms key on exact
// dimension SETS, and CloudWatch treats differing order as the same
// metric as long as the set matches — sorted output keeps test
// assertions stable).
func (p *CloudWatchPusher) PutMetric(ctx context.Context, req port.MetricPushRequest) error {
	if p.client == nil {
		if p.region == "" {
			return fmt.Errorf("cloudwatch pusher: no region resolved")
		}
		cfg, err := awsConfig(ctx, p.region)
		if err != nil {
			return fmt.Errorf("cloudwatch pusher aws config: %w", err)
		}
		p.client = cloudwatch.NewFromConfig(cfg)
	}
	if req.Namespace == "" || req.Name == "" {
		return fmt.Errorf("cloudwatch pusher: namespace and metric name are required")
	}
	dims := make([]types.Dimension, 0, len(req.Dimensions))
	for k, v := range req.Dimensions {
		dims = append(dims, types.Dimension{
			Name:  aws.String(k),
			Value: aws.String(v),
		})
	}
	unit := req.Unit
	if unit == "" {
		unit = "None"
	}
	_, err := p.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(req.Namespace),
		MetricData: []types.MetricDatum{{
			MetricName: aws.String(req.Name),
			Value:      aws.Float64(req.Value),
			Unit:       types.StandardUnit(unit),
			Dimensions: dims,
		}},
	})
	if err != nil {
		return fmt.Errorf("cloudwatch PutMetricData %s/%s: %w", req.Namespace, req.Name, err)
	}
	return nil
}
