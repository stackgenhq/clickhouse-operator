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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// VolumeGrower grows the node's own data volume online. Self-healing
// contract: the disk-autoscale job asks ClickHouse for EFFECTIVE free
// space (system.disks — volume minus keep_free_space), and when it
// falls under the floor the job grows this instance's attached volume
// and the ext4 filesystem without downtime.
type VolumeGrower struct {
	region string
	client ec2Client
}

// ec2Client is the SDK surface the grower needs (the fake is a
// one-liner for the conformance suite).
type ec2Client interface {
	DescribeVolumes(ctx context.Context, params *ec2.DescribeVolumesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error)
	ModifyVolume(ctx context.Context, params *ec2.ModifyVolumeInput, optFns ...func(*ec2.Options)) (*ec2.ModifyVolumeOutput, error)
	DescribeVolumesModifications(ctx context.Context, params *ec2.DescribeVolumesModificationsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVolumesModificationsOutput, error)
}

// NewVolumeGrower builds the production grower.
func NewVolumeGrower(region string) *VolumeGrower {
	return &VolumeGrower{region: region}
}

// WithEC2Client injects the API client (conformance tests).
func (g *VolumeGrower) WithEC2Client(c ec2Client) *VolumeGrower {
	g.client = c
	return g
}

func (g *VolumeGrower) client_(ctx context.Context) (ec2Client, error) {
	if g.client != nil {
		return g.client, nil
	}
	if g.region == "" {
		return nil, fmt.Errorf("volume grower: no region resolved")
	}
	cfg, err := awsConfig(ctx, g.region)
	if err != nil {
		return nil, fmt.Errorf("volume grower aws config: %w", err)
	}
	g.client = ec2.NewFromConfig(cfg)
	return g.client, nil
}

// DescribeOwnVolume finds the volume attached to this instance at the
// given device (e.g. /dev/sdb → nvme1n1 mapping is the platform's).
func (g *VolumeGrower) DescribeOwnVolume(ctx context.Context, req port.VolumeGrowRequest) (*port.VolumeInfo, error) {
	c, err := g.client_(ctx)
	if err != nil {
		return nil, err
	}
	if req.InstanceID == "" {
		return nil, fmt.Errorf("volume grower: instance id required")
	}
	filters := []types.Filter{{
		Name:   aws.String("attachment.instance-id"),
		Values: []string{req.InstanceID},
	}}
	if req.Device != "" {
		filters = append(filters, types.Filter{
			Name:   aws.String("attachment.device"),
			Values: []string{req.Device},
		})
	}
	out, err := c.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{Filters: filters})
	if err != nil {
		return nil, fmt.Errorf("describe volumes: %w", err)
	}
	if len(out.Volumes) == 0 {
		return nil, fmt.Errorf("no volume attached to %s at %s", req.InstanceID, req.Device)
	}
	return &port.VolumeInfo{
		VolumeID:   aws.ToString(out.Volumes[0].VolumeId),
		SizeGB:     aws.ToInt32(out.Volumes[0].Size),
		AttachedTo: req.InstanceID,
	}, nil
}

// GrowVolume extends the volume to targetSizeGB (a no-op when already
// at or beyond the target) and returns whether a modification started.
func (g *VolumeGrower) GrowVolume(ctx context.Context, volumeID string, targetSizeGB int32) (bool, error) {
	c, err := g.client_(ctx)
	if err != nil {
		return false, err
	}
	if volumeID == "" || targetSizeGB <= 0 {
		return false, fmt.Errorf("volume grower: volume id and positive target size required")
	}
	cur, err := c.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: []string{volumeID},
	})
	if err != nil {
		return false, fmt.Errorf("describe volume %s: %w", volumeID, err)
	}
	if len(cur.Volumes) == 0 {
		return false, fmt.Errorf("volume %s not found", volumeID)
	}
	if aws.ToInt32(cur.Volumes[0].Size) >= targetSizeGB {
		return false, nil // already big enough
	}
	_, err = c.ModifyVolume(ctx, &ec2.ModifyVolumeInput{
		VolumeId: aws.String(volumeID),
		Size:     aws.Int32(targetSizeGB),
	})
	if err != nil {
		return false, fmt.Errorf("modify volume %s to %d: %w", volumeID, targetSizeGB, err)
	}
	return true, nil
}

// WaitForModification blocks until the volume's in-flight modification
// completes (bounded by the ctx deadline).
func (g *VolumeGrower) WaitForModification(ctx context.Context, volumeID string) error {
	c, err := g.client_(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		out, err := c.DescribeVolumesModifications(ctx, &ec2.DescribeVolumesModificationsInput{
			VolumeIds: []string{volumeID},
		})
		if err != nil {
			return fmt.Errorf("describe modifications %s: %w", volumeID, err)
		}
		if len(out.VolumesModifications) == 0 {
			return nil // no modification in flight
		}
		switch string(out.VolumesModifications[len(out.VolumesModifications)-1].ModificationState) {
		case "completed":
			return nil
		case "failed":
			return fmt.Errorf("volume modification failed for %s", volumeID)
		}
		select {
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
