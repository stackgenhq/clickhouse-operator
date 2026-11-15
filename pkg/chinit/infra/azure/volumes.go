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

package azure

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// VolumeGrower grows the node's own data disk online (ARM managed-disk
// resize — non-disruptive and idempotent, same contract as the AWS
// EBS grower). Self-healing contract: the disk-autoscale job asks
// ClickHouse for EFFECTIVE free space (system.disks — volume minus
// keep_free_space), and when it falls under the floor the job grows
// this VM's attached disk and the ext4 filesystem without downtime.
//
// The attachment seam is the VM's LUN: the disk-autoscale config names
// the data disk by device path (the label mount's device), which on
// Azure maps to the datadisk LUN the VMSS profile assigns. The grower
// lists the VM's dataDisks, matches by LUN (the device's nvmeXnY has no
// stable LUN arithmetic, but the DATA disk is conventionally LUN 0
// while cache/keeper take 1/2 — the same convention chinit's disk
// candidates use), and resizes the managed disk.
type VolumeGrower struct {
	client      httpClient
	TokenSource func(ctx context.Context, resource string) (string, error)
}

// NewVolumeGrower builds the grower.
func NewVolumeGrower() *VolumeGrower {
	return &VolumeGrower{
		client: http.DefaultClient,
		TokenSource: func(ctx context.Context, resource string) (string, error) {
			return managedIdentityToken(ctx, http.DefaultClient, resource)
		},
	}
}

// WithHTTPClient injects the HTTP client (tests).
func (g *VolumeGrower) WithHTTPClient(c httpClient) *VolumeGrower {
	g.client = c
	return g
}

// vmScope resolves THIS VM's ARM id from the environment (subscription,
// resource group, VM name). The same env-pinning pattern PrivateDNS
// uses: the wrapper user-data pins what the spec keeps provider-neutral.
// The VM name is the IMDS HostnameHint the boot already knows, but the
// disk-autoscale subcommand re-derives from state; env keeps this seam
// independent of both.
func vmScope() (string, error) {
	sub := strings.TrimSpace(os.Getenv("CHINIT_AZURE_SUBSCRIPTION_ID"))
	rg := strings.TrimSpace(os.Getenv("CHINIT_AZURE_RESOURCE_GROUP"))
	vm := strings.TrimSpace(os.Getenv("CHINIT_AZURE_VM_NAME"))
	if sub == "" || rg == "" || vm == "" {
		return "", fmt.Errorf("azure VM scope not configured (CHINIT_AZURE_SUBSCRIPTION_ID, CHINIT_AZURE_RESOURCE_GROUP, CHINIT_AZURE_VM_NAME)")
	}
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/virtualMachines/%s", sub, rg, vm), nil
}

// vmDoc is the VM response shape the grower reads (data disks only).
type vmDoc struct {
	Properties struct {
		StorageProfile struct {
			DataDisks []struct {
				Lun         int    `json:"lun"`
				Name        string `json:"name"`
				ManagedDisk struct {
					ID string `json:"id"`
				} `json:"managedDisk"`
			} `json:"dataDisks"`
		} `json:"storageProfile"`
	} `json:"properties"`
}

// diskDoc is the managed-disk response shape.
type diskDoc struct {
	Name       string `json:"name"`
	Properties struct {
		DiskSizeGB        int    `json:"diskSizeGB"`
		ProvisioningState string `json:"provisioningState"`
	} `json:"properties"`
	SKU struct {
		Name string `json:"name"`
	} `json:"sku"`
}

// lunOfDevice maps the disk-autoscale device path to the data-disk LUN
// by the stack's convention: the metadata/data disk is LUN 0, cache
// LUN 1, keeper LUN 2 (the Azure LUN layout the disk candidates
// resolve). The AWS default device (/dev/sdb) IS the data disk, so it
// maps to LUN 0 on Azure — only an explicit cache/keeper device path
// names another LUN.
func lunOfDevice(device string) int {
	switch {
	case strings.Contains(device, "sdc"), strings.Contains(device, "lun1"):
		return 1
	case strings.Contains(device, "sdd"), strings.Contains(device, "lun2"):
		return 2
	default:
		return 0
	}
}

// DescribeOwnVolume returns the data disk attached to this VM at the
// request's device/LUN.
func (g *VolumeGrower) DescribeOwnVolume(ctx context.Context, req port.VolumeGrowRequest) (*port.VolumeInfo, error) {
	scope, err := vmScope()
	if err != nil {
		return nil, err
	}
	token, err := g.TokenSource(ctx, "https://management.azure.com/")
	if err != nil {
		return nil, fmt.Errorf("managed identity token (ARM): %w", err)
	}
	vm, err := g.getJSON(ctx, token, fmt.Sprintf("https://management.azure.com%s?api-version=2023-03-01", scope))
	if err != nil {
		return nil, fmt.Errorf("describe VM %s: %w", scope, err)
	}
	var doc vmDoc
	if err := json.Unmarshal(vm, &doc); err != nil {
		return nil, fmt.Errorf("decode VM %s: %w", scope, err)
	}
	lun := lunOfDevice(req.Device)
	for _, d := range doc.Properties.StorageProfile.DataDisks {
		if d.Lun != lun {
			continue
		}
		if d.ManagedDisk.ID == "" {
			return nil, fmt.Errorf("data disk at LUN %d carries no managed-disk id", lun)
		}
		info, err := g.describeDisk(ctx, token, d.ManagedDisk.ID, req.InstanceID)
		if err != nil {
			return nil, err
		}
		return info, nil
	}
	return nil, fmt.Errorf("no data disk at LUN %d on this VM", lun)
}

// describeDisk fetches one managed disk's current size.
func (g *VolumeGrower) describeDisk(ctx context.Context, token, diskID, attachedTo string) (*port.VolumeInfo, error) {
	body, err := g.getJSON(ctx, token, fmt.Sprintf("https://management.azure.com%s?api-version=2023-04-02", diskID))
	if err != nil {
		return nil, fmt.Errorf("describe disk %s: %w", diskID, err)
	}
	var d diskDoc
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, fmt.Errorf("decode disk %s: %w", diskID, err)
	}
	return &port.VolumeInfo{
		VolumeID:   diskID,
		SizeGB:     int32(d.Properties.DiskSizeGB),
		AttachedTo: attachedTo,
	}, nil
}

// GrowVolume extends the managed disk to targetSizeGB (a no-op when
// already at or beyond the target) and returns whether a resize
// started. ARM's PATCH on diskSizeGB is the same online grow the
// portal performs — no detach, no downtime.
func (g *VolumeGrower) GrowVolume(ctx context.Context, volumeID string, targetSizeGB int32) (bool, error) {
	if volumeID == "" || targetSizeGB <= 0 {
		return false, fmt.Errorf("volume grower: disk id and positive target size required")
	}
	token, err := g.TokenSource(ctx, "https://management.azure.com/")
	if err != nil {
		return false, fmt.Errorf("managed identity token (ARM): %w", err)
	}
	cur, err := g.describeDisk(ctx, token, volumeID, "")
	if err != nil {
		return false, err
	}
	if cur.SizeGB >= targetSizeGB {
		return false, nil // already big enough
	}
	patch := struct {
		Properties struct {
			DiskSizeGB int `json:"diskSizeGB"`
		} `json:"properties"`
	}{}
	patch.Properties.DiskSizeGB = int(targetSizeGB)
	body, err := json.Marshal(patch)
	if err != nil {
		return false, err
	}
	url := fmt.Sprintf("https://management.azure.com%s?api-version=2023-04-02", volumeID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("resize disk %s: %w", volumeID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return false, fmt.Errorf("resize disk %s to %d: status %d", volumeID, targetSizeGB, resp.StatusCode)
	}
	return true, nil
}

// WaitForModification polls until the managed disk reports a steady
// provisioning state (ARM disk resize is asynchronous; the disk's
// provisioningState flips to "Updating" while the resize runs and
// back to "Succeeded" when it lands).
func (g *VolumeGrower) WaitForModification(ctx context.Context, volumeID string) error {
	token, err := g.TokenSource(ctx, "https://management.azure.com/")
	if err != nil {
		return fmt.Errorf("managed identity token (ARM): %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		body, err := g.getJSON(ctx, token, fmt.Sprintf("https://management.azure.com%s?api-version=2023-04-02", volumeID))
		if err != nil {
			return fmt.Errorf("describe disk %s: %w", volumeID, err)
		}
		var d diskDoc
		if err := json.Unmarshal(body, &d); err != nil {
			return fmt.Errorf("decode disk %s: %w", volumeID, err)
		}
		switch d.Properties.ProvisioningState {
		case "Succeeded", "":
			return nil // resize converged (or never started — both steady)
		case "Failed":
			return fmt.Errorf("disk resize failed for %s", volumeID)
		}
		select {
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// getJSON is the ARM GET helper.
func (g *VolumeGrower) getJSON(ctx context.Context, token, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return body, nil
}
