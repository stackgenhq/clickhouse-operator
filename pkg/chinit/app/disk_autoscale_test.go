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

package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins `chinit disk-autoscale` — the every-5-minute timer the
// units matrix installs on writers AND readers. The job keeps EFFECTIVE
// disk headroom above the floor by growing the node's own data volume +
// filesystem online (the 2026-11-14 incident: keep_free_space on a
// 32 GiB volume left ~1 MiB effective, merges stalled on
// NOT_ENOUGH_SPACE, parts climbed past 1800 toward the
// parts_to_throw_insert hard reject at 3000).

// diskAutoscaleDir is this suite's scratch dir (state files) — never
// /var/lib.
var diskAutoscaleDir = filepath.Join(os.TempDir(), "chinit-app-disk-autoscale-test")

// diskAutoscaleState seeds a writer's state file.
func diskAutoscaleState(identity model.NodeIdentity) string {
	data, err := json.Marshal(nodeState{Identity: identity})
	Expect(err).NotTo(HaveOccurred())
	Expect(os.MkdirAll(diskAutoscaleDir, 0755)).To(Succeed())
	path := filepath.Join(diskAutoscaleDir, "state.json")
	Expect(os.WriteFile(path, data, 0644)).To(Succeed())
	return path
}

// diskAutoscaleFakes records the exec + provider surface.
type diskAutoscaleFakes struct {
	runner   *fakeRunner
	grower   *fakeVolumeGrower
	pusher   *fakeMetricPusher
	provider *diskAutoscaleProvider
}

// fakeVolumeGrower records grow calls and answers with fixed volumes.
type fakeVolumeGrower struct {
	volume      *chinitport.VolumeInfo
	described   []chinitport.VolumeGrowRequest
	grew        []int32
	waited      []string
	describeErr error
	growErr     error
}

func (f *fakeVolumeGrower) DescribeOwnVolume(ctx context.Context, req chinitport.VolumeGrowRequest) (*chinitport.VolumeInfo, error) {
	f.described = append(f.described, req)
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return f.volume, nil
}

func (f *fakeVolumeGrower) GrowVolume(ctx context.Context, volumeID string, targetSizeGB int32) (bool, error) {
	if f.growErr != nil {
		return false, f.growErr
	}
	f.grew = append(f.grew, targetSizeGB)
	return true, nil
}

func (f *fakeVolumeGrower) WaitForModification(ctx context.Context, volumeID string) error {
	f.waited = append(f.waited, volumeID)
	return nil
}

// fakeMetricPusher records pushes.
type fakeMetricPusher struct {
	pushed []chinitport.MetricPushRequest
}

func (f *fakeMetricPusher) PutMetric(ctx context.Context, req chinitport.MetricPushRequest) error {
	f.pushed = append(f.pushed, req)
	return nil
}

// diskAutoscaleProvider composes the capability fakes.
type diskAutoscaleProvider struct {
	name   string
	grower chinitport.VolumeGrower
	pusher chinitport.MetricPusher
}

func (p *diskAutoscaleProvider) Name() string { return p.name }
func (p *diskAutoscaleProvider) Metadata(context.Context) (chinitport.InstanceMetadata, error) {
	return chinitport.InstanceMetadata{}, nil
}
func (p *diskAutoscaleProvider) GetSecretValue(context.Context, chinitport.SecretRequest) (string, error) {
	return "", nil
}
func (p *diskAutoscaleProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}
func (p *diskAutoscaleProvider) DescribeOwnVolume(ctx context.Context, req chinitport.VolumeGrowRequest) (*chinitport.VolumeInfo, error) {
	return p.grower.DescribeOwnVolume(ctx, req)
}
func (p *diskAutoscaleProvider) GrowVolume(ctx context.Context, id string, size int32) (bool, error) {
	return p.grower.GrowVolume(ctx, id, size)
}
func (p *diskAutoscaleProvider) WaitForModification(ctx context.Context, id string) error {
	return p.grower.WaitForModification(ctx, id)
}
func (p *diskAutoscaleProvider) PutMetric(ctx context.Context, req chinitport.MetricPushRequest) error {
	return p.pusher.PutMetric(ctx, req)
}

// noGrowProvider is the capability-absent provider: metrics yes, volume
// growth no — AsVolumeGrower must return nil for it.
type noGrowProvider struct {
	name   string
	pusher chinitport.MetricPusher
}

func (p *noGrowProvider) Name() string { return p.name }
func (p *noGrowProvider) Metadata(context.Context) (chinitport.InstanceMetadata, error) {
	return chinitport.InstanceMetadata{}, nil
}
func (p *noGrowProvider) GetSecretValue(context.Context, chinitport.SecretRequest) (string, error) {
	return "", nil
}
func (p *noGrowProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}
func (p *noGrowProvider) PutMetric(ctx context.Context, req chinitport.MetricPushRequest) error {
	return p.pusher.PutMetric(ctx, req)
}

var _ = Describe("runDiskAutoscale", func() {
	var fakes *diskAutoscaleFakes
	var statePath string

	seed := func(freeGB float64) {
		fakes = &diskAutoscaleFakes{runner: &fakeRunner{}}
		identity := model.NodeIdentity{
			Role:        model.RoleWriter,
			Hostname:    "writer-0",
			InstanceID:  "i-0cf1cda168d841a50",
			Region:      "us-west-2",
			ClusterName: "ch-int-dev",
		}
		statePath = diskAutoscaleState(identity)

		fakes.grower = &fakeVolumeGrower{volume: &chinitport.VolumeInfo{
			VolumeID: "vol-test", SizeGB: 32, AttachedTo: identity.InstanceID,
		}}
		fakes.pusher = &fakeMetricPusher{}
		fakes.provider = &diskAutoscaleProvider{name: "aws", grower: fakes.grower, pusher: fakes.pusher}

		origRun := runCommand
		origBuild := buildProvider
		runCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			// The free-space probe: return the seeded bytes.
			return []byte(strconv.FormatInt(int64(freeGB*(1<<30)), 10)), nil
		}
		buildProvider = func(string, string) (chinitport.Provider, error) {
			return fakes.provider, nil
		}
		DeferCleanup(func() {
			runCommand = origRun
			buildProvider = origBuild
		})
	}

	It("is a no-op above the floor", func() {
		seed(40) // 40 GiB effective > 8 GiB floor
		Expect(runDiskAutoscale([]string{"-state", statePath})).To(Succeed())
		Expect(fakes.grower.grew).To(BeEmpty())
		Expect(fakes.grower.described).To(BeEmpty())
	})

	It("grows the volume and resizes the filesystem below the floor", func() {
		seed(2) // 2 GiB effective < 8 GiB floor: must grow by 32 to 64
		Expect(runDiskAutoscale([]string{"-state", statePath})).To(Succeed())
		Expect(fakes.grower.described).To(HaveLen(1))
		Expect(fakes.grower.described[0].InstanceID).To(Equal("i-0cf1cda168d841a50"))
		Expect(fakes.grower.described[0].Device).To(Equal("/dev/sdb"))
		Expect(fakes.grower.grew).To(Equal([]int32{64}))
		Expect(fakes.grower.waited).To(Equal([]string{"vol-test"}))
		// The fs step shells findmnt/resize2fs: the exec fake answered
		// with the seeded bytes for every call, so success means the
		// commands ran and were tolerated.
		autoScaled := false
		for _, p := range fakes.pusher.pushed {
			if p.Name == "DiskAutoscaled" {
				autoScaled = true
			}
		}
		Expect(autoScaled).To(BeTrue(), "the grow outcome must be published for the alarm side")
	})

	It("fails loudly when the provider cannot grow volumes", func() {
		seed(2)
		// Capability absent: a provider type with no grow methods
		// (AsVolumeGrower returns nil), not a nil grower field.
		origBuild := buildProvider
		buildProvider = func(string, string) (chinitport.Provider, error) {
			return &noGrowProvider{name: "aws", pusher: fakes.pusher}, nil
		}
		DeferCleanup(func() { buildProvider = origBuild })

		err := runDiskAutoscale([]string{"-state", statePath})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cannot grow volumes"))
		// The pressure finding is still published (the alarm-side signal).
		pressurePushed := false
		for _, p := range fakes.pusher.pushed {
			if p.Name == "DiskEffectiveFreeGB" {
				pressurePushed = true
			}
		}
		Expect(pressurePushed).To(BeTrue(), "the pressure finding must reach CloudWatch even when the remedy is absent")
	})

	It("never grows past the ceiling", func() {
		seed(2)
		fakes.grower.volume.SizeGB = 250 // near the 256 GiB ceiling
		Expect(runDiskAutoscale([]string{"-state", statePath})).To(Succeed())
		Expect(fakes.grower.grew).To(Equal([]int32{256}))
	})

	It("reports the pressure without growing when already at the ceiling", func() {
		seed(2)
		fakes.grower.volume.SizeGB = 256
		err := runDiskAutoscale([]string{"-state", statePath})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("ceiling"))
		Expect(fakes.grower.grew).To(BeEmpty())
	})
})
