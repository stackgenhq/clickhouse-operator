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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// The grower specs run under the package's single suite entrypoint
// (claims_list_suite_test.go).

// TestVolumeGrowerSuite runs the Azure managed-disk grower specs (part
// of the package's single TestAzureSuite entrypoint).

// fakeARM answers the ARM REST surface the grower touches: the VM GET
// (data disks by LUN) and the managed-disk GET/PATCH.
type fakeARM struct {
	vmDoc      string
	diskDoc    string
	diskSize   int
	resizes    map[string]int // disk id → target size
	diskStates map[string]string
}

func (f *fakeARM) Do(req *http.Request) (*http.Response, error) {
	respond := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
	}
	if strings.Contains(req.URL.Path, "/virtualMachines/") {
		if req.Method != http.MethodGet {
			return respond(http.StatusMethodNotAllowed, ""), nil
		}
		return respond(http.StatusOK, f.vmDoc), nil
	}
	switch req.Method {
	case http.MethodGet:
		return respond(http.StatusOK, f.diskDoc), nil
	case http.MethodPatch:
		var patch struct {
			Properties struct {
				DiskSizeGB int `json:"diskSizeGB"`
			} `json:"properties"`
		}
		_ = json.NewDecoder(req.Body).Decode(&patch)
		f.resizes["last"] = patch.Properties.DiskSizeGB
		return respond(http.StatusOK, f.diskDoc), nil
	}
	return respond(http.StatusNotFound, ""), nil
}

var _ = Describe("Azure VolumeGrower", func() {
	var grower *VolumeGrower

	BeforeEach(func() {
		grower = NewVolumeGrower().WithHTTPClient(&fakeARM{
			vmDoc: `{"properties":{"storageProfile":{"dataDisks":[
				{"lun":0,"name":"writer-0-data","managedDisk":{"id":"/subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/disks/writer-0-data"}},
				{"lun":1,"name":"writer-0-cache","managedDisk":{"id":"/subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/disks/writer-0-cache"}}
			]}}}`,
			diskDoc: `{"name":"writer-0-data","properties":{"diskSizeGB":64,"provisioningState":"Succeeded"}}`,
			resizes: map[string]int{},
		})
		grower.TokenSource = func(context.Context, string) (string, error) {
			return "token", nil
		}
		// The ARM scope envs: the describe path resolves THIS VM.
		_ = setEnv("CHINIT_AZURE_SUBSCRIPTION_ID", "s")
		_ = setEnv("CHINIT_AZURE_RESOURCE_GROUP", "rg")
		_ = setEnv("CHINIT_AZURE_VM_NAME", "writer-0")
	})

	It("describes the LUN-0 data disk with its current size", func() {
		info, err := grower.DescribeOwnVolume(context.Background(), port.VolumeGrowRequest{
			InstanceID: "vmid",
			Device:     "/dev/sdb",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(info.SizeGB).To(Equal(int32(64)))
		Expect(info.VolumeID).To(ContainSubstring("writer-0-data"))
		Expect(info.AttachedTo).To(Equal("vmid"))
	})

	It("grows the disk to the target and reports a started resize", func() {
		grew, err := grower.GrowVolume(context.Background(),
			"/subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/disks/writer-0-data", 96)
		Expect(err).NotTo(HaveOccurred())
		Expect(grew).To(BeTrue())
	})

	It("is a no-op when the disk is already at the target", func() {
		grew, err := grower.GrowVolume(context.Background(),
			"/subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/disks/writer-0-data", 64)
		Expect(err).NotTo(HaveOccurred())
		Expect(grew).To(BeFalse(), "an at-target disk must not be patched")
	})

	It("fails loudly when the VM scope envs are missing", func() {
		_ = setEnv("CHINIT_AZURE_VM_NAME", "")
		_, err := grower.DescribeOwnVolume(context.Background(), port.VolumeGrowRequest{InstanceID: "vmid"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("VM scope"))
	})
})

// setEnv sets an env var, restoring via DeferCleanup at the call site.
func setEnv(key, value string) error {
	DeferCleanup(func() { _ = os.Unsetenv(key) })
	return os.Setenv(key, value)
}
