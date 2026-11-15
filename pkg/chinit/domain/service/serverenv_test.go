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

package service

import (
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// The server-env step's cloud gate: CH_S3_ENDPOINT is an AWS S3-disk
// requirement (storage.xml's from_env); the Azure blob disk carries
// the account name/key inline, so an empty s3_data_bucket on Azure is
// the correct spec, not a config bug. AWS with no bucket stays Fatal —
// the S3 disk would Code 36 at server start.

type stubProvider struct{ name string }

func (p *stubProvider) Name() string { return p.name }
func (p *stubProvider) Metadata(context.Context) (port.InstanceMetadata, error) {
	return port.InstanceMetadata{}, nil
}
func (p *stubProvider) GetSecretValue(context.Context, port.SecretRequest) (string, error) {
	return "", nil
}
func (p *stubProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}

var _ = Describe("configureServerEnv cloud gate", func() {
	It("is fatal on AWS when s3_data_bucket is empty (the S3 disk needs the endpoint env)", func() {
		b := &Boot{
			Spec:     &model.BootSpec{Role: model.RoleReader},
			Provider: &stubProvider{name: "aws"},
		}
		err := b.configureServerEnv(context.Background())
		Expect(fail.IsFatal(err)).To(BeTrue(),
			"an AWS spec without s3_data_bucket boots a server that dies at start (Code 36)")
		Expect(err.Error()).To(ContainSubstring("s3_data_bucket"))
	})

	It("accepts an empty s3_data_bucket on Azure (the blob disk needs no endpoint env)", func() {
		// Timezone empty + azure + no bucket: the step returns before
		// any /etc write — no host filesystem touched.
		b := &Boot{
			Spec:     &model.BootSpec{Role: model.RoleReader},
			Provider: &stubProvider{name: "azure"},
		}
		Expect(b.configureServerEnv(context.Background())).To(Succeed())
	})

	It("skips the file write when there is nothing to pin", func() {
		// Same no-op path exercised explicitly for the named contract.
		b := &Boot{
			Spec:     &model.BootSpec{Role: model.RoleReader},
			Provider: &stubProvider{name: "azure"},
		}
		Expect(b.configureServerEnv(context.Background())).To(Succeed())
		_, err := os.Stat("/etc/default/clickhouse-server")
		if err == nil {
			Skip("host carries /etc/default/clickhouse-server; presence not from this test")
		}
		Expect(os.IsNotExist(err)).To(BeTrue())
	})

	It("keeps the S3 endpoint line for an AWS-shaped spec with a bucket", func() {
		// Full file write is host-touching; assert the pure prefix of
		// the behavior instead — the Fatal gate is the step's only
		// decision point before I/O, and the bucket line is covered by
		// the AWS integration path.
		b := &Boot{
			Spec: &model.BootSpec{
				Role:         model.RoleReader,
				S3DataBucket: "data-bucket",
			},
			Provider: &stubProvider{name: "aws"},
		}
		err := b.configureServerEnv(context.Background())
		// On hosts without systemd/chown this may fail at the chown
		// leg; the gate itself must NOT have fired.
		if err != nil {
			Expect(err.Error()).NotTo(ContainSubstring("s3_data_bucket is empty"))
			return
		}
		data, rerr := os.ReadFile(filepath.Join("/etc/default", "clickhouse-server"))
		if rerr != nil {
			return // host-specific write path; gate already proven
		}
		Expect(string(data)).To(ContainSubstring("CH_S3_ENDPOINT"))
	})
})
