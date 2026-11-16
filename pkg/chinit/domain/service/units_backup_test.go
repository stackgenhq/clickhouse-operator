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

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// This file pins configureBackup's config.yml shape. The golden AMI
// ships clickhouse-backup 2.5.0, which predates the `password_file`
// config key (silently ignored → empty-password auth, code 516), and
// the `default` user is chctl-restricted (readonly mode rejects the
// tool's connect_timeout setting). Without this pin, a node boot
// regresses to an unusable backup config and the nightly backup
// never lands (the Nov 15 finding: an empty backups bucket for days).

var _ = Describe("configureBackup", func() {
	var dir string

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
		old := BootstrapDir
		BootstrapDir = dir
		oldPath := bootstrapAdminPasswordPath
		bootstrapAdminPasswordPath = func() string { return filepath.Join(BootstrapDir, "admin-password") }
		oldCfgDir := backupConfigDir
		backupConfigDir = func() string { return filepath.Join(dir, "etc-clickhouse-backup") }
		oldBin := backupToolBin
		backupToolBin = func() string { return filepath.Join(dir, "bin", "clickhouse-backup") }
		Expect(os.MkdirAll(filepath.Join(dir, "bin"), 0755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "bin", "clickhouse-backup"), []byte("#!/bin/sh\n"), 0755)).To(Succeed())
		DeferCleanup(func() {
			BootstrapDir = old
			bootstrapAdminPasswordPath = oldPath
			backupConfigDir = oldCfgDir
			backupToolBin = oldBin
		})
		Expect(os.WriteFile(filepath.Join(dir, "admin-password"), []byte("sekrit\n"), 0600)).To(Succeed())
	})

	It("writes an inline admin password for clickhouse-backup 2.5.0", func() {
		// RoleWriter (not RoleBackup) exercises the config render
		// without the backup replica's systemd timer install — the
		// config shape is the regression this pins.
		b := &Boot{Spec: &model.BootSpec{
			Role:        model.RoleWriter,
			ClusterName: "ch-int-dev",
			Backup:      model.Backup{Bucket: "bkt", Prefix: "backups", RetentionDays: 7, CompressionFormat: "tar"},
		}, Identity: model.NodeIdentity{Region: "us-west-2"}}

		Expect(configureBackupForTest(b)).To(Succeed())

		cfg, err := os.ReadFile(backupConfigPath())
		Expect(err).NotTo(HaveOccurred())
		Expect(string(cfg)).To(ContainSubstring("  username: admin\n"))
		// 2.5.0 ignores password_file; the password must be inline.
		Expect(string(cfg)).To(ContainSubstring("  password: sekrit\n"))
		Expect(string(cfg)).NotTo(ContainSubstring("password_file"))
		// The retention and bucket wiring ride along.
		Expect(string(cfg)).To(ContainSubstring("backups_to_keep_remote: 7"))
		Expect(string(cfg)).To(ContainSubstring("bucket: bkt"))
		Expect(string(cfg)).To(ContainSubstring("prefix: backups/ch-int-dev"))
	})

	It("fails closed when the admin password file is missing", func() {
		Expect(os.Remove(filepath.Join(dir, "admin-password"))).To(Succeed())
		b := &Boot{Spec: &model.BootSpec{
			Role:        model.RoleWriter,
			ClusterName: "ch-int-dev",
			Backup:      model.Backup{Bucket: "bkt"},
		}, Identity: model.NodeIdentity{Region: "us-west-2"}}

		err := configureBackupForTest(b)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("admin-password"))
	})
})

// configureBackupForTest runs configureBackup with a context so tests
// can call it directly against the temp BootstrapDir.
func configureBackupForTest(b *Boot) error {
	return b.configureBackup(context.Background())
}
