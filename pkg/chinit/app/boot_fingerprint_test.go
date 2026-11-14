// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law and agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("specFingerprint", func() {
	writeSpec := func(dir, body string) string {
		p := filepath.Join(dir, "chinit.yaml")
		Expect(os.WriteFile(p, []byte(body), 0o600)).To(Succeed())
		return p
	}

	It("changes when only the spec file changes, even with the bundle hash unchanged", func() {
		dir := GinkgoT().TempDir()
		a := writeSpec(dir, "role: reader\nnetworks: [10.0.0.0/8]\n")
		first, err := specFingerprint(a, "bundleXYZ")
		Expect(err).NotTo(HaveOccurred())

		// Identical bundle hash, one different byte in the spec.
		_ = os.WriteFile(a, []byte("role: reader\nnetworks: [10.0.0.0/16]\n"), 0o600)
		second, err := specFingerprint(a, "bundleXYZ")
		Expect(err).NotTo(HaveOccurred())

		Expect(first).NotTo(Equal(second), "a spec-only edit must invalidate resume")
		Expect(first).To(ContainSubstring("bundleXYZ"))
	})

	It("is stable for an unchanged spec", func() {
		dir := GinkgoT().TempDir()
		a := writeSpec(dir, "role: writer\n")
		one, err := specFingerprint(a, "bundleXYZ")
		Expect(err).NotTo(HaveOccurred())
		two, err := specFingerprint(a, "bundleXYZ")
		Expect(err).NotTo(HaveOccurred())
		Expect(one).To(Equal(two))
	})

	It("falls back to a spec-only fingerprint when the bundle hash is unset", func() {
		dir := GinkgoT().TempDir()
		a := writeSpec(dir, "role: reader\n")
		got, err := specFingerprint(a, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HavePrefix("spec:"))
	})

	It("errors when the spec file is missing instead of fingerprinting stale state", func() {
		dir := GinkgoT().TempDir()
		_, err := specFingerprint(filepath.Join(dir, "absent.yaml"), "bundleXYZ")
		Expect(err).To(HaveOccurred())
	})
})
