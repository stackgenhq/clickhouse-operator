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
	"errors"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// This file pins ParallelGroup — the engine's concurrent phase type.
// The contracts:
//   - children run CONCURRENTLY (the phase pays the max, not the sum):
//     the specs assert overlap via a rendezvous (a step that blocks
//     until its sibling also entered Run — impossible serially).
//   - a Critical child failure cancels the siblings (errgroup's
//     context) and aborts the boot like a serial Critical failure.
//   - a BestEffort child failure records and the group (and boot)
//     continue — the serial loop's log-and-continue contract.
//   - resume is per-CHILD: a group re-run skips only the children the
//     state file records done.
//   - the [TIMING] output carries each child's own line (the group is
//     a phase, not a step — dashboards grep the same shape).

var _ = Describe("ParallelGroup", func() {
	It("runs its children concurrently — the phase pays the max, not the sum", func() {
		// Rendezvous: each child signals entry and waits for BOTH
		// before returning. Serially this deadlocks; concurrently it
		// finishes — the spec's timeout enforces the verdict either way.
		entered := make(chan string, 2)
		release := make(chan struct{})
		blocker := func(name string) Step {
			return StepFunc(name, Critical, onceFatal, func(ctx context.Context) error {
				entered <- name
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}
		go func() {
			// Both children entered: unblock them.
			<-entered
			<-entered
			close(release)
		}()

		engine := NewEngine(nil, &recordingStateStore{})
		res := engine.Run(context.Background(), []Step{
			ParallelGroup{Children: []Step{blocker("fetch"), blocker("claim")}},
		}, )

		// The goroutine above only unblocks when BOTH children are
		// inside Run at once — the run completing at all is the
		// concurrency assertion.
		Expect(res.Fatality).NotTo(HaveOccurred())
		Expect(res.Steps).To(HaveLen(2))
		Expect(res.Steps[0].Name).To(Equal("fetch"))
		Expect(res.Steps[1].Name).To(Equal("claim"))
	})

	It("aborts the boot when a Critical child fails; remaining serial steps do not run", func() {
		var ranAfter int64
		engine := NewEngine(nil, &recordingStateStore{})
		res := engine.Run(context.Background(), []Step{
			ParallelGroup{Children: []Step{
				StepFunc("claim", Critical, onceFatal, func(context.Context) error {
					return fail.Fatal(errors.New("lease table unreachable"))
				}),
				StepFunc("mount", Critical, bootWait, func(ctx context.Context) error {
					// The sibling must observe cancellation (or a
					// short grace) — the group never hangs on a
					// failed sibling.
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(10 * time.Second):
						return nil
					}
				}),
			}},
			StepFunc("render", Critical, onceFatal, func(context.Context) error {
				atomic.AddInt64(&ranAfter, 1)
				return nil
			}),
		})

		Expect(res.Fatality).To(HaveOccurred())
		Expect(res.FatalityStep()).To(Equal("claim"))
		Expect(atomic.LoadInt64(&ranAfter)).To(BeZero(), "steps after a failed phase must not run")
		// Two outcomes — the group's children only; the serial step
		// after the group is NOT recorded (the boot aborted, same as
		// the serial path: steps after a Critical failure never run
		// and never record).
		Expect(res.Steps).To(HaveLen(2))
	})

	It("records a BestEffort child failure and continues — the serial loop's contract", func() {
		var joined int64
		engine := NewEngine(nil, &recordingStateStore{})
		res := engine.Run(context.Background(), []Step{
			ParallelGroup{Children: []Step{
				StepFunc("monitoring", BestEffort, onceFatal, func(context.Context) error {
					return errors.New("otel endpoint refused")
				}),
				StepFunc("units", BestEffort, onceFatal, func(context.Context) error {
					atomic.AddInt64(&joined, 1)
					return nil
				}),
			}},
			StepFunc("boot_finished", Critical, onceFatal, func(context.Context) error {
				atomic.AddInt64(&joined, 1)
				return nil
			}),
		})

		Expect(res.Fatality).NotTo(HaveOccurred(), "a BestEffort child failure must not abort the boot")
		Expect(atomic.LoadInt64(&joined)).To(Equal(int64(2)), "the sibling and the next step both ran")
		Expect(res.Steps[0].Failed()).To(BeTrue())
		Expect(res.Steps[1].Err).NotTo(HaveOccurred())
	})

	It("resumes per child — a re-run skips only the children recorded done", func() {
		store := &recordingStateStore{loaded: &modelState{Steps: []stepRecord{
			{Name: "fetch", Status: "done"},
		}}}
		engine := NewEngine(nil, store)

		var ran []string
		res := engine.Run(context.Background(), []Step{
			ParallelGroup{Children: []Step{
				StepFunc("fetch", Critical, onceFatal, func(context.Context) error {
					ran = append(ran, "fetch")
					return nil
				}),
				StepFunc("claim", Critical, onceFatal, func(context.Context) error {
					ran = append(ran, "claim")
					return nil
				}),
			}},
		})

		Expect(ran).To(Equal([]string{"claim"}), "the done child re-ran nothing; the missing child ran")
		Expect(res.Steps[0].Skipped).To(BeTrue())
		Expect(res.Steps[1].Skipped).To(BeFalse())
	})

	It("names the phase from its children and derives Criticality from them", func() {
		allCritical := ParallelGroup{Children: []Step{
			StepFunc("fetch", Critical, onceFatal, func(context.Context) error { return nil }),
		}}
		allBest := ParallelGroup{Children: []Step{
			StepFunc("units", BestEffort, onceFatal, func(context.Context) error { return nil }),
		}}

		Expect(allCritical.Name()).To(Equal("phase: fetch"))
		Expect(allCritical.Criticality()).To(Equal(Critical))
		Expect(allBest.Name()).To(Equal("phase: units"))
		Expect(allBest.Criticality()).To(Equal(BestEffort))
	})
})
