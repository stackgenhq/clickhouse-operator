// Copyright 2026 Altinity Ltd and/or its affiliates. All rights reserved.
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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// selfClaimFakeDynamoDB records every condition expression so the specs
// can assert the self-claim disjunct is present. It answers PutItem as
// if the condition failed for every ordinal except the one the caller
// flags as "owned by us".
type selfClaimFakeDynamoDB struct {
	ownedOrdinal int   // the ordinal this instance holds; -1 = none
	putAttempts  int
	conditions   []string
}

func (f *selfClaimFakeDynamoDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.putAttempts++
	f.conditions = append(f.conditions, *in.ConditionExpression)
	hostname := in.Item["hostname"].(*dynamodbtypes.AttributeValueMemberS).Value
	if f.ownedOrdinal >= 0 && hostname == hostnameFromOrdinal("writer-", f.ownedOrdinal) {
		// Own lease: the instance_id = :me disjunct lets our put succeed.
		if contains(*in.ConditionExpression, "instance_id = :me") {
			return &dynamodb.PutItemOutput{}, nil
		}
		return nil, &dynamodbtypes.ConditionalCheckFailedException{}
	}
	return nil, &dynamodbtypes.ConditionalCheckFailedException{}
}

func (f *selfClaimFakeDynamoDB) UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (f *selfClaimFakeDynamoDB) DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}

func hostnameFromOrdinal(prefix string, n int) string {
	if n == 0 {
		return prefix + "0"
	}
	suffix := ""
	for x := n; x > 0; x /= 10 {
		suffix = string(rune('0'+x%10)) + suffix
	}
	return prefix + suffix
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

var _ = Describe("DynamoDBClaimer self-claim", func() {
	It("re-claims the ordinal this instance already holds instead of skipping it", func() {
		fake := &selfClaimFakeDynamoDB{ownedOrdinal: 1}
		claimer := NewDynamoDBClaimer("us-east-1").WithClient(fake)
		got, err := claimer.Claim(context.Background(), chinitport.ClaimRequest{
			Table:           "node-claims",
			Prefix:          "writer-",
			Count:           3,
			InstanceID:      "i-self",
			LeaseSeconds:    900,
			DeadlineSeconds: 5,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(1), "the node must re-claim ITS OWN lease rather than drifting to another ordinal")
		for _, cond := range fake.conditions {
			Expect(cond).To(ContainSubstring("instance_id = :me"),
				"every put must carry the self-claim disjunct so a refresh re-claim cannot steal/move the lease")
		}
	})

	It("keeps walking when the self-held ordinal is taken but OTHER ordinals are claimed by others", func() {
		fake := &selfClaimFakeDynamoDB{ownedOrdinal: -1} // we hold nothing
		claimer := NewDynamoDBClaimer("us-east-1").WithClient(fake)
		_, err := claimer.Claim(context.Background(), chinitport.ClaimRequest{
			Table:           "node-claims",
			Prefix:          "writer-",
			Count:           2,
			InstanceID:      "i-self",
			LeaseSeconds:    900,
			DeadlineSeconds: 2,
		})
		// All ordinals refuse the write — the claim must fail, not silently succeed.
		Expect(err).To(HaveOccurred())
	})
})
