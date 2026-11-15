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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// TableClaimLister implements LeaseLister's discovery half over the
// claimer's own table: one Query enumerates every live lease. Lives on
// the claimer (same account, same token source, same client) — the
// provider delegates ListActiveClaims here.
//
// Query shape: Table Storage Query Entities with
// $filter=lease_until gt <unix-now> — the server-side liveness gate.
// The claim rows all live in one PartitionKey ("readers" — the AWS
// port's one-role-partition shape carried over, RowKey is the
// hostname), so the query is one partition read, not a table scan.
type TableClaimLister struct {
	claimer *TableClaimer
}

// NewTableClaimLister builds the lister over the claimer.
func NewTableClaimLister(c *TableClaimer) *TableClaimLister {
	return &TableClaimLister{claimer: c}
}

// queryResponse is the no-metadata JSON envelope of Query Entities.
type queryResponse struct {
	Value []leaseEntity `json:"value"`
}

// ListActiveClaims returns every lease row with lease_until > now.
//
// Belt and braces with the AWS lister: the server-side $filter keeps
// expiring rows off the wire, and the code-side check (clock skew
// between the filter evaluation and the response decode) keeps them
// out of the inventory — a stale claim must never render into
// remote_servers (the vacant-slot finding).
func (l *TableClaimLister) ListActiveClaims(ctx context.Context, req chinitport.ListClaimsRequest) ([]chinitport.Claim, error) {
	c := l.claimer
	if c.accountName == "" {
		return nil, fmt.Errorf("azure storage account not configured (CHINIT_AZURE_STORAGE_ACCOUNT)")
	}
	now := time.Now().Unix()
	base := fmt.Sprintf("https://%s.table.core.windows.net/%s()", c.accountName, req.Table)
	q := url.Values{}
	q.Set("$filter", fmt.Sprintf("lease_until gt %d", now))
	q.Set("Accept", "application/json;odata=nometadata")
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	token, err := c.TokenSource(ctx, "https://storage.azure.com/")
	if err != nil {
		return nil, fmt.Errorf("managed identity token (table): %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Accept", "application/json;odata=nometadata")

	var claims []chinitport.Claim
	nextLink := ""
	for {
		target := httpReq
		if nextLink != "" {
			target, err = http.NewRequestWithContext(ctx, http.MethodGet, nextLink, nil)
			if err != nil {
				return nil, err
			}
			target.Header.Set("Authorization", "Bearer "+token)
			target.Header.Set("Accept", "application/json;odata=nometadata")
		}
		resp, err := c.client.Do(target)
		if err != nil {
			return nil, fmt.Errorf("query lease table %s: %w", req.Table, err)
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("query lease table %s: status %d", req.Table, resp.StatusCode)
		}
		if readErr != nil {
			return nil, readErr
		}
		var page queryResponse
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("decode lease rows in %s: %w", req.Table, err)
		}
		for _, row := range page.Value {
			// Code-side liveness filter (same contract as the AWS
			// lister): a row that expired between the filter and the
			// decode must never reach the inventory.
			if row.LeaseUntil <= now {
				continue
			}
			claims = append(claims, chinitport.Claim{
				Hostname:   row.RowKey,
				InstanceID: row.InstanceID,
				IP:         row.IP,
			})
		}
		// OData pagination: odata.nextLink (no-metadata responses
		// still carry it when the query pages).
		var meta struct {
			Next string `json:"odata.nextLink"`
		}
		_ = json.Unmarshal(body, &meta)
		if meta.Next == "" {
			break
		}
		nextLink = meta.Next
		if !strings.HasPrefix(strings.ToLower(meta.Next), "https://") {
			// nextLink may be host-relative; anchor it to the account.
			nextLink = fmt.Sprintf("https://%s.table.core.windows.net/%s", c.accountName, strings.TrimPrefix(meta.Next, "/"))
		}
	}
	return claims, nil
}
