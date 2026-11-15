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
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)


// errorsAs is errors.As (indirection keeps the dynamodb.go file's SDK
// imports to the two it needs).
func errorsAs(err error, target interface{}) bool { return errors.As(err, target) }

// Route53API is the Route53 surface the registrar needs (tests fake it).
type Route53API interface {
	ListHostedZonesByName(ctx context.Context, in *route53.ListHostedZonesByNameInput, opts ...func(*route53.Options)) (*route53.ListHostedZonesByNameOutput, error)
	ChangeResourceRecordSets(ctx context.Context, in *route53.ChangeResourceRecordSetsInput, opts ...func(*route53.Options)) (*route53.ChangeResourceRecordSetsOutput, error)
	ListResourceRecordSets(ctx context.Context, in *route53.ListResourceRecordSetsInput, opts ...func(*route53.Options)) (*route53.ListResourceRecordSetsOutput, error)
}

// Route53Registrar manages the node's private-zone A record.
type Route53Registrar struct {
	region string
	client Route53API
}

// NewRoute53Registrar builds the registrar.
func NewRoute53Registrar(region string) *Route53Registrar {
	return &Route53Registrar{region: region}
}

// WithClient injects a concrete client (tests).
func (r *Route53Registrar) WithClient(c Route53API) *Route53Registrar {
	r.client = c
	return r
}

func (r *Route53Registrar) clientOrInit(ctx context.Context) (Route53API, error) {
	if r.client != nil {
		return r.client, nil
	}
	cfg, err := awsConfig(ctx, r.region)
	if err != nil {
		return nil, err
	}
	r.client = route53.NewFromConfig(cfg)
	return r.client, nil
}

// ResolveZoneID finds the private zone by exact DNS name (Terraform
// cannot pass the zone id without a module cycle; boot resolution is
// the documented workaround, cached in the identity snapshot).
func (r *Route53Registrar) ResolveZoneID(ctx context.Context, zoneName string) (string, error) {
	client, err := r.clientOrInit(ctx)
	if err != nil {
		return "", err
	}
	out, err := client.ListHostedZonesByName(ctx, &route53.ListHostedZonesByNameInput{
		DNSName: awsString(zoneName),
	})
	if err != nil {
		return "", err
	}
	want := zoneName
	if len(want) == 0 || want[len(want)-1] != '.' {
		want += "."
	}
	for _, z := range out.HostedZones {
		if z.Name != nil && *z.Name == want {
			id := *z.Id
			// The API returns /hostedzone/<id>; the change API wants
			// the bare id.
			return trimHostedZonePrefix(id), nil
		}
	}
	return "", &zoneNotFoundError{Zone: zoneName}
}

// zoneNotFoundError names the resolution failure (WARNING-class: the
// boot continues without self-registration; the caller decides).
type zoneNotFoundError struct{ Zone string }

func (e *zoneNotFoundError) Error() string {
	return "no Route53 hosted zone named " + e.Zone
}

func trimHostedZonePrefix(id string) string {
	const prefix = "/hostedzone/"
	if len(id) > len(prefix) && id[:len(prefix)] == prefix {
		return id[len(prefix):]
	}
	return id
}

// UpsertARecord creates or replaces the node's A record.
func (r *Route53Registrar) UpsertARecord(ctx context.Context, req chinitport.ARecordRequest) error {
	client, err := r.clientOrInit(ctx)
	if err != nil {
		return err
	}
	if req.TTL == 0 {
		req.TTL = 60
	}
	_, err = client.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: awsString(req.ZoneID),
		ChangeBatch: &route53types.ChangeBatch{
			Changes: []route53types.Change{{
				Action: route53types.ChangeActionUpsert,
				ResourceRecordSet: &route53types.ResourceRecordSet{
					Name:            awsString(fqdn(req.RecordName, req.ZoneName)),
					Type:            route53types.RRTypeA,
					TTL:             int64Ptr(req.TTL),
					ResourceRecords: []route53types.ResourceRecord{{Value: awsString(req.IP)}},
				},
			}},
		},
	})
	return err
}

// DeleteARecord removes the node's A record (drain/termination).
//
// Route53 DELETE requires the EXACT ResourceRecords values of the live
// record ("Expected exactly one of [AliasTarget, all of [TTL, and
// ResourceRecords]..." — the live 0.2.7 finding: a valueless DELETE is
// rejected and the A record lingers after scale-in, so peers keep
// resolving a dead node's IP). The request's IP is used when present;
// otherwise the live set is listed by name and deleted value-by-value
// (the drain-time record may have been re-registered under a different
// IP by a replacement node that already claimed the ordinal).
//
// A requested-IP delete that FAILS falls back to the live value set:
// a previous holder's drain may never have released the name (an
// EC2-health-check termination whose drain could not dispatch leaves
// the A record pointing at the OLD instance's IP — the live
// 2026-11-15 finding: writer-1.ch-int-dev.clickhouse.internal kept a
// dead 10.0.1.236 for hours, hanging every clusterAllReplicas fan-out
// and schema-sync on the fleet). The terminating node owns the ordinal
// for the hook's duration, so any value under this name is stale by
// construction.
func (r *Route53Registrar) DeleteARecord(ctx context.Context, req chinitport.ARecordRequest) error {
	client, err := r.clientOrInit(ctx)
	if err != nil {
		return err
	}
	d := newRecordDeleter(r, client, req)
	values, err := d.targetValues(ctx)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return nil // nothing to delete — the record is already gone
	}
	for _, v := range values {
		if err := d.deleteValue(ctx, v); err != nil {
			// The value may not match the live record (a previous
			// holder's stale record): sweep the LIVE value set.
			return d.fallbackToLiveValues(ctx, err)
		}
	}
	return nil
}

// recordDeleter owns one record's DELETE flow: the requested values,
// the single-value delete call, and the stale-value fallback. A value
// type over the registrar's client so the fallback lives at the same
// boundary as the primary delete (one place knows the DELETE shape).
type recordDeleter struct {
	// registrar resolves the record's FQDN (name + zone joining).
	registrar *Route53Registrar
	// client is the SDK surface (the tests' fake).
	client Route53API
	// req is the caller's delete request (zone, name, requested IP).
	req chinitport.ARecordRequest
}

// newRecordDeleter binds the delete flow's collaborators.
func newRecordDeleter(registrar *Route53Registrar, client Route53API, req chinitport.ARecordRequest) *recordDeleter {
	return &recordDeleter{registrar: registrar, client: client, req: req}
}

// targetValues names the values the primary delete attempts: the
// requested IP when the caller knows it, the live set otherwise (a
// DELETE without the exact value set is rejected).
func (d *recordDeleter) targetValues(ctx context.Context) ([]string, error) {
	if d.req.IP != "" {
		return []string{d.req.IP}, nil
	}
	return d.registrar.listRecordValues(ctx, d.client, d.req.ZoneID, d.recordName())
}

// deleteValue removes one value from the named record.
func (d *recordDeleter) deleteValue(ctx context.Context, value string) error {
	_, err := d.client.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: awsString(d.req.ZoneID),
		ChangeBatch: &route53types.ChangeBatch{
			Changes: []route53types.Change{{
				Action: route53types.ChangeActionDelete,
				ResourceRecordSet: &route53types.ResourceRecordSet{
					Name:            awsString(d.recordName()),
					Type:            route53types.RRTypeA,
					TTL:             int64Ptr(d.req.TTL),
					ResourceRecords: []route53types.ResourceRecord{{Value: awsString(value)}},
				},
			}},
		},
	})
	return err
}

// fallbackToLiveValues is the requested-value-delete failure path: the
// record may carry a stale previous holder's value the request cannot
// name. It sweeps the LIVE value set by name; the already-failed value
// is not retried. primaryErr is returned unchanged when the fallback
// itself cannot proceed (the original failure is the truth).
func (d *recordDeleter) fallbackToLiveValues(ctx context.Context, primaryErr error) error {
	live, err := d.registrar.listRecordValues(ctx, d.client, d.req.ZoneID, d.recordName())
	if err != nil {
		return primaryErr // the original delete failure is the truth
	}
	if len(live) == 0 {
		return nil // a concurrent cleanup won the race
	}
	for _, v := range live {
		if err := d.deleteValue(ctx, v); err != nil {
			return err
		}
	}
	return nil
}

// recordName is the record's FQDN.
func (d *recordDeleter) recordName() string {
	return fqdn(d.req.RecordName, d.req.ZoneName)
}

// listRecordValues returns the A-record values a name currently holds
// (empty when the record is gone) — the no-IP delete's target set and
// the stale-value fallback's sweep set.
func (r *Route53Registrar) listRecordValues(ctx context.Context, client Route53API, zoneID, recordName string) ([]string, error) {
	out, err := client.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{
		HostedZoneId:    awsString(zoneID),
		StartRecordName: awsString(recordName),
		StartRecordType: route53types.RRTypeA,
		MaxItems:        int32Ptr(1),
	})
	if err != nil {
		return nil, err
	}
	values := []string{}
	for _, rs := range out.ResourceRecordSets {
		if aws.ToString(rs.Name) != recordName || rs.Type != route53types.RRTypeA {
			continue
		}
		for _, rr := range rs.ResourceRecords {
			values = append(values, aws.ToString(rr.Value))
		}
	}
	return values, nil
}

// fqdn joins the short hostname and the zone name.
func fqdn(name, zone string) string {
	if zone == "" {
		return name
	}
	if len(zone) > 0 && zone[len(zone)-1] == '.' {
		return name + "." + zone[:len(zone)-1] + "."
	}
	return name + "." + zone + "."
}

// int64Ptr is the numeric pointer helper.
func int64Ptr(v int) *int64 { return &[]int64{int64(v)}[0] }

// int32Ptr is the Route53 MaxItems helper.
func int32Ptr(v int32) *int32 { return &v }
