// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package metered

// Code generated by gowrap. DO NOT EDIT.
// template: ../../templates/metered.tmpl
// gowrap: http://github.com/hexdigest/gowrap

import (
	"context"

	"go.uber.org/yarpc"

	"github.com/uber/cadence/client/sharddistributor"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
)

// sharddistributorClient implements sharddistributor.Client interface instrumented with retries
type sharddistributorClient struct {
	client        sharddistributor.Client
	metricsClient metrics.Client
}

// NewShardDistributorClient creates a new instance of sharddistributorClient with retry policy
func NewShardDistributorClient(client sharddistributor.Client, metricsClient metrics.Client) sharddistributor.Client {
	return &sharddistributorClient{
		client:        client,
		metricsClient: metricsClient,
	}
}

func (c *sharddistributorClient) GetShardOwner(ctx context.Context, gp1 *types.GetShardOwnerRequest, p1 ...yarpc.CallOption) (gp2 *types.GetShardOwnerResponse, err error) {
	c.metricsClient.IncCounter(metrics.ShardDistributorClientGetShardOwnerScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.ShardDistributorClientGetShardOwnerScope, metrics.CadenceClientLatency)
	gp2, err = c.client.GetShardOwner(ctx, gp1, p1...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.ShardDistributorClientGetShardOwnerScope, metrics.CadenceClientFailures)
	}
	return gp2, err
}
