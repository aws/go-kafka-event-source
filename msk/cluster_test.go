// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package msk

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
)

type mockMskCluster struct {
	listOutput   *kafka.ListClustersOutput
	brokerOutput *kafka.GetBootstrapBrokersOutput
	listErr      error
	brokerErr    error
}

func (m mockMskCluster) ListClusters(context.Context, *kafka.ListClustersInput, ...func(*kafka.Options)) (*kafka.ListClustersOutput, error) {
	return m.listOutput, m.listErr
}

func (m mockMskCluster) GetBootstrapBrokers(context.Context, *kafka.GetBootstrapBrokersInput, ...func(*kafka.Options)) (*kafka.GetBootstrapBrokersOutput, error) {
	return m.brokerOutput, m.brokerErr
}

func TestClusterReturnsErrorOnNilBootsrapBrokers(t *testing.T) {
	m := mockMskCluster{
		listOutput: &kafka.ListClustersOutput{
			ClusterInfoList: []types.ClusterInfo{
				{ClusterName: aws.String("test"), ClusterArn: aws.String("arn")},
			},
		},
		brokerOutput: &kafka.GetBootstrapBrokersOutput{},
	}
	c := &MskCluster{
		clusterName: "test",
		authType:    SaslIam,
		client:      m,
	}

	_, err := c.Config()
	if err == nil {
		t.Error("expected error")
	}
}

func TestClusterReturnsErrorOnMskListFailure(t *testing.T) {
	m := mockMskCluster{
		listErr: errors.New("error"),
	}
	c := &MskCluster{
		clusterName: "test",
		authType:    SaslIam,
		client:      m,
	}

	_, err := c.Config()
	if err == nil {
		t.Error("expected error")
	}

}

func TestClusterReturnsErrorOnMskBootstrapBrokersFailure(t *testing.T) {
	m := mockMskCluster{
		listOutput: &kafka.ListClustersOutput{
			ClusterInfoList: []types.ClusterInfo{
				{ClusterName: aws.String("test"), ClusterArn: aws.String("arn")},
			},
		},
		brokerErr: errors.New("error"),
	}
	c := &MskCluster{
		clusterName: "test",
		authType:    SaslIam,
		client:      m,
	}

	_, err := c.Config()
	if err == nil {
		t.Error("expected error")
	}

}

func TestClusterSuccess(t *testing.T) {
	m := mockMskCluster{
		listOutput: &kafka.ListClustersOutput{
			ClusterInfoList: []types.ClusterInfo{
				{ClusterName: aws.String("test"), ClusterArn: aws.String("arn")},
			},
		},
		brokerOutput: &kafka.GetBootstrapBrokersOutput{
			BootstrapBrokerStringSaslIam: aws.String("a,b,c"),
		},
	}
	c := &MskCluster{
		clusterName: "test",
		authType:    SaslIam,
		client:      m,
	}

	opts, err := c.Config()
	if err != nil {
		t.Error(err)
	}
	if len(opts) == 0 {
		t.Error("no options built")
	}

	if len(opts) != len(c.builtOptions) {
		t.Error("no options saved for reuse")
	}
}
