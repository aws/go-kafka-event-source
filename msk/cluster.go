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

/*
Package msk provides an MskCluster which is compatible with the [github.com/aws/go-kafka-event-source/streams.Cluster] interface.
GKES is a non-proprietray library and using [MSK] is not required. This package is provided as a convenience for those
who are using [MSK].

Disclaimer: github.com/aws/go-kafka-event-source/msk is not maintained or endorsed by the MSK development team. It is maintained by the developers od GKES.
If you have issues with GKES->MSK connectivity, or would like new GKES->MSK features, https://github.com/aws/go-kafka-event-source is the place to ask first.

[MSK]: https://aws.amazon.com/msk/
*/
package msk

import (
	// "crypto/tls"
	// "fmt"
	// "strings"

	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	kaws "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type MskClient interface {
	ListClusters(context.Context, *kafka.ListClustersInput, ...func(*kafka.Options)) (*kafka.ListClustersOutput, error)
	GetBootstrapBrokers(context.Context, *kafka.GetBootstrapBrokersInput, ...func(*kafka.Options)) (*kafka.GetBootstrapBrokersOutput, error)
}

type AuthType int

const (
	None AuthType = iota
	MutualTLS
	SaslScram
	SaslIam
	PublicMutualTLS
	PublicSaslScram
	PublicSaslIam
)

// An implementation of [github.com/aws/go-kafka-event-source/streams.Cluster].
type MskCluster struct {
	clusterName   string
	client        MskClient
	authType      AuthType
	tlsConfig     *tls.Config
	awsConfig     aws.Config
	scram         scram.Auth
	clientOptions []kgo.Opt
}

// Returns the default AWS client config with default region of `region`. DefaultClientConfig panics on errors.
func DefaultClientConfig(region string) aws.Config {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithDefaultRegion(region))
	if err != nil {
		panic(err)
	}
	return cfg
}

// Creates a new MskCluster using DefaultClientConfig. If you're application is running in EC2/ECS Task or Lambda, this is likely the initializer you need.
// See [Sasl IAM support] if using SaslIAm/PublicSaslIam AuthType. [Look here to see how to custom client SDK options], such as a custom Rery mechanism.
// Note: your application's IAM role will need access to the 'ListClusters' and 'GetBootstrapBrokers' calls for your MSK Cluster.
//
// [Look here to see how to custom client SDK options]: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/retry#pkg-examples
// [Sasl IAM support]: https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html
func NewMskCluster(clusterName string, authType AuthType, region string, optFns ...func(*kafka.Options)) *MskCluster {
	return NewMskClusterWithClientConfig(clusterName, authType, DefaultClientConfig(region), optFns...)
}

// Creates a new MskCluster using the specified awsConfig. If you are using STS for authentication, you will likely need to create your own AWS config.
// If you are running on some sort of managed container like EC2/ECS Task or Lambda, you can likely use [NewMskCluster] instead.
// Note: your application's IAM role will need access to the 'ListClusters' and 'GetBootstrapBrokers' calls for your MSK Cluster.
func NewMskClusterWithClientConfig(clusterName string, authType AuthType, awsConfig aws.Config, optFns ...func(*kafka.Options)) *MskCluster {
	return &MskCluster{
		clusterName: clusterName,
		authType:    authType,
		awsConfig:   awsConfig,
		client:      kafka.NewFromConfig(awsConfig, optFns...),
	}
}

// Used primarily for MutualTLS authentication. If you need any configuration beyond the certificate itself, or simply switch on TLS,
// you'll need to use WithClientOptions instead. See WithClientOptions for an example
//
//	cluster := msk.NewMskCluster("MyCluster", msk.MutualTLS, "us-east-1").WithTlsConfig(myMutualTlsConfig)
func (c *MskCluster) WithTlsConfig(tlsConfig *tls.Config) *MskCluster {
	c.tlsConfig = tlsConfig
	return c
}

// Used to supply additional kgo client options. Caution: Options supplied here will override any set by MskCluster.
// This call replaces any client options previously set. Usage:
//
//	cluster := msk.NewMskCluster("MyCluster", msk.MutualTLS, "us-east-1").WithClientOptions(
//		kgo.Dialer((&tls.Dialer{Config: tlsConfig, NetDialer: &net.Dialer{KeepAlive: 20 * time.Minute}}).DialContext))
func (c *MskCluster) WithClientOptions(opts ...kgo.Opt) *MskCluster {
	c.clientOptions = opts
	return c
}

// WithScramUserPass is used to set user/password info for SaslScram/PublicSaslScram auth types.
// This package does not provide for Scram credential rotation:
//
//	cluster := msk.NewMskCluster("MyCluster", msk.SaslScram, "us-east-1").WithScramUserPass("super", "secret")
func (c *MskCluster) WithScramUserPass(user, pass string) *MskCluster {
	c.scram = scram.Auth{
		User: user,
		Pass: pass,
	}
	return c
}

// Called by GKES when intiializing Kafka clients. The MskClluster will call ListClusters with a ClusterNameFilter (using cluster.clusterName)
// to rertieve the ARN for your specific cluster. Once the arn is retrieved, GetBootstrapBrokers will be called and the appropriate
// broker addresses for the specified authType will be used to seed the underlying kgo.Client
func (c *MskCluster) Config() (opts []kgo.Opt, err error) {
	brokers, err := c.getBootstrapBrokers()
	if len(brokers) > 0 {
		opts = append(opts, kgo.SeedBrokers(brokers...))
	}
	if c.tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(c.tlsConfig))
	}
	switch c.authType {
	case SaslIam, PublicSaslIam:
		opts = append(opts, kgo.SASL(kaws.ManagedStreamingIAM(c.saslIamAuth)))
	case SaslScram, PublicSaslScram:
		// MSK only supports SHA512
		opts = append(opts, kgo.SASL(c.scram.AsSha512Mechanism()))
	}
	opts = append(opts, c.clientOptions...)
	return
}

// provides the IAM auth mechanism from using aws.Config CredentialsProvider, so as sessions expire, we should be ok.
func (c *MskCluster) saslIamAuth(ctx context.Context) (auth kaws.Auth, err error) {
	var creds aws.Credentials
	if creds, err = c.awsConfig.Credentials.Retrieve(ctx); err != nil {
		auth = kaws.Auth{
			AccessKey:    creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
		}
	}
	return
}

// fetches broker urls from MSK API and returns the correct list based on AuthType
func (c *MskCluster) getBootstrapBrokers() (brokers []string, err error) {
	var arn string
	var res *kafka.GetBootstrapBrokersOutput
	if arn, err = c.getClusterArn(); err != nil {
		return
	}
	if res, err = c.client.GetBootstrapBrokers(context.TODO(), &kafka.GetBootstrapBrokersInput{
		ClusterArn: aws.String(arn),
	}); err != nil {
		return
	}
	bootstrapString := *res.BootstrapBrokerString
	switch c.authType {
	case MutualTLS:
		bootstrapString = *res.BootstrapBrokerStringTls
	case SaslScram:
		bootstrapString = *res.BootstrapBrokerStringSaslScram
	case SaslIam:
		bootstrapString = *res.BootstrapBrokerStringSaslIam
	case PublicMutualTLS:
		bootstrapString = *res.BootstrapBrokerStringPublicTls
	case PublicSaslScram:
		bootstrapString = *res.BootstrapBrokerStringPublicSaslScram
	case PublicSaslIam:
		bootstrapString = *res.BootstrapBrokerStringPublicSaslIam
	}
	brokers = strings.Split(bootstrapString, ",")
	return
}

func (c *MskCluster) getClusterArn() (arn string, err error) {
	var res *kafka.ListClustersOutput
	if res, err = c.client.ListClusters(context.TODO(), &kafka.ListClustersInput{
		ClusterNameFilter: aws.String(c.clusterName),
	}); err != nil {
		return
	}
	if len(res.ClusterInfoList) == 0 {
		err = fmt.Errorf("cluster not found: %s", c.clusterName)
		return
	}
	ci := res.ClusterInfoList[0]
	if ci.ClusterArn == nil {
		err = fmt.Errorf("cluster not found (nil ClusterInfo): %s", c.clusterName)
		return
	}
	arn = *ci.ClusterArn
	return
}
