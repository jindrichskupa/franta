// Package auth maps a cluster's config to franz-go client options.
package auth

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"franta/internal/config"
)

// Build returns the franz-go options implementing the cluster's TLS and SASL
// settings. The IAM SASL callback resolves AWS credentials lazily at connect.
func Build(c config.Cluster) ([]kgo.Opt, error) {
	var opts []kgo.Opt
	if c.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	switch c.Auth.Type {
	case "", "plaintext":
		// no SASL
	case "plain":
		if c.Auth.Username == "" {
			return nil, errors.New("plain auth: username required")
		}
		pw, err := c.Auth.ResolvePassword()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(plain.Auth{User: c.Auth.Username, Pass: pw}.AsMechanism()))
	case "scram":
		if c.Auth.Username == "" {
			return nil, errors.New("scram auth: username required")
		}
		pw, err := c.Auth.ResolvePassword()
		if err != nil {
			return nil, err
		}
		a := scram.Auth{User: c.Auth.Username, Pass: pw}
		switch c.Auth.Mechanism {
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(a.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(a.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("scram auth: unsupported mechanism %q", c.Auth.Mechanism)
		}
	case "iam":
		region, profile := c.Auth.Region, c.Auth.Profile
		opts = append(opts, kgo.SASL(aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
			var loadOpts []func(*awsconfig.LoadOptions) error
			if region != "" {
				loadOpts = append(loadOpts, awsconfig.WithRegion(region))
			}
			if profile != "" {
				loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(profile))
			}
			cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
			if err != nil {
				return aws.Auth{}, fmt.Errorf("load aws config: %w", err)
			}
			cr, err := cfg.Credentials.Retrieve(ctx)
			if err != nil {
				return aws.Auth{}, fmt.Errorf("retrieve aws credentials: %w", err)
			}
			return aws.Auth{
				AccessKey:    cr.AccessKeyID,
				SecretKey:    cr.SecretAccessKey,
				SessionToken: cr.SessionToken,
			}, nil
		})))
	default:
		return nil, fmt.Errorf("unknown auth type %q", c.Auth.Type)
	}
	return opts, nil
}
