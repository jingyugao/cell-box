// Package objectstorage owns Cellbox's durable objects. Local directories are caches only.
package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var ErrNotFound = errors.New("object not found")
var ErrConflict = errors.New("object changed")
var ErrUnavailable = errors.New("object storage request failed")

type Config struct {
	Endpoint        string `json:"endpoint,omitempty"`
	Bucket          string `json:"bucket"`
	Region          string `json:"region,omitempty"`
	Prefix          string `json:"prefix,omitempty"`
	PathStyle       bool   `json:"pathStyle,omitempty"`
	AccessKeyEnv    string `json:"accessKeyEnv,omitempty"`
	SecretKeyEnv    string `json:"secretKeyEnv,omitempty"`
	SessionTokenEnv string `json:"sessionTokenEnv,omitempty"`
}

func (c *Config) Validate() error {
	if c.Bucket == "" || strings.ContainsAny(c.Bucket, "/\\\x00\r\n") {
		return errors.New("objectStorage.bucket is required")
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
			return errors.New("objectStorage.endpoint must be an HTTP(S) origin")
		}
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.Prefix == "" {
		c.Prefix = "cellbox"
	}
	c.Prefix = strings.TrimSuffix(c.Prefix, "/")
	if c.Prefix == "" || strings.HasPrefix(c.Prefix, "/") || path.Clean(c.Prefix) != c.Prefix || strings.ContainsAny(c.Prefix, "\\\x00\r\n") || c.Prefix == "." || c.Prefix == ".." || strings.HasPrefix(c.Prefix, "../") {
		return errors.New("objectStorage.prefix must be a nonempty relative object prefix")
	}
	if (c.AccessKeyEnv == "") != (c.SecretKeyEnv == "") {
		return errors.New("objectStorage credential environment names must be paired")
	}
	return nil
}

// Objects keys are relative to Config.Prefix. Condition is empty for normal
// writes, * for create-only, or a quoted ETag for compare-and-swap writes.
type Objects interface {
	Get(context.Context, string) (io.ReadCloser, int64, string, error)
	List(context.Context, string) ([]string, error)
	Put(context.Context, string, io.ReadSeeker, int64, string) (string, error)
	Delete(context.Context, string) error
	DeleteIfMatch(context.Context, string, string) error
	DeletePrefix(context.Context, string) error
}

type Client struct {
	s3             *s3.Client
	bucket, prefix string
}

func New(ctx context.Context, c Config) (*Client, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(c.Region)}
	if c.AccessKeyEnv != "" {
		access, secret := os.Getenv(c.AccessKeyEnv), os.Getenv(c.SecretKeyEnv)
		if access == "" || secret == "" {
			return nil, errors.New("object storage credentials are missing")
		}
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(access, secret, os.Getenv(c.SessionTokenEnv))))
	}
	config, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, errors.New("cannot initialize object storage credentials")
	}
	client := s3.NewFromConfig(config, func(o *s3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
		o.UsePathStyle = c.PathStyle
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &Client{s3: client, bucket: c.Bucket, prefix: c.Prefix + "/"}, nil
}
func (c *Client) key(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || path.Clean(key) != key || key == "." || key == ".." || strings.HasPrefix(key, "../") || strings.ContainsAny(key, "\\\x00\r\n") {
		return "", errors.New("invalid object key")
	}
	return c.prefix + key, nil
}
func storageError(err error) error {
	if err == nil {
		return nil
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) && apiError.ErrorCode() == "NoSuchBucket" {
		return ErrUnavailable
	}
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) {
		switch response.HTTPStatusCode() {
		case http.StatusNotFound:
			return ErrNotFound
		case http.StatusPreconditionFailed, http.StatusConflict:
			return ErrConflict
		}
	}
	// SDK errors can include signed URLs or endpoint details. Keep errors generic.
	return ErrUnavailable
}
func (c *Client) Get(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	key, err := c.key(key)
	if err != nil {
		return nil, 0, "", err
	}
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, 0, "", storageError(err)
	}
	return out.Body, aws.ToInt64(out.ContentLength), aws.ToString(out.ETag), nil
}

// List returns keys relative to Config.Prefix. The requested prefix is treated
// as a directory boundary, so "images/a" cannot include "images/ab".
func (c *Client) List(ctx context.Context, prefix string) ([]string, error) {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix != "" && (strings.HasPrefix(prefix, "/") || path.Clean(prefix) != prefix || prefix == "." || prefix == ".." || strings.HasPrefix(prefix, "../") || strings.ContainsAny(prefix, "\\\x00\r\n")) {
		return nil, errors.New("invalid object prefix")
	}
	fullPrefix := c.prefix
	if prefix != "" {
		fullPrefix += prefix + "/"
	}
	pager := s3.NewListObjectsV2Paginator(c.s3, &s3.ListObjectsV2Input{Bucket: aws.String(c.bucket), Prefix: aws.String(fullPrefix)})
	keys := make([]string, 0)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, storageError(err)
		}
		for _, item := range page.Contents {
			fullKey := aws.ToString(item.Key)
			if !strings.HasPrefix(fullKey, fullPrefix) || !strings.HasPrefix(fullKey, c.prefix) {
				return nil, errors.New("object listing escaped requested prefix")
			}
			keys = append(keys, strings.TrimPrefix(fullKey, c.prefix))
		}
	}
	return keys, nil
}

func (c *Client) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, condition string) (string, error) {
	key, err := c.key(key)
	if err != nil {
		return "", err
	}
	input := &s3.PutObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key), Body: body, ContentLength: aws.Int64(size)}
	if condition == "*" {
		input.IfNoneMatch = aws.String("*")
	} else if condition != "" {
		input.IfMatch = aws.String(condition)
	}
	if condition == "" {
		// Multipart keeps checkpoint size independent of S3's single-PUT limit.
		upload := transfermanager.New(c.s3, func(o *transfermanager.Options) { o.PartSizeBytes = 16 << 20; o.Concurrency = 2 })
		out, err := upload.UploadObject(ctx, &transfermanager.UploadObjectInput{Bucket: input.Bucket, Key: input.Key, Body: body, ContentLength: input.ContentLength})
		if err != nil {
			return "", storageError(err)
		}
		return aws.ToString(out.ETag), nil
	}
	out, err := c.s3.PutObject(ctx, input, func(o *s3.Options) { o.RetryMaxAttempts = 1 })
	if err != nil {
		return "", storageError(err)
	}
	if aws.ToString(out.ETag) == "" {
		return "", errors.New("object storage did not return an ETag")
	}
	return aws.ToString(out.ETag), nil
}
func (c *Client) Delete(ctx context.Context, key string) error {
	key, err := c.key(key)
	if err != nil {
		return err
	}
	_, err = c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)})
	return storageError(err)
}

func (c *Client) DeleteIfMatch(ctx context.Context, key, etag string) error {
	key, err := c.key(key)
	if err != nil {
		return err
	}
	if etag == "" {
		return errors.New("object ETag is required for conditional delete")
	}
	_, err = c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key), IfMatch: aws.String(etag)})
	return storageError(err)
}

func (c *Client) DeletePrefix(ctx context.Context, prefix string) error {
	key, err := c.key(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return err
	}
	pager := s3.NewListObjectsV2Paginator(c.s3, &s3.ListObjectsV2Input{Bucket: aws.String(c.bucket), Prefix: aws.String(key + "/")})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return storageError(err)
		}
		objects := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, item := range page.Contents {
			objects = append(objects, types.ObjectIdentifier{Key: item.Key})
		}
		if len(objects) == 0 {
			continue
		}
		out, err := c.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(c.bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)}})
		if err != nil {
			return storageError(err)
		}
		if len(out.Errors) > 0 {
			return fmt.Errorf("object storage could not delete %d objects", len(out.Errors))
		}
	}
	return nil
}
