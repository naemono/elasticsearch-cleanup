package elasticsearch

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"regexp"

	"github.com/avast/retry-go"
	esv8 "github.com/elastic/go-elasticsearch/v8"
)

const (
	oneGBinBytes = 1024 * 1024 * 1024
)

var indexPattern = regexp.MustCompile(`^\.ds\-(?P<Index>[a-zA-Z0-9\-\_\.]+)\-(?P<Date>\d\d\d\d\.\d\d\.\d\d\-\d{6})$`)

// Config is the configuration for the Elasticsearch Client
type Config struct {
	URL          string
	Username     string
	Password     string
	DisableSSL   bool
	MaxSizeBytes int
}

// Client is the client that controls the Elasticsearch data ingestion
type Client struct {
	Config
	V8Client *esv8.TypedClient
}

// New creates a new Elasticsearch typed client from given Config
func New(config Config) (*Client, error) {
	esv8Config := esv8.Config{
		Addresses: []string{config.URL},
		Username:  config.Username,
		Password:  config.Password,
	}
	if config.DisableSSL {
		esv8Config.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}
	client, err := esv8.NewTypedClient(esv8Config)
	if err != nil {
		return nil, fmt.Errorf("while creating client: %w", err)
	}
	if config.MaxSizeBytes == 0 {
		config.MaxSizeBytes = oneGBinBytes
	}
	return &Client{
		Config:   config,
		V8Client: client,
	}, nil
}

// Go will attempt to rollover indices that are larger than the MaxSizeBytes
func (c *Client) Go(ctx context.Context) error {
	res, err := c.V8Client.Indices.Stats().Index("_all").Do(ctx)
	if err != nil {
		return fmt.Errorf("while getting indices: %w", err)
	}
	var indicesRolledOver int
	for index, stats := range res.Indices {
		if stats.Total.Store.SizeInBytes > int64(c.MaxSizeBytes) {
			match := indexPattern.FindStringSubmatch(index)
			if len(match) == 0 {
				fmt.Printf("Index %s does not match pattern, skipping\n", index)
				continue
			}
			paramsMap := map[string]string{}
			for i, name := range indexPattern.SubexpNames() {
				paramsMap[name] = match[i]
			}
			fmt.Printf("About to rollover Index: %s, Size: %d\n", index, stats.Total.Store.SizeInBytes)
			retry.Do(func() error {
				if _, err := c.V8Client.Indices.Rollover(paramsMap["Index"]).Do(ctx); err != nil {
					return fmt.Errorf("while rolling over %s index: %w", paramsMap["Index"], err)
				}
				return nil
			}, retry.Attempts(10))

			retry.Do(func() error {
				if _, err := c.V8Client.Indices.Delete(index).Do(ctx); err != nil {
					return fmt.Errorf("while deleting %s index: %w", index, err)
				}
				return nil
			}, retry.Attempts(10))
			indicesRolledOver++
		}
	}
	if indicesRolledOver == 0 {
		fmt.Println("No indices to rollover")
		return nil
	}

	return c.rerouteRetryShards(ctx)
}

func (c *Client) rerouteRetryShards(ctx context.Context) error {
	_, err := c.V8Client.Cluster.Reroute().Do(ctx)
	if err != nil {
		return fmt.Errorf("while attempting rerouting of unassigned shards: %w", err)
	}
	return nil
}
