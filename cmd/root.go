/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/naemono/elasticsearch-cleanup/pkg/elasticsearch"
)

const (
	oneGBinBytes = 1024 * 1024 * 1024
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "cleanup [-U https://localhost:9200 [-u username] -p password [-d true] [-m 104857600]",
	Short:   "cleanup large ES indices",
	Long:    `Cleanup large indices within an Elasticsearch instance.`,
	PreRunE: preRun,
	RunE:    run,
}

var (
	maxSizeBytes int
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringP("username", "u", "elastic", "Elasticsearch username")
	rootCmd.Flags().StringP("password", "p", "", "Elasticsearch password")
	rootCmd.Flags().StringP("url", "U", "https://localhost:9200", "Elasticsearch URL")
	rootCmd.Flags().BoolP("disable-ssl", "d", true, "Whether to disable ssl verification")
	rootCmd.Flags().IntVarP(&maxSizeBytes, "max-size-bytes", "m", oneGBinBytes, "Max size of index in bytes")
}

func run(cmd *cobra.Command, args []string) error {
	disableSSL, err := strconv.ParseBool(cmd.Flag("disable-ssl").Value.String())
	if err != nil {
		return fmt.Errorf("while parsing disable-ssl flag: %w", err)
	}
	client, err := elasticsearch.New(elasticsearch.Config{
		URL:          cmd.Flag("url").Value.String(),
		Username:     cmd.Flag("username").Value.String(),
		Password:     cmd.Flag("password").Value.String(),
		MaxSizeBytes: maxSizeBytes,
		DisableSSL:   disableSSL,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	return client.Go(ctx)
}

func preRun(cmd *cobra.Command, args []string) error {
	if cmd.Flag("password").Value.String() == "" {
		return fmt.Errorf("password cannot be empty")
	}
	if _, err := url.Parse(cmd.Flag("url").Value.String()); err != nil {
		return fmt.Errorf("Elasticsearch url parse failed: %s", err)
	}
	return nil
}
