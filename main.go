package main

import (
	"bufio"
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"google.golang.org/api/iterator"
	"os"
	"strings"
)

func toCamelCase(s string) string {
	parts := strings.Split(s, "_")
	c := cases.Title(language.Und)
	for i := 1; i < len(parts); i++ {
		parts[i] = c.String(parts[i])
	}
	return strings.Join(parts, "")
}

func main() {
	var (
		projectID    string
		outputPath   string
		query        string
		unsafe       string
		numberPrefix string
	)

	var rootCmd = &cobra.Command{
		Use:   "bigquery-to-json",
		Short: "Fetch BigQuery records and write them as JSON",
		RunE: func(cmd *cobra.Command, args []string) error {
			if query == "" {
				stat, _ := os.Stdin.Stat()
				if (stat.Mode() & os.ModeCharDevice) == 0 {
					scanner := bufio.NewScanner(os.Stdin)
					var sb strings.Builder
					for scanner.Scan() {
						sb.WriteString(scanner.Text() + "\n")
					}
					if err := scanner.Err(); err != nil {
						return fmt.Errorf("failed to read from stdin: %w", err)
					}
					query = sb.String()
				}
			}

			return BQuery(
				projectID,
				query,
				outputPath,
				unsafe == "true",
				numberPrefix == "true")
		},
	}

	rootCmd.Flags().StringVarP(&projectID, "project", "p", "", "GCP Project ID (required)")
	rootCmd.Flags().StringVarP(&outputPath, "output", "o", "output.txt", "Path to the output file")
	rootCmd.Flags().StringVarP(&unsafe, "unsafe", "u", "false", "Ignore no LIMIT N in query")
	rootCmd.Flags().StringVarP(&numberPrefix, "numberPrefix", "n", "false", "Adds N: { before JSON")

	rootCmd.Flags().StringVar(&query, "query", "", "Custom BigQuery SQL query (overrides table, column, and value)")
	_ = rootCmd.MarkFlagRequired("project")
	_ = rootCmd.MarkFlagRequired("query")
	_ = rootCmd.MarkFlagRequired("output")
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type SBigQuery struct {
	client *bigquery.Client
}

func BQuery(
	projectID string,
	query string,
	outputPath string,
	unsafe bool,
	numberPrefix bool) error {

	if query == "" {
		fmt.Println("Empty query. Nothing to do.")
		return nil
	}
	if !strings.Contains(query, "LIMIT") && !strings.Contains(query, "limit") {
		if !unsafe {
			fmt.Println("Query does not contain LIMIT or limit. Use --unsafe flag.")
		}
	}
	var results []map[string]bigquery.Value
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		fmt.Printf("Error creating BigQuery client: %v\n", err)
		return err
	}
	defer func(client *bigquery.Client) {
		_ = client.Close()
	}(client)

	srv := &SBigQuery{
		client: client,
	}
	q := srv.client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("unable to read query: %v", err)
	}
	var row map[string]bigquery.Value
	for {
		itErr := it.Next(&row)
		if errors.Is(itErr, iterator.Done) {
			break
		}
		if itErr != nil {
			return fmt.Errorf("unable to iterate through query results: %v", itErr)
		}

		camelCaseRow := make(map[string]bigquery.Value)
		for key, value := range row {
			camelCaseRow[toCamelCase(key)] = value
		}

		results = append(results, camelCaseRow)
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	for idx, result := range results {
		jsonData, jsonErr := json.Marshal(result)
		if jsonErr != nil {
			return fmt.Errorf("failed to marshal row %d: %v", idx, jsonErr)
		}

		var fileErr error
		if numberPrefix {
			_, fileErr = file.WriteString(fmt.Sprintf("%s\n", string(jsonData)))
		} else {
			_, fileErr = file.WriteString(fmt.Sprintf("%d: %s\n", idx+1, string(jsonData)))
		}
		if fileErr != nil {
			return fmt.Errorf("failed to write row %d to file: %v", idx, jsonErr)
		}
	}
	fmt.Printf("Data written to %s\n", outputPath)
	return nil
}
