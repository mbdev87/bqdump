# bqdump
Dump BQ query result to JSON

# Example usage 

```
Fetch BigQuery records and write them as JSON

Usage:
  bigquery-to-json [flags]

Flags:
  -h, --help                  help for bigquery-to-json
  -n, --numberPrefix string   Adds N: { before JSON (default "false")
  -o, --output string         Path to the output file (default "output.txt")
  -p, --project string        GCP Project ID (required)
      --query string          Custom BigQuery SQL query (overrides table, column, and value)
  -u, --unsafe string         Ignore no LIMIT N in query (default "false")

```
Command
```
go run main.go --project=my-project-2137 --query='SELECT * FROM `my-project-2137.app_logs.app_logs_table` WHERE job_id=2412282254 LIMIT 10'
```


# Queries with no LIMIT 

If query does not contain `LIMIT` you need to use `--unsafe=true` Otherwise it will result in error: 

`Query does not contain LIMIT or limit. Use --unsafe flag.` 
