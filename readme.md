# SurrealDB Mass CSV import tool

This tool exists to facilitate importing huge volumes of CSV data into SurrealDB, running stable past 10 hour jobs.
It should support 10+GB CSV files by doing partial reads and minimizing the data stored in memory. Under testing it keeps below
2GB in most scenarios.

> This has initially been tested with a 140GB dataset of data from the United States EPA division.

## Prerequisites
 - Install either Bun(>= 1.2.2) or NodeJS (>= v22).
 - Run `npm install` or `bun install` for your respective installation.
 <!-- - (optional) run `npm run build` -->
 - Place your CSV files into the `data/` folder.
 - Run the program from a terminal and overload variables as needed.

e.g. for bun
```shell
SURREAL_URL="127.0.0.1:8000" \
SURREAL_USERNAME="root" \
SURREAL_PASSWORD="root" \
SURREAL_TARGET_NAMESPACE="data-mine" \
SURREAL_TARGET_DATABASE="my-database" \
CSV_IMPORT_TABLENAME="dataset" \
bun run src/import.ts
```

e.g. for node
```shell
SURREAL_URL="127.0.0.1:8000" \
SURREAL_USERNAME="root" \
SURREAL_PASSWORD="root" \
SURREAL_TARGET_NAMESPACE="data-mine" \
SURREAL_TARGET_DATABASE="my-database" \
CSV_IMPORT_TABLENAME="dataset" \
node --lazy -r ts-node/register src/import.ts
```

## Running


## Configuration options

### SURREAL_URL
URL (including protocol and port) to access SurrealDB
### SURREAL_USERNAME
The username used to authenticate against Surreal
### SURREAL_PASSWORD
The password used to authenticate against Surreal
### SURREAL_TARGET_NAMESPACE
The namespace that data will be inserted into. Must be created before the program runs.
### SURREAL_TARGET_DATABASE
The database that data will be inserted into. Must be created before the program runs.
### CSV_IMPORT_TABLENAME
The table that data will be inserted into. If the table doesn't exist a schemaless table will be defined automatically.
If left blank data will be inserted into tables matching the CSV filename.


## Roadmap

 - [ ] Enable record fingerprinting and deduplication
 - [ ] Enable resume upon partial import
 - [ ] Enable failed record logging
 - [ ] Enable custom key transformers
 - [ ] Enable custom value transformers

## Contributing

PRs are welcome. Yes the codebase is a bit chaotic at the moment. I plan to rewrite this with something like enquirer to provide a much more friendly interface while at the same time keeping direct CLI support. 