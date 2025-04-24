# SurrealDB Mass CSV import tool

This tool exists to facilitate importing huge volumes of CSV data into SurrealDB, running stable past 10 hour jobs.
It should support 10+GB CSV files by doing partial reads and minimizing the data stored in memory. Under testing it keeps below
2GB in most scenarios.

> This has initially been tested with a 140GB dataset of data from the United States EPA division.

## Screenshot

![importing.gif](./assets/importing.gif)


## Prerequisites
 - Install either Bun(>= 1.2.2) or NodeJS (>= v22).
 - Run `npm install` or `bun install` for your respective installation.
 <!-- - (optional) run `npm run build` -->
 - Place your CSV files into the `data/` folder.
 - Run the program from a terminal and overload variables as needed.

> Note: Bun is nearly 2x faster at loading records into the database.
> In our tests, we found node can handle 8-10K records per second, while Bun
> can handle around 18-20k records per second.

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
If left blank data will be inserted into tables .
### CSV_CLEAR_TABLE_BEFORE_INSERT
Set to `true` to clear tables before records are inserted. Default `false`.
### CSV_TERMINAL_REFRESH_INTERVAL
The integer amount of time between display refresh calls. Defaults to 50(ms).
### CSV_BELL_BETWEEN_FILES
Set to `true` to send ANSI bell packets after each file completes it's import. Defaults to `false`.
### CSV_BELL_ON_FINISH
Set to `true` to send ANSI bell packets after each file completes it's import. Defaults to `false`.
### CSV_COUNT_RECORDS_BEFORE_INSERT
Set to `false` to disable scanning all CSV files before import. Disabling this will break the overall time estimate. Defaults to `true`. 
### CSV_INJECT_SOURCE_AS_FIELD
Set to `false` to disable writing the source CSV filename to the created record. Defaults to `true`.
### CSV_SOURCE_TABLE_FIELDNAME
If `CSV_INJECT_SOURCE_AS_FIELD` is enabled, the column name that the CSV filename will be written to. Defaults to `_source`.
### CSV_READER_HIGHWATERMARK
The target memory chunk limit in bytes of the CSV file stream reader. Values above 524288 may cause noticeable stuttering in the terminal output stream. Should be at least 2x the byte size of the largest record to import (This prevents stream read stagnation). Defaults to `524288`
### CSV_CALCULATE_RECORD_CHECKSUM
Set to `false` to disable checksum processing of imported CSV records. Defaults to `true`.
### CSV_CHECKSUM_ALGORITHM
When `CSV_CALCULATE_RECORD_CHECKSUM` is enabled, this specifies the hashing algorithm used. Algorithm must be installed on the system and can be verified by running `node -p "require('crypto').getHashes().join('\n')"`. Defaults to `sha256`.
### CSV_CHECKSUM_COLUMN_NAME
The column name that the calculated checksum will be inserted into. If `CSV_USE_CHECKSUM_AS_ID` is set to `true` this will create an additional column. Defaults to `_sha`.
### CSV_USE_CHECKSUM_AS_ID
When checksums of records are calculated, Set to `false` to disable setting the record ID based on a checksum of of the record data. Defaults to `true`.

## Roadmap

 - [X] Enable record fingerprinting and deduplication
 - [ ] Enable resume upon partial import
 - [ ] Enable failed record logging
 - [ ] Enable custom key transformers
 - [ ] Enable custom value transformers

## Contributing

PRs are welcome. Yes the codebase is a bit chaotic at the moment. I plan to rewrite this with something like enquirer to provide a much more friendly interface while at the same time keeping direct CLI support. 

## License

