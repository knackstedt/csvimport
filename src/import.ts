import fs from "fs/promises";
import { createReadStream } from "fs";
import crypto from "crypto";

import Surreal from 'surrealdb';
import { parse } from '@fast-csv/parse';
import yoctoSpinner from 'yocto-spinner';

import { countNewlines, fingerPrintCSV, printTime } from './utils';
import { BLUE, CYAN, GRAY, GREEN, RED, RESET, YELLOW } from './ansi';

const SURREAL_URL = process.env["SURREAL_URL"] || "ws://127.0.0.1:8000";
const SURREAL_USERNAME = process.env["SURREAL_USERNAME"] || "root";
const SURREAL_PASSWORD = process.env["SURREAL_PASSWORD"] || "root";
const database = process.env["SURREAL_TARGET_DATABASE"] || "test1";
const namespace = process.env["SURREAL_TARGET_NAMESPACE"] || "test";


// If not specified, each record in the CSV file will be 
const targetTablename = process.env["CSV_IMPORT_TABLENAME"] || "";
const injectSourceTableAsField: boolean = JSON.parse(process.env['CSV_INJECT_SOURCE_AS_FIELD']?.toLowerCase() || 'true');
const sourceTableField = process.env['CSV_SOURCE_TABLE_FIELDNAME'] || "_source"
const sendBellBetweenFiles: boolean = JSON.parse(process.env['CSV_BELL_BETWEEN_FILES']?.toLowerCase() || 'false');
const sendBellAtCompletion: boolean = JSON.parse(process.env['CSV_BELL_ON_FINISH']?.toLowerCase() || 'false');
const clearTableBeforeInsert: boolean = JSON.parse(process.env['CSV_CLEAR_TABLE_BEFORE_INSERT']?.toLowerCase() || 'false');
const terminalRefreshRate = parseInt(process.env['CSV_TERMINAL_REFRESH_INTERVAL'] || '50');
const countRecordsToInsertAtStart: boolean = JSON.parse(process.env['CSV_COUNT_RECORDS_BEFORE_INSERT']?.toLowerCase() || 'true');

// Target number of bytes to read on the CSV file. 
// Should be enough bytes to include 2 lines at any given time.
// default 512k
// larger values **will** cause the program to hang during read operations.
const highWaterMark = parseInt(process.env['CSV_READER_HIGHWATERMARK']) || (512 * 1024);

const calculateChecksum: boolean = JSON.parse(process.env['CSV_CALCULATE_RECORD_CHECKSUM']?.toLowerCase() || 'true');
// Algorithm to use when `calculateChecksum` is enabled.
const rowHashAlgorithm = process.env['CSV_CHECKSUM_ALGORITHM'] || "sha256"; 
// MUST have calculateChecksum enabled.
const checksumField = process.env['CSV_CHECKSUM_COLUMN_NAME'] || "_sha"; // Column name in the table for the checksum field.
// de-duplicate records based on their shasum. This makes their ID a hash of their data.
// MUST have calculateChecksum enabled.
const useChecksumAsId: boolean = JSON.parse(process.env['CSV_USE_CHECKSUM_AS_ID']?.toLowerCase() || 'true');


// Marginally increase insert rate at the cost of memory. 
// defaults to 40.
const maxInsertPromises = 40;

// List of fields to index during the import
// https://surrealdb.com/docs/surrealql/statements/define/indexes
const indexFields: {
    indexName: string,
    fields: string,
    isUnique: boolean
}[] = [];
// [{
//     indexName: "paramDesc",
//     fields: "parameterDesc SEARCH ANALYZER ascii BM25 HIGHLIGHTS CONCURRENTLY",
//     isUnique: false
// }];



const spinner = yoctoSpinner({});
let start: number;
let totalInsertions = 0;
let csvFiles: {
    path: string,
    entries: number
}[];
let targetInsertionsForDataset: number;

(async() => {
    const db = new Surreal();

    console.log(GRAY + "Connecting to Surreal…");

    await db.connect(SURREAL_URL);
    await db.signin({
        username: SURREAL_USERNAME,
        password: SURREAL_PASSWORD
    });
    await db.use({
        database,
        namespace,
    });

    await db.query(`DEFINE NAMESPACE IF NOT EXISTS \`${namespace}\``);
    await db.query(`DEFINE DATABASE IF NOT EXISTS \`${database}\``);

    const processStart = Date.now();

    spinner.start();
    spinner.text = "Scanning for CSV files…";
    const filesInDataDir = await fs.readdir("./data");
    const csvFilesInDataDir = filesInDataDir.filter(f => f.endsWith(".csv"));
    spinner.stop();

    console.log(`${GRAY}Found ${GREEN}${csvFilesInDataDir.length.toLocaleString()}${GRAY} CSV files`);

    if (countRecordsToInsertAtStart) {
        // Count all of the records in the entire folder before starting the import 
        csvFiles = [];
        for (let i = 0; i < csvFilesInDataDir.length; i++) {
            const fn = csvFilesInDataDir[i];
            spinner.start();
            const lines = (await countNewlines(fn, spinner, terminalRefreshRate)) - 1;
            spinner.stop();
            console.log(`${CYAN}(${(i+1).toLocaleString()}/${csvFilesInDataDir.length.toLocaleString()}) ${GRAY}Found ${GREEN}${lines.toLocaleString()}${GRAY} records in ${CYAN}${fn}${GRAY}`);

            csvFiles.push({
                path: fn,
                entries: lines
            });
        }
        targetInsertionsForDataset = csvFiles.map(c => c.entries).reduce((a, b) => a + b, 0);

        console.log(`Found ${GREEN}${targetInsertionsForDataset.toLocaleString()}${GRAY} total records in ${CYAN}${csvFilesInDataDir.length.toLocaleString()}${GRAY} CSV files`);
    }
    else {
        csvFiles = await Promise.all(csvFilesInDataDir.map(async fn => {
            return {
                path: fn,
                entries: null
            };
        }));
    }

    
    for (let i = 0; i < csvFiles.length; i++) {
        const filename = csvFiles[i].path;
        const tableName = targetTablename || filename;
        const fileImportStarttime = Date.now();
        const insertPromises = [];
        let targetInsertionsForFile = 0;
        let totalRowsInsertedForFile = 0;

        try {
            
            if (clearTableBeforeInsert) {
                await db.query(`DELETE \`${tableName}\``);
            }
    
            await db.query(`DEFINE TABLE IF NOT EXISTS \`${tableName}\` SCHEMALESS`);
    
            // Prepare indexes before we do the mass import
            for (let { indexName, fields, isUnique } of indexFields) {
                await db.query(`DEFINE INDEX IF NOT EXISTS ${indexName} ON \`${tableName}\` FIELDS ${fields} ${isUnique ? 'UNIQUE' : ''}`);
            }
    
            if (!countRecordsToInsertAtStart) {
                console.log(`${GRAY}Reading CSV file ${CYAN}${filename}${GRAY}…`);
                targetInsertionsForFile = (await countNewlines(filename, spinner, terminalRefreshRate)) - 1;
                console.log(`${GRAY}Found ${GREEN}${targetInsertionsForFile.toLocaleString()}${GRAY} records in ${CYAN}${filename}${RESET}`);
            }
            else {
                targetInsertionsForFile = csvFiles[i]?.entries;
                console.log(`${GRAY}Found ${GREEN}${targetInsertionsForFile.toLocaleString()}${GRAY} records in ${CYAN}${filename}${RESET}`);
            }
    
            spinner.start();
            const csvDelimiter = await fingerPrintCSV(filename);

            // Limit the buffer chunks to approx. 512K
            // Larger values cause the program to lock momentarily as the data is read.
            const fileStream = createReadStream('./data/' + filename, { highWaterMark });
            const parser = parse({
                delimiter: csvDelimiter,
                headers: headers => headers.map(h => h.toLowerCase().replace(/_([a-z])/g, (match, g) => g.toUpperCase()))
            });
            fileStream.pipe(parser);
    
            let _i = setInterval(() => {
                // Prevent adding more than 200 insert promises at any given time
                if (insertPromises.length > maxInsertPromises) {
                    fileStream.pause();
                }
                else {
                    fileStream.resume();
                }
    
                const totalProgress = totalInsertions / targetInsertionsForDataset;
                const totalBarWidth = process.stdout.columns > 103 ? 40 : 20;
                const totalLChars = Math.floor(totalProgress * totalBarWidth);
                const totalRChars = totalBarWidth - totalLChars;
                const totalTimeElapsed = (Date.now() - processStart);
                const totalTimeEstimate = totalTimeElapsed / totalProgress;
                const totalTimeEstimateRemaining = totalTimeEstimate - totalTimeElapsed;
                const totalSpeed = Math.floor(totalInsertions / (totalTimeElapsed / 1000));

                const fileProgress = totalRowsInsertedForFile / targetInsertionsForFile;
                const barWidth = process.stdout.columns > 103 ? 40 : 20;
                const lChars = Math.floor(fileProgress * barWidth);
                const rChars = barWidth - lChars;
                const timeElapsed = (Date.now() - fileImportStarttime);
                const timeEstimate = timeElapsed / fileProgress;
                const timeEstimateRemaining = timeEstimate - timeElapsed;

                const speed = Math.floor(totalRowsInsertedForFile / (timeElapsed/1000));
                
                spinner.text = [
                    GRAY + `Job:`,
                    `${YELLOW}[${printTime(totalTimeElapsed)}]`,
                    CYAN + ''.padEnd(totalLChars, "━") + GRAY + ''.padEnd(totalRChars, "━"),
                    BLUE + (totalProgress * 100).toFixed(2) + '%',
                    GREEN + totalInsertions.toLocaleString() + "/" + CYAN + targetInsertionsForDataset.toLocaleString() + GRAY + "rows",
                    RED + totalSpeed.toLocaleString() + "/s",
                    GRAY + "eta",
                    printTime(totalTimeEstimateRemaining) + GRAY
                ].join(" ") +
                '\n' + 
                [
                    "File: " + YELLOW + `  ${printTime(timeElapsed)}`,
                    CYAN + ''.padEnd(lChars, "━") + GRAY + ''.padEnd(rChars, "━"),
                    BLUE + (fileProgress*100).toFixed(2) + '%',
                    GREEN + totalRowsInsertedForFile.toLocaleString() + "/" + CYAN + targetInsertionsForFile.toLocaleString() + GRAY + "rows",
                    RED + speed.toLocaleString() + "/s",
                    GRAY + "eta",
                    printTime(timeEstimateRemaining) + GRAY
                ].join(" ");
            }, terminalRefreshRate);
    
            parser.on("data", (data) => {
                // Generate a shasum of the record -- this can be used as the primary key instead of 
                // a random one generated upon insertion.
                if (calculateChecksum) {
                    const checksum = crypto.hash(rowHashAlgorithm, JSON.stringify(data));
                    data[checksumField] = checksum;

                    if (useChecksumAsId) {
                        data.id = checksum;
                    }
                }
                if (injectSourceTableAsField) {
                    data[sourceTableField] = filename;
                }

                // const p = db.upsert(tableName, data)
                //     .then(() => {
                //         totalRowsInsertedForFile++;
                //         totalInsertions++;
                //     });
                const p = db.insert(tableName, [data])
                    .then(() => {
                        totalRowsInsertedForFile++;
                        totalInsertions++;
                    });
    
                // Record the promise and purge from the promises array
                insertPromises.push(p);
                p.finally(() => { insertPromises.splice(insertPromises.indexOf(p), 1) });
    
                // If we start getting more promises than our target limit, we'll
                // pause the source data stream until we've dropped below our threshold.
                if (insertPromises.length > maxInsertPromises) {
                    fileStream.pause();
                }
            });

            // This doesn't appear to ever fire.
            parser.on("error", err => {
                console.error(err);
            });
    
            // Wait for the data stream to complete
            await new Promise<void>((res, rej) => {
                // datastream.on("end") ?
                // Do we need to explicitly call something to tell the parser it's done with it's work?
                parser.on("end", async () => {
                    // Let all promises settle before we finalize the import.
                    await Promise.all([...insertPromises]);

                    clearInterval(_i);
                    spinner.stop();

                    console.log([
                        "Successfully Imported",
                        GREEN + totalRowsInsertedForFile.toLocaleString() + GRAY,
                        "of",
                        CYAN + targetInsertionsForFile.toLocaleString(),
                        BLUE + (totalRowsInsertedForFile / targetInsertionsForFile * 100) + '%',
                        GRAY + "records from",
                        '"' + CYAN + filename + GRAY +'"',
                        "in",
                        YELLOW + printTime(Date.now() - fileImportStarttime) + GRAY
                    ].join(" "));

                    res();
                })
            });
    
            if (sendBellBetweenFiles) {
                // Send a terminal bell event. (probably doesn't work for windows)
                // https://stackoverflow.com/questions/8557624/how-i-trigger-the-system-bell-in-nodejs
                process.stdout.write('\u0007');
            }
        }
        catch(ex) {
            spinner.stop();
            console.error(ex);
            console.error([
                "Failed to complete import of " + RED + filename,
                GRAY + "Inserted " + GREEN + totalRowsInsertedForFile + GRAY,
                "rows of",
                CYAN + targetInsertionsForFile.toLocaleString() + GRAY + " total",
                BLUE + (totalRowsInsertedForFile / targetInsertionsForFile * 100).toFixed(2) + '%',
                GRAY + "records from",
                CYAN + filename + GRAY,
                "in",
                YELLOW + printTime(Date.now() - fileImportStarttime) + GRAY
            ].join(" "));
            throw ex;
        }
    }

    if (sendBellAtCompletion) {
        process.stdout.write('\u0007');
    }

    console.log([
        "Bulk insertion job " + GREEN + "succeeded" + GRAY +".\n",
        GRAY + "Imported " + GREEN + totalInsertions.toLocaleString() + GRAY,
        "of",
        BLUE + (targetInsertionsForDataset ? targetInsertionsForDataset.toLocaleString() : 'unknown') + GRAY,
        "from " + CYAN + csvFiles.length + GRAY + " total files.\n",
        "in " + YELLOW + printTime(Date.now() - processStart) + RESET
    ].join(" "));

    process.exit(0);
})().catch(err => {
    spinner.stop();
    console.error(err);
    console.error([
        "Bulk insertion job " + RED + "failed" + GRAY + ".\n",
        GRAY + "Imported " + GREEN + totalInsertions.toLocaleString() + GRAY,
        "of",
        BLUE + (targetInsertionsForDataset ? targetInsertionsForDataset.toLocaleString() : "unknown") + GRAY,
        "from " + CYAN + csvFiles + GRAY + "total files.\n",
        "in " + YELLOW + printTime(Date.now() - start) + RESET
    ].join(" "));
    process.exit(1);
});
