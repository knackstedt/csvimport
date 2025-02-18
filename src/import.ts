import fs from "fs";
import crypto from "crypto";
import { parse } from '@fast-csv/parse';

import Surreal from 'surrealdb';
import yoctoSpinner from 'yocto-spinner';

const SURREAL_URL = process.env["SURREAL_URL"] || "ws://127.0.0.1:8000";
const SURREAL_USERNAME = process.env["SURREAL_USERNAME"] || "root";
const SURREAL_PASSWORD = process.env["SURREAL_PASSWORD"] || "root";
const database = process.env["SURREAL_TARGET_DATABASE"] || "test";
const namespace = process.env["SURREAL_TARGET_NAMESPACE"] || "test";


// If not specified, each record in the CSV file will be 
const targetTablename = process.env["CSV_IMPORT_TABLENAME"] || "";
const injectSourceTableAsField = true;
const sourceTableField = "_source"
const sendBellBetweenFiles = true;
const clearTableBeforeInsert = false;
const embedSrcRowSha = false; // embed the source record's md5/sha sum (for signature verification...?)
const rowHashAlgorithm = "sha256"; // checksum when `embedSrcRowSha` is provided.
const checksumField = "_sha"; // Column name in the table for the checksum field.
const deduplicate = true; // de-duplicate records based on their shasum. TODO.


// List of fields to index during the import
// https://surrealdb.com/docs/surrealql/statements/define/indexes
const indexFields: {
    indexName: string,
    fields: string,
    isUnique: boolean
}[] = [{
    indexName: "paramDesc",
    fields: "parameterDesc",
    isUnique: false
}];

function countNewlines(csvFilename) {
    const fileStream = fs.createReadStream('./data/' + csvFilename, { highWaterMark: 512 * 1024 });
    let count = 0;
    const td = new TextDecoder();
    return new Promise<number>((res, rej) => {
        fileStream.on("data", data => {
            const text = td.decode(data as Buffer);
            count += text.match(/\n/g).length;
        })
        fileStream.on("end", () => {
            res(count);
        })
    })
}

const printTime = (duration: number) => {
    let milliseconds = Math.floor((duration % 1000) / 100);
    let seconds = Math.floor((duration / 1000) % 60);
    let minutes = Math.floor((duration / (1000 * 60)) % 60);
    let hours = Math.floor((duration / (1000 * 60 * 60)) % 24);

    hours = (hours < 10) ? 0 + hours : hours;
    minutes = (minutes < 10) ? 0 + minutes : minutes;
    seconds = (seconds < 10) ? 0 + seconds : seconds;

    return (hours ? (hours + "h ") : '') + minutes + "m " + seconds + "." + milliseconds + "s";
}

const BOLD = "\x1b[1m";
const RESET = "\x1b[0m";
const ITALICIZE = "\x1b[3m";
const UNDERLINE = "\x1b[4m";

const RED = RESET + "\x1b[31m";
const GREEN = RESET + BOLD + "\x1b[32m";
const YELLOW = RESET + "\x1b[33m";
const BLUE = RESET + "\x1b[34m";
const MAGENTA = RESET + "\x1b[35m";
const CYAN = RESET + BOLD + "\x1b[36m";
const WHITE = RESET + "\x1b[37m";
const GRAY = RESET + "\x1b[30m";

const spinner = yoctoSpinner({});
const start = Date.now();
let totalInsertions = 0;
let csvFiles: string[];

(async() => {
    const db = new Surreal();

    console.log(GRAY +"Connecting to Surreal…");

    await db.connect(SURREAL_URL);
    await db.signin({
        username: SURREAL_USERNAME,
        password: SURREAL_PASSWORD
    });
    await db.use({
        database,
        namespace,
    });

    console.log("Finding CSV files…");
    const dirFiles = fs.readdirSync("./data");
    csvFiles = dirFiles.filter(f => f.endsWith(".csv"));
    console.log("Found " + CYAN + csvFiles.length.toLocaleString() + GRAY + " CSV files");
    
    for (let i = 0; i < csvFiles.length; i++) {
        const csvFilename = csvFiles[i];
        const table = targetTablename || csvFilename;
        const fileStart = Date.now();
        let targetLineCount = 0;
        const insertPromises = [];
        let rowsInserted = 0;

        try {
    
            if (clearTableBeforeInsert) {
                await db.query(`DELETE \`${table}\``);
            }
    
            await db.query(`DEFINE TABLE IF NOT EXISTS \`${table}\` SCHEMALESS`);
    
            // Prepare indexes before we do the mass import
            for (let { indexName, fields, isUnique } of indexFields) {
                await db.query(`DEFINE INDEX IF NOT EXISTS ${indexName} ON \`${table}\` FIELDS ${fields} ${isUnique ? 'UNIQUE' : ''}`);
            }
    
            console.log("Reading CSV file " + CYAN + csvFilename + GRAY + "…");
            targetLineCount = await countNewlines(csvFilename);
            console.log("Found " + GREEN + (targetLineCount -1).toLocaleString() + GRAY + " records in " + CYAN + csvFilename + RESET);
    
            spinner.start();
    
            // Limit the buffer chunks to approx. 512K
            // Larger values cause the program to lock momentarily as the data is read.
            const fileStream = fs.createReadStream('./data/' + csvFilename, { highWaterMark: 512 * 1024 });
    
            const parser = parse({
                delimiter: ",",
                headers: headers => headers.map(h => h.toLowerCase().replace(/_([a-z])/g, (match, g) => g.toUpperCase()))
            });
            const dataStream = fileStream.pipe(parser);
    
            let _i = setInterval(() => {
                // Prevent adding more than 200 insert promises at any given time
                if (insertPromises.length > 20) {
                    fileStream.pause();
                }
                else {
                    fileStream.resume();
                }
    
                const progress = rowsInserted / targetLineCount;
                const barWidth = 
                    process.stdout.columns > 103 ? 40 : 20;
                const lChars = Math.ceil(progress * barWidth);
                const rChars = barWidth - lChars;
                const totalTimeElapsed = (Date.now() - start);
                const timeElapsed = (Date.now() - fileStart);
                const timeEstimate = timeElapsed / progress;
                const timeEstimateRemaining = timeEstimate - timeElapsed;
    
                const speed = Math.floor(rowsInserted / (timeElapsed/1000));
    
                spinner.text = [
                    `[${YELLOW}${printTime(totalTimeElapsed)}(${printTime(timeElapsed)})${RESET}]`,
                    CYAN + ''.padEnd(lChars, "━") + GRAY + ''.padEnd(rChars, "━"),
                    BLUE + (progress*100).toFixed(2) + '%',
                    GREEN + rowsInserted.toLocaleString() + "/" + CYAN + targetLineCount.toLocaleString() + GRAY + "rows",
                    RED + speed + "/s",
                    GRAY + "eta",
                    printTime(timeEstimateRemaining) + GRAY
                ].join(" ");
            }, 50);
    
    
            parser.on("data", (data) => {
                // Generate a shasum of the record -- this can be used as the primary key instead of 
                // a random one generated upon insertion.
                if (embedSrcRowSha) {
                    const checksum = crypto.hash(rowHashAlgorithm, JSON.stringify(data));
                    data[checksumField] = checksum;
                }
                if (injectSourceTableAsField) {
                    data[sourceTableField] = csvFilename;
                }
                
                let p = db.insert(table, data)
                    .then(function () { rowsInserted++; totalInsertions++ });
    
                // Record the promise and purge from the promises array
                insertPromises.push(p);
                p.finally(() => { insertPromises.splice(insertPromises.indexOf(p), 1) });
    
                if (insertPromises.length > 20) {
                    fileStream.pause();
                }
            })
            parser.on("error", err => {
                console.error(err);
            })
    
            await new Promise((res, rej) => {
                dataStream.on("end", () => {
                    spinner.stop();
                    console.log([
                        "Successfully Imported",
                        GREEN + rowsInserted.toLocaleString() + GRAY,
                        "of",
                        CYAN + targetLineCount.toLocaleString(),
                        BLUE + (rowsInserted / targetLineCount * 100).toFixed(2) + '%',
                        GRAY + "records from",
                        '"' + CYAN + csvFilename + GRAY +'"',
                        "in",
                        YELLOW + printTime(Date.now() - start)
                    ].join(" "));
                    clearInterval(_i);
    
                    // Let all promises settle before we import the next file.
                    Promise.all(insertPromises)
                        .then(() => res(0))
                })
            });
    
            if (sendBellBetweenFiles) {
                // Send a terminal bell event. (probably doesn't work for windows)
                // https://stackoverflow.com/questions/8557624/how-i-trigger-the-system-bell-in-nodejs
                process.stdout.write('\u0007');
            }
        }
        catch(ex) {
            console.error(ex);
            console.error([
                "Failed to complete import of " + RED + csvFilename,
                GRAY + "Inserted " + GREEN + rowsInserted + GRAY,
                "rows of",
                CYAN + targetLineCount.toLocaleString() + GRAY + " total",
                BLUE + (rowsInserted / targetLineCount * 100).toFixed(2) + '%',
                GRAY + "records from",
                '"' + CYAN + csvFilename + GRAY + '"',
                "in",
                YELLOW + printTime(Date.now() - fileStart)
            ].join(" "));
            throw ex;
        }
    }

    process.exit(0);
})().catch(err => {
    console.error([
        "Bulk insertion job " + RED + "FAILED.\n",
        GRAY + "Inserted " + GREEN + totalInsertions + GRAY,
        "From " + CYAN + csvFiles + GRAY + "total files.\n",
        "In " + YELLOW + printTime(Date.now() - start)
    ].join(" "));
    process.exit(1);
});
