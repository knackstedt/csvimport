import fs from 'fs';
import readline from "readline";
import { Spinner } from 'yocto-spinner';
import { CYAN, GRAY, GREEN } from './ansi';

/**
 * Async scan LFS CSV files and count the occurrences of newline markers.
 * 
 * TODO: Are there other delimiters that may come into play for CSV files?
 */
export const countNewlines = (csvFilename: string, spinner: Spinner, terminalRefreshRate: number) => {
    const fileStream = fs.createReadStream('./data/' + csvFilename, { highWaterMark: 512 * 1024 });
    let count = 0;

    const td = new TextDecoder();
    return new Promise<number>((res, rej) => {
        const _i = setInterval(() => {
            spinner.text = `${GRAY}Scanning ${GREEN}${(count - 1).toLocaleString()}${GRAY} entries in ${CYAN}${csvFilename}`;

        }, terminalRefreshRate)
        fileStream.on("data", data => {
            const text = td.decode(data as Buffer);
            count += text.match(/[\r\n]/g).length;
        })
        fileStream.on("end", () => {
            clearInterval(_i);
            res(count);
        })
    })
}

/**
 * Print an millisecond duration as a human readable time
 * e.g. 65000 => 1m 5.0s
 */
export const printTime = (duration: number) => {
    let milliseconds = Math.floor((duration % 1000) / 100);
    let seconds = Math.floor((duration / 1000) % 60);
    let minutes = Math.floor((duration / (1000 * 60)) % 60);
    let hours = Math.floor((duration / (1000 * 60 * 60)) % 24);

    hours = (hours < 10) ? 0 + hours : hours;
    minutes = (minutes < 10) ? 0 + minutes : minutes;
    seconds = (seconds < 10) ? 0 + seconds : seconds;

    return (hours ? (hours + "h ") : '') + minutes + "m " + seconds + "." + milliseconds + "s";
}

/**
 * Calculate the mathematical "mode" of an array
 * e.g. [1,1,1,1,1,1,2,2,2,3,3,3,4,4,4] => 1
 */
const calculateMode = (array: any[]) => {
    const map = new Map<any, number>();
    let maxCount = 0;
    let result = null;

    for (let i = 0; i < array.length; i++) {
        const item = array[i];
        const count = (map.get(item) || 0) + 1;
        map.set(item, count);

        if (count > maxCount) {
            maxCount = count;
            result = item;
        }
    }

    return result;
}

/**
 * Perform a basic line scan of the first 10 lines to fingerprint the delimiter of the CSV file
 */
export const fingerPrintCSV = (csvFilename: string, linesToScan = 10) => {
    const fileStream = fs.createReadStream('./data/' + csvFilename, { highWaterMark: 512 * 1024 });

    return new Promise<string>((res, rej) => {        
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity // To handle different line endings (CRLF or LF)
        });

        // Count the number of symbol occurrences in the line, return the largest count of matches
        const fingerPrintLine = (line: string) => {
            // Here we detect the delimiter and update the headings
            const semicolons = line.match(/;/g)?.length || 0;
            const commas = line.match(/,/g)?.length || 0;
            const tabs = line.match(/\t/g)?.length || 0;
            const pipes = line.match(/\|/g)?.length || 0;

            return [
                { val: semicolons, key: ";" },
                { val: commas, key: "," },
                { val: tabs, key: "\t" },
                { val: pipes, key: "|" }
            ].sort((a, b) => b.val - a.val)[0].key;
        }

        let lineNo = 1;
        let detectedDelims = [];
        rl.addListener("line", line => {
            detectedDelims.push(fingerPrintLine(line));
            if (lineNo++ > linesToScan) {
                fileStream.close();
                rl.close();

                // Return the most common delim that we detected.
                res(calculateMode(detectedDelims));
            }
        });
    })
}