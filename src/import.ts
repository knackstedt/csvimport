import fs from "fs";
import { parse } from '@fast-csv/parse';

import Surreal from 'surrealdb';
import yoctoSpinner from 'yocto-spinner';

const SURREAL_URL = "ws://127.0.0.1:8000";
const SURREAL_USERNAME = "root";
const SURREAL_PASSWORD = "root";
const database = "test";
const namespace = "test";

// If not specified, each record in the CSV file will be 
const targetDatabase = "epa_dmr";

(async() => {

    const db = new Surreal();
    
    const spinner = yoctoSpinner({ text: 'Connecting to Database…' }).start();
    
    await db.connect(SURREAL_URL);
    await db.signin({
        username: SURREAL_USERNAME,
        password: SURREAL_PASSWORD
    });
    await db.use({
        database,
        namespace,
    });

    console.log(
        spinner.text = "Finding CSV files…"
    );
    const dirFiles = fs.readdirSync("./data");
    const csvFiles = dirFiles.filter(f => f.endsWith(".csv"));
    
    for (let i = 0; i < csvFiles.length; i++) {
        const csvFilename = csvFiles[i];
    
        console.log(
            spinner.text = "Reading CSV file " + csvFilename + "…"
        );

        // Limit the buffer chunks to approx. 512K
        const fileStream = fs.createReadStream('./data/' + csvFilename, { highWaterMark: 512 * 1024 });
        const insertPromises = [];
        let counter = 0;

        
        await new Promise((res, rej) => {
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

                spinner.text = "Imported [" + counter.toLocaleString() + "] rows from " + csvFilename + " [" + insertPromises.length.toString().padEnd(3, ' ') + "] queued";
            });


            parser.on("data", function(data) {
                data['_source'] = csvFilename;
                let p = db.insert(targetDatabase || csvFilename, data)
                    .then(function() { counter++ });

                // Record the promise and purge from the promises array
                insertPromises.push(p);
                p.finally(function() { insertPromises.splice(insertPromises.indexOf(p), 1) });

                if (insertPromises.length > 20) {
                    fileStream.pause();
                }
            })
            dataStream.on("end", () => {
                console.log("Imported [" + counter.toLocaleString() + "] records for " + csvFilename);
                clearInterval(_i);

                // Let all promises settle before we import the next file.
                Promise.all(insertPromises)
                    .then(() => res(0))
            })
        });
    }

    process.exit(0);
})();
