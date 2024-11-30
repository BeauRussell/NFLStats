import {Database, Statement} from 'sqlite3';
import * as fs from 'fs';
import csv = require('csv-parser');
import { SingleBar, Presets } from 'cli-progress';

const DB_FILE: string = '../../data/nflstats.db';
const RAW_DIRECTORY: string = '../../data/raw';
const PROCESSED_DIRECTORY: string = '../../data/processed';
const bar: SingleBar = new SingleBar({}, Presets.shades_classic);

function writeCSVToDB(csvPath: string, tableName: string): Promise<void> {
    return new Promise((resolve, reject): void => {
        const db: Database = new Database(DB_FILE);

        // Read CSV File
        let csvData: string[] = [];
        fs.createReadStream(csvPath)
            .pipe(csv())
            .on('data', (row: any): void => {
                console.log(row);
                if (csvData.length == 6362) {
                    console.log(row);
                }
                csvData.push(row);
            })
            // .on('end', (): void => {
            //     const columns: string[] = Object.keys(csvData[0]);
            //     const createQuery: string = `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(' TEXT, ')} TEXT)`;
            //     db.run(createQuery, (err: Error): void => {
            //         if (err) {
            //             return reject(err);
            //         }
            //
            //         bar.start(100,0);
            //
            //         // Clean out empty headers, save indexes to remove later
            //         const headers: string[] = [];
            //         const emptyIndexes: number[] = [];
            //         columns.forEach((header: string, index: number) => {
            //             if (header != '') {
            //                 headers.push(header);
            //             } else {
            //                 emptyIndexes.push(index);
            //             }
            //         });
            //
            //         // Insert CSV data into the table
            //         const insertQuery: string = `INSERT INTO ${tableName} (${headers.join(', ')}) VALUES (${headers.map((): string => '?').join(', ')})`;
            //
            //         const stmt: Statement = db.prepare(insertQuery);
            //         csvData.forEach((row: object, rowNum: number): void => {
            //             // Clean row of bad data
            //             const rowValues: string[] = Object.values(row);
            //             emptyIndexes.forEach((index: number): void => {
            //                 rowValues.splice(index, 1);
            //             });
            //             if (rowNum == 6362) {
            //                 console.log(rowValues);
            //             }
            //             bar.update(Math.floor(rowNum / csvData.length * 100));
            //
            //             stmt.run(rowValues, (err: Error): void => {
            //                 if (err) {
            //                     console.log('Failed to insert row value ' + rowNum, err);
            //                     reject(err);
            //                 }
            //             });
            //         });
            //         stmt.finalize((err: Error): void => {
            //             if (err) {
            //                 reject(err);
            //             }
            //             bar.stop();
            //             resolve();
            //         });
            //
            //         db.close();
            //     });
            // })
            .on('error', (err: Error): void => {
                reject(err)
            });
    });
}

fs.promises.readdir(RAW_DIRECTORY)
    .then(async (file_names: string[]): Promise<void> => {
        for (let file_name of file_names) {
            console.log(`Starting file: ${file_name}`);
            const file_path: string = `${RAW_DIRECTORY}/${file_name}`;
            try {
                await writeCSVToDB(file_path, 'plays');
                await fs.promises.rename(file_path, `${PROCESSED_DIRECTORY}/${file_name}`);
            } catch (err) {
                console.log(err);
            }
        }
    });
