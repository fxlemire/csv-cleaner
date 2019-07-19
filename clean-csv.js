#!/usr/bin/env node

const program = require('commander');
const parse = require('csv-parse');
const stringify = require('csv-stringify');
const fs = require('fs');
const path = require('path');
const ProgressBar = require('progress');
const { Transform } = require('stream');
const { promisify } = require('util');

const mkdir = promisify(fs.mkdir);
const readdir = promisify(fs.readdir);
const stat = promisify(fs.stat);

const DESCRIPTION = `
A script to remove columns or entries that only contain null values (e.g. null, empty strings, 0).

The filtered files are saved inside a \`filtered\` folder within the provided folder path.

How to use: node clean-csv.js <path_to_folder_containing_csv_files>
`;

let FOLDER_READ_PATH = '';
const FOLDER_WRITE_PATH = 'filtered';

/**
 * Returns an object where each key represents a file name, and each value is an array of the column names to be kept
 */
const findColumns = async () => {
  console.log(`Reading all csv files in ${FOLDER_READ_PATH}...`);
  const files = await readdir(FOLDER_READ_PATH);
  const columnsToKeep = {};

  const fileParsingPromises = files.map(
    (fileName) =>
      new Promise(async (resolve, reject) => {
        let fileStats;

        try {
          fileStats = await stat(path.resolve(path.join(FOLDER_READ_PATH, fileName)));
        } catch (err) {
          console.error(`An error occurred when reading file ${fileName}`, err);
          reject(err);
          return;
        }

        if (fileStats.isDirectory() || fileName.split('.').pop() !== 'csv') {
          resolve();
          return;
        }

        const fileStream = fs.createReadStream(path.resolve(FOLDER_READ_PATH, fileName));
        const parser = parse({
          columns: true,
          trim: true,
        });
        columnsToKeep[fileName] = [];

        const findColumns = new Transform({
          objectMode: true,
          transform(entry, encoding, cb) {
            Object.keys(entry).forEach((columnName) => {
              const isEmpty = [null, 0, '', undefined, 'NULL', '0'].includes(entry[columnName]);
              if (!isEmpty) {
                const isColumnAlreadyStored = columnsToKeep[fileName].some((v) => v === columnName);

                if (!isColumnAlreadyStored) {
                  columnsToKeep[fileName].push(columnName);
                }
              }
            });

            this.push(entry);
            cb();
          },
        });

        fileStream.pipe(parser).pipe(findColumns);

        findColumns.on('error', (err) => {
          console.log('An error occurred', err);
          reject(err);
        });

        findColumns.on('data', () => {});

        findColumns.on('end', () => {
          console.log(`Finished parsing file ${fileName}`);
          resolve();
        });
      }),
  );

  await Promise.all(fileParsingPromises);

  return columnsToKeep;
};

const filterCsvs = async (columnsToKeep) => {
  const bar = new ProgressBar('[:bar] :current/:total -- ', {
    complete: '=',
    incomplete: ' ',
    total: Object.keys(columnsToKeep).length,
    width: 20,
  });

  const files = await readdir(FOLDER_READ_PATH);

  try {
    await mkdir(path.resolve(path.join(FOLDER_READ_PATH, FOLDER_WRITE_PATH)));
  } catch (error) {
    if (error.code !== 'EEXIST') {
      console.error('Could not create `filtered` folder', error);
    }
  }

  const fileWritePromises = files.map(
    (fileName) =>
      new Promise(async (resolve, reject) => {
        let fileStats;

        try {
          fileStats = await stat(path.resolve(path.join(FOLDER_READ_PATH, fileName)));
        } catch (err) {
          console.error(`An error occurred when reading file ${fileName}`, err);
          reject(err);
          return;
        }

        if (fileStats.isDirectory() || fileName.split('.').pop() !== 'csv') {
          resolve();
          return;
        }

        const fileStream = fs.createReadStream(path.resolve(path.join(FOLDER_READ_PATH, fileName)));
        const parser = parse({
          columns: true,
          trim: true,
        });
        const stringifier = stringify({
          header: true,
          delimiter: ',',
        });
        const filterEmpty = new Transform({
          objectMode: true,
          transform(entry, encoding, cb) {
            const newEntry = columnsToKeep[fileName].reduce((acc, columnName) => {
              return {
                ...acc,
                [columnName]: entry[columnName],
              };
            }, {});

            const isNotEmpty = Object.values(newEntry).some(
              (value) => ![null, 0, '', undefined, 'NULL', '0'].includes(value),
            );

            if (isNotEmpty) {
              this.push(newEntry);
            }
            cb();
          },
        });

        const writeStream = fs.createWriteStream(
          path.resolve(path.join(FOLDER_READ_PATH, FOLDER_WRITE_PATH, fileName)),
        );

        writeStream.on('finish', () => {
          bar.tick();
          console.log(`Finished writing file ${FOLDER_WRITE_PATH}/${fileName}`);
          resolve();
        });

        fileStream
          .pipe(parser)
          .pipe(filterEmpty)
          .pipe(stringifier)
          .pipe(writeStream);
      }),
  );

  return Promise.all(fileWritePromises);
};

if (require.main === module) {
  program
    .version('1.0.0', '-v, --version')
    .usage('[options] <csvFolderPath>')
    .on('--help', () => console.log(DESCRIPTION))
    .parse(process.argv);

  (async () => {
    FOLDER_READ_PATH = path.resolve(path.join('./', program.args[0]));

    if (!FOLDER_READ_PATH) {
      console.error('You must specify a path as argument');
      process.exit(1);
    }

    try {
      const columnsToKeep = await findColumns();
      await filterCsvs(columnsToKeep);
      console.log('done!');
    } catch (error) {
      console.error('Error: ', error);
      process.exit(1);
    }

    process.exit(0);
  })();
}
