import { client } from "./elastic.js";
import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';

async function csvToJsonStream(csvFilePath, outputFolder) {
    return new Promise((resolve, reject) => {
        const results = [];
        const fileName = path.basename(csvFilePath, '.csv');
        const jsonFilePath = path.join(outputFolder, `music-${fileName}.json`);
        
        if (!fs.existsSync(outputFolder)) {
            fs.mkdirSync(outputFolder);
        }
        
        fs.createReadStream(csvFilePath)
            .pipe(csv({ separator: '|', trim: true }))
            .on('data', (data) => results.push(data))
            .on('end', () => {
                fs.writeFileSync(jsonFilePath, JSON.stringify(results, null, 4));
                console.log(`Archivo JSON '${jsonFilePath}' creado con éxito.`);
                resolve();
            })
            .on('error', (error) => reject(error));
    });
}

const csvFolder = './songs';
const outputFolder = './json';


function getJsonFileNamesWithPrefix(jsonFolder) {
    try {
        const fileNames = fs.readdirSync(jsonFolder);
        const jsonFileNames = fileNames
            .filter(fileName => fileName.endsWith('.json'))
            .map(fileName => path.basename(fileName, '.json'));
        return jsonFileNames;
    } catch (error) {
        console.error('Error al obtener los nombres de los archivos JSON:', error.message);
        return [];
    }
}

const getSongData = async (jsonFolder, fileName) => {
    try {
        const jsonFilePath = path.join(jsonFolder, `${fileName}.json`);
        const jsonArray = JSON.parse(fs.readFileSync(jsonFilePath, 'utf8'));
        return jsonArray;
    } catch (error) {
        console.error('Error al leer el archivo JSON:', error.message);
        return [];
    }
}

fs.readdir(csvFolder, async (err, files) => {
    if (err) {
        console.error('Error al leer la carpeta:', err);
        return;
    }

    const csvFiles = files.filter(file => file.endsWith('.csv'));
    
    for (const csvFile of csvFiles) {
        const csvFilePath = path.join(csvFolder, csvFile);
        await csvToJsonStream(csvFilePath, outputFolder);
    }

    const jsonFiles = getJsonFileNamesWithPrefix(outputFolder);

    for (const element of jsonFiles) {
        const songData = await getSongData(outputFolder, element);
        await client.helpers.bulk({
            datasource: songData,
            pipeline: 'ent-search-generic-ingestion',
            onDocument: (doc) => ({index: {_index: element}})
        });
        console.log(`Índice '${element}' LLENADO con éxito.`);
    }
});