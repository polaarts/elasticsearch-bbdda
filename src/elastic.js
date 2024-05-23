import { Client } from "@elastic/elasticsearch";

export const client = new Client({
    node: process.env.ELASTIC_ENDPOINT,
    auth: {
        apiKey: process.env.API_KEY
    }
});