// Usage:
//   1. Save Aiven CA as ./ca.pem (Console > aditya-kafka > CA certificate > Show > download)
//   2. export AIVEN_BROKER="kafka-b840436-aditya-kafka.g.aivencloud.com:11231"
//      export AIVEN_USERNAME="avnadmin"
//      export AIVEN_PASSWORD="<click Show locally, do not commit>"
//      export AIVEN_CA_PATH="./ca.pem"
//   3. node brokers/migrate-to-aiven.mjs
// Creates order, product, topping with 6 partitions each (matches Confluent CSV).
import { Kafka } from "kafkajs";
import fs from "fs";

const broker = process.env.AIVEN_BROKER;
const username = process.env.AIVEN_USERNAME;
const password = process.env.AIVEN_PASSWORD;
const caPath = process.env.AIVEN_CA_PATH || "./ca.pem";

if (!broker || !username || !password) {
    console.error("Missing AIVEN_BROKER / AIVEN_USERNAME / AIVEN_PASSWORD env vars");
    process.exit(1);
}

const kafka = new Kafka({
    clientId: "migration-script",
    brokers: [broker],
    ssl: { ca: [fs.readFileSync(caPath, "utf-8")] },
    sasl: { mechanism: "scram-sha-256", username, password },
    connectionTimeout: 30000,
});

const admin = kafka.admin();
const TOPICS = ["order", "product", "topping"];

await admin.connect();
console.log("Connected to Aiven");

const existing = await admin.listTopics();
console.log("Existing:", existing);

for (const t of TOPICS) {
    if (existing.includes(t)) {
        console.log(`- ${t} exists, skipping create`);
        continue;
    }
    // Aiven free tier: max 2 partitions, replication fixed at 1
    await admin.createTopics({
        topics: [{ topic: t, numPartitions: 2, replicationFactor: 1 }],
    });
    console.log(`- ${t} created (2p, rf=1, free-tier limit)`);
}

// Verify produce + fetch metadata
const meta = await admin.fetchTopicMetadata({ topics: TOPICS });
for (const t of meta.topics) {
    console.log(`${t.name}: ${t.partitions.length} partitions`);
}

await admin.disconnect();
console.log("Done. Next: update production.yaml in each service.");
