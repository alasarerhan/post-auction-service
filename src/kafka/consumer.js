const { Kafka } = require("kafkajs");
const dotenv = require("dotenv");

const biddingService = require("../domain/bidding.service");
const topics = require("./topics");

dotenv.config();

const brokers = (process.env.KAFKA_BROKERS || "")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

let consumer;
let started = false;

function parseBoolean(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").toLowerCase());
}

function parseMultiline(value) {
  return value ? value.replace(/\\n/g, "\n") : undefined;
}

function buildKafkaConfig() {
  const sslEnabled = process.env.KAFKA_SSL ? parseBoolean(process.env.KAFKA_SSL) : true;
  const username = process.env.KAFKA_SASL_USERNAME;
  const password = process.env.KAFKA_SASL_PASSWORD;
  const ca = parseMultiline(process.env.KAFKA_SSL_CA);
  const cert = parseMultiline(process.env.KAFKA_SSL_CERT);
  const key = parseMultiline(process.env.KAFKA_SSL_KEY);

  let ssl = undefined;
  if (sslEnabled) {
    if (ca || cert || key) {
      ssl = {
        rejectUnauthorized: !parseBoolean(process.env.KAFKA_SSL_REJECT_UNAUTHORIZED_FALSE),
        ca: ca ? [ca] : undefined,
        cert,
        key
      };
    } else {
      ssl = true;
    }
  }

  return {
    clientId: process.env.KAFKA_CLIENT_ID || "auction-bidding-service",
    brokers,
    ssl,
    sasl:
      username && password
        ? {
            mechanism: "plain",
            username,
            password
          }
        : undefined
  };
}

function parseMessage(message) {
  try {
    return JSON.parse(message.value.toString());
  } catch (error) {
    console.error("Failed to parse Kafka message:", error.message);
    return null;
  }
}

async function start() {
  if (!brokers.length || started) {
    if (!brokers.length) {
      console.log("Kafka consumer disabled because KAFKA_BROKERS is not configured.");
    }
    return;
  }

  const kafka = new Kafka(buildKafkaConfig());

  consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || "auction-bidding-service"
  });

  await consumer.connect();
  await consumer.subscribe({
    topics: Object.values(topics.consumedTopics),
    fromBeginning: true
  });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const payload = parseMessage(message);

      if (!payload) {
        return;
      }

      console.log(`Received message on topic ${topic}`);

      try {
        switch (topic) {
          case topics.consumedTopics.USER_BUYER_REGISTERED:
            await biddingService.handleBuyerRegistered(payload);
            break;
          case topics.consumedTopics.CATALOG_BASKET_CREATED:
            await biddingService.handleBasketCreated(payload);
            break;
          case topics.consumedTopics.CATALOG_PUBLISHED:
            await biddingService.handleCatalogPublished(payload);
            break;
          default:
            console.log(`Unhandled topic ${topic}`);
        }
      } catch (error) {
        console.error(`Kafka consumer failed on topic ${topic}:`, error);
      }
    }
  });

  started = true;
  console.log("Kafka consumer started");
}

async function stop() {
  if (consumer && started) {
    await consumer.disconnect();
    started = false;
  }
}

module.exports = {
  start,
  stop
};
