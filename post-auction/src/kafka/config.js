const dotenv = require("dotenv");

dotenv.config();

function hasKafkaBrokers() {
  return Boolean(process.env.KAFKA_BROKERS);
}

function getKafkaGroupId() {
  return process.env.KAFKA_GROUP_ID;
}

function buildKafkaConfig() {
  return {
    clientId: process.env.KAFKA_CLIENT_ID,
    brokers: [process.env.KAFKA_BROKERS],
    ssl: true,
    sasl: {
      mechanism: "plain",
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD
    },
    // Fail fast instead of the kafkajs defaults (5 retries / 30s timeout) so an
    // unhealthy broker cannot pile up long-running publish/consume attempts.
    connectionTimeout: 5000,
    requestTimeout: 8000,
    retry: {
      retries: 3,
      initialRetryTime: 300,
      maxRetryTime: 5000
    }
  };
}

module.exports = {
  hasKafkaBrokers,
  getKafkaGroupId,
  buildKafkaConfig
};
