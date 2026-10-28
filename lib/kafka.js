const { Kafka } = require('kafkajs');
const { kafkaBrokers, appName } = require('./config');

const kafka = new Kafka({
  clientId: appName,
  brokers: kafkaBrokers
});

async function createProducer() {
  const producer = kafka.producer();
  await producer.connect();
  return producer;
}

async function createConsumer({ groupId, topics }) {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }
  return consumer;
}

async function sendMessage(topic, key, message) {
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ key, value: JSON.stringify(message) }]
  });
  await producer.disconnect();
}

module.exports = {
  kafka,
  createProducer,
  createConsumer,
  sendMessage
};
