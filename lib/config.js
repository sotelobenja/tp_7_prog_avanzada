require('dotenv').config();

const config = {
  kafkaBrokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',').map((item) => item.trim()).filter(Boolean),
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/rutasur',
  appName: process.env.APP_NAME || 'rutasur-app'
};

module.exports = config;
