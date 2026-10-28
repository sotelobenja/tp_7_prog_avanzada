const { MongoClient } = require('mongodb');
const { mongoUri } = require('./config');

let client;
let db;

async function connectMongo() {
  if (!client) {
    client = new MongoClient(mongoUri, {
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 5000
    });
    await client.connect();
    db = client.db('rutasur');
  }

  return db;
}

async function withCollection(collectionName, callback) {
  const database = await connectMongo();
  const collection = database.collection(collectionName);
  return callback(collection);
}

module.exports = {
  connectMongo,
  withCollection
};
