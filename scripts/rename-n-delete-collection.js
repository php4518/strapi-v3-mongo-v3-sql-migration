/* eslint-disable no-await-in-loop */
require('dotenv').config();

const collectionMap = require('../data-jsons/mongo-collection-rename.json')
const mongoDeleteExtras = require('../data-jsons/delete-collections.json')
const forceDeleteCollection = require('../data-jsons/force-delete-collections.json')

const mongo = require('../src/mongo');

const executeCollectionUpdate = () => {

  // Connect to the MongoDB server
  mongo.connect(async (err) => {
    if (err) {
      console.error('Error connecting to MongoDB:', err);
      return;
    }
    console.log("connected to Mongo DB")
    // Database
    const db = mongo.db();
    const successded = [];
    const falied = [];
    try {
    // eslint-disable-next-line no-restricted-syntax, guard-for-in
      for (const i in Object.keys(collectionMap)) {
        const key = Object.keys(collectionMap)[i];
        if (db.collection(key)) {
          try {
          // eslint-disable-next-line no-await-in-loop
            await db.collection(key).rename(collectionMap[key]);
            successded.push(key);
            console.log(`Collection "${key}" has been renamed to "${collectionMap[key]}"`);
          } catch (e) {
            falied.push(key);
            console.error('Error renaming collection:', e);
          }
        }
      }
      // eslint-disable-next-line no-restricted-syntax, guard-for-in
      for (const i in mongoDeleteExtras) {
        const collectionName = mongoDeleteExtras[i];
        const collection = db.collection(collectionName);

        // Count the documents in the collection
        const count = await collection.countDocuments();

        if (count === 0) {
        // Delete the collection
          await collection.drop();
          console.log('Collection deleted because it contains no data.');
        } else {
          console.log('Collection has data. Not deleting.');
        }
      }
      // eslint-disable-next-line no-restricted-syntax, guard-for-in
      for (const i in forceDeleteCollection) {
        const collectionName = forceDeleteCollection[i];
        const collection = db.collection(collectionName);
        // Delete the collection
        await collection.drop();
      }

      console.log('======= FORMATTING DATA in collection =========');
      await db.collection("users-permissions_user").updateMany({firstName: {$eq: null}},[{$set: {firstName: ""}}]);
      await db.collection("users-permissions_user").updateMany({lastName: {$eq: null}},[{$set: {lastName: ""}}]);
      await db.collection("users-permissions_user").updateMany({ phoneNumber: { $gt: 999999999999999 } }, [{ $set: { phoneNumber: 9999999999 } }])
      await db.collection('upload_file').updateMany({width: {$eq: null}, height: {$eq: null}},[{$set: {width: 100, height: 100}}])
      await db.collection("upload_file").updateMany({size: {$type: 'object'}}, [{ $set: { "size": 25} }])
      console.log('======= FORMATTING complete =========');

    } catch (renameError) {
      console.error('Error renaming collection:', renameError);
    } finally {
    // Close the connection
      mongo.close();
    }
  });
};
// TODO: uncomment this when migrate to new data;
executeCollectionUpdate();
