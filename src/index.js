require('dotenv').config();

const logger = require('./logger');
const _ = require('lodash');
const pluralize = require('pluralize');
const { singular } = pluralize;

const knex = require('./knex');
const schemaInspector = require('knex-schema-inspector').default;
const inspector = schemaInspector(knex);
const mongo = require('./mongo');
const { transformEntry } = require('./transform');
const idMap = require('./id-map');
const renamedCollections = require('../data-jsons/mongo-collection-rename.json');

const getGlobalId = (model, modelName, prefix) => {
  let globalId = prefix ? `${prefix}-${modelName}` : modelName;

  return model.globalId || _.upperFirst(_.camelCase(globalId));
};

const getCollectionName = (associationA, associationB) => {
  if (associationA.dominant && _.has(associationA, 'collectionName')) {
    return associationA.collectionName;
  }

  if (associationB.dominant && _.has(associationB, 'collectionName')) {
    return associationB.collectionName;
  }

  return [associationA, associationB]
    .sort((a, b) => {
      if (a.collection === b.collection) {
        if (a.dominant) return 1;
        else return -1;
      }
      return a.collection < b.collection ? -1 : 1;
    })
    .map((table) => {
      return _.snakeCase(`${pluralize.plural(table.collection)}_${pluralize.plural(table.via)}`);
    })
    .join('__');
};

async function getModelDefs(db) {
  const coreStore = db.collection('core_store');

  const cursor = coreStore.find({
    key: { $regex: /^model_def/ },
  });

  const res = (await cursor.toArray())
    .map((item) => JSON.parse(item.value))
    .map((model) => {
      const { uid } = model;

      if (!model.uid.includes('::')) {
        return {
          ...model,
          modelName: uid.split('.')[1],
          globalId: _.upperFirst(_.camelCase(`component_${uid}`)),
        };
      }

      let plugin;
      let apiName;
      let modelName;

      if (uid.startsWith('strapi::')) {
        plugin = 'admin';
        modelName = uid.split('::')[1];
      } else if (uid.startsWith('plugins')) {
        plugin = uid.split('::')[1].split('.')[0];
        modelName = uid.split('::')[1].split('.')[1];
      } else if (uid.startsWith('application')) {
        apiName = uid.split('::')[1].split('.')[0];
        modelName = uid.split('::')[1].split('.')[1];
      }

      return {
        ...model,
        plugin,
        apiName,
        modelName,
        globalId: getGlobalId(model, modelName, plugin),
      };
    });

  await cursor.close();

  return res;
}

const ignoreMongoId = ['strapi_permission', 'i18n_locales', 'users-permissions_permission', 'strapi_role', 'users-permissions_role', 'strapi_administrator', 'core_store'];

const relationDuplicates = {
  'user_profiles': 'user',
  'components_custom_pages_components_recommended_readings__magazine_article_links': 'components_custom_pages_components_recommended_reading_id'
}

async function run() {
  await knex.raw('ALTER TABLE memberships ALTER COLUMN membership_plan TYPE text');
  await knex.raw('ALTER TABLE gifted_memberships ALTER COLUMN membership_plan TYPE text');
  logger.info("alter postgres column")
  let completedTables = [];
  try {
    logger.info("Connecting to MongoDB...")
    await mongo.connect();

    const db = mongo.db();
    logger.info("Connected! Fetching model definitions...")

    const models = await getModelDefs(db);

    const modelMap = models.reduce((acc, model) => {
      acc[model.uid] = model;
      return acc;
    }, {});

    logger.info("Models fetched successfully. Executing pre-migration steps...")
    const dialect = require(`./dialects/${knex.client.config.client}`)(knex, inspector);
    await dialect.delAllTables(knex);
    await dialect.beforeMigration?.(knex);
    logger.info("Pre-migration steps complete")
    // 1st pass: for each document create a new row and store id in a map
    logger.info("First Pass - Creating rows and mapping IDs to indexes...")
    for (const model of models) {
      const finalCollectionName = renamedCollections[model.collectionName] || model.collectionName;
      if (!completedTables.includes(finalCollectionName)) {
        let count = 0;
        // const checkForDuplicates = Object.keys(duplicateKeyTable).includes(finalCollectionName);
        // const key = duplicateKeyTable[finalCollectionName]

        const cursor = db.collection(finalCollectionName).find();
        logger.verbose(`Processing collection ${finalCollectionName}`)
        while (await cursor.hasNext()) {
          const entry = await cursor.next();
          const row = transformEntry(entry, model);
          count++
          console.log(finalCollectionName, ': ', entry._id.toString(), ':', count);
          if (!ignoreMongoId.includes(finalCollectionName)) {
            row.mongoid = entry._id;
          }
          if (finalCollectionName === 'membership_payments') {
            row.transection_id = 0;
          }

          row.id = idMap.next(entry._id, finalCollectionName);

          // if (checkForDuplicates && entry[key]) {
          //   const data = await knex(finalCollectionName).where({ [key]: entry[key] });
          //   if (data.length) {
          //     console.log('already exists');
          //   } else {
          //     await knex(finalCollectionName).insert(row);
          //   }
          // } else {
          await knex(finalCollectionName).insert(row);
          // }
        }
        completedTables.push(finalCollectionName);
        await cursor.close();
      } else {
        logger.debug(`Already processed collection ${finalCollectionName}`);
      }
    }

    logger.info("Second Pass - Rows created and IDs mapped. Linking components & relations with tables...")
    // 2nd pass: for each document's components & relations create the links in the right tables
    completedTables = [];
    for (const model of models) {
      const finalCollectionName = renamedCollections[model.collectionName] || model.collectionName;
      if (!completedTables.includes(finalCollectionName)) {
        const cursor = db.collection(finalCollectionName).find();
        logger.verbose(`Processing collection ${finalCollectionName}`)
        while (await cursor.hasNext()) {
          const entry = await cursor.next();

          for (const key of Object.keys(entry)) {
            const attribute = model.attributes[key];

            if (!attribute || !entry[key]) {
              continue;
            }

            if (attribute.type === 'component') {
              // create compo links
              const componentModel = modelMap[attribute.component];
              const componentModelCollectionName = renamedCollections[componentModel.collectionName] || componentModel.collectionName;
              const linkTableName = `${finalCollectionName}_components`;

              const rows = entry[key].map((mongoLink, idx) => {
                return {
                  id: idMap.next(mongoLink._id, linkTableName),
                  field: key,
                  order: idx + 1,
                  component_type: componentModelCollectionName,
                  component_id: idMap.get(mongoLink.ref),
                  [`${singular(finalCollectionName)}_id`]: idMap.get(entry._id),
                };
              }).filter((c) => !!c.component_id);

              if (rows.length > 0) {
                logger.debug(`Filling component ${key} joining table - ${JSON.stringify(rows)}`)
                await knex(linkTableName).insert(rows);
              }

              continue;
            }

            if (attribute.type === 'dynamiczone') {

              // create compo links
              const linkTableName = `${finalCollectionName}_components`;

              const rows = entry[key].map((mongoLink, idx) => {
                const componentModel = models.find((m) => m.globalId === mongoLink.kind);
                const componentModelCollectionName = renamedCollections[componentModel.collectionName] || componentModel.collectionName;

                return {
                  id: idMap.next(mongoLink._id, linkTableName),
                  field: key,
                  order: idx + 1,
                  component_type: componentModelCollectionName,
                  component_id: idMap.get(mongoLink.ref),
                  [`${singular(finalCollectionName)}_id`]: idMap.get(entry._id),
                };
              });

              if (rows.length > 0) {
                logger.debug(`Filling dynamiczone ${key} joining table - ${JSON.stringify(rows)}`)
                await knex(linkTableName).insert(rows);
              }

              continue;
            }

            if (attribute.model === 'file' && attribute.plugin === 'upload') {
              if (!entry[key]) {
                continue;
              }

              const row = {
                upload_file_id: idMap.get(entry[key]),
                related_id: idMap.get(entry._id),
                related_type: finalCollectionName,
                field: key,
                order: 1,
              };
              logger.debug(`Linking single file - ${key} - ${JSON.stringify(row)}`)
              await knex('upload_file_morph').insert(row);
            }

            if (attribute.collection === 'file' && attribute.plugin === 'upload') {
              const rows = entry[key].map((e, idx) => ({
                upload_file_id: idMap.get(e),
                related_id: idMap.get(entry._id),
                related_type: finalCollectionName,
                field: key,
                order: idx + 1,
              }));

              if (rows.length > 0) {
                logger.debug(`Linking multiple files - ${key} - ${JSON.stringify(rows)}`)
                await knex('upload_file_morph').insert(rows);
              }
            }

            if (attribute.model || attribute.collection) {
              // create relation links

              const targetModel = models.find((m) => {
                return (
                  [attribute.model, attribute.collection].includes(m.modelName) &&
                  (!attribute.plugin || (attribute.plugin && attribute.plugin === m.plugin))
                );
              });

              const targetAttribute = targetModel?.attributes?.[attribute.via];

              const isOneWay = attribute.model && !attribute.via && attribute.model !== '*';
              const isOneToOne =
                attribute.model &&
                attribute.via &&
                targetAttribute?.model &&
                targetAttribute?.model !== '*';
              const isManyToOne =
                attribute.model &&
                attribute.via &&
                targetAttribute?.collection &&
                targetAttribute?.collection !== '*';
              const isOneToMany =
                attribute.collection &&
                attribute.via &&
                targetAttribute?.model &&
                targetAttribute?.model !== '*';
              const isManyWay =
                attribute.collection && !attribute.via && attribute.collection !== '*';
              const isMorph = attribute.model === '*' || attribute.collection === '*';

              // TODO: check dominant side
              const isManyToMany =
                attribute.collection &&
                attribute.via &&
                targetAttribute?.collection &&
                targetAttribute?.collection !== '*';


              if (isOneWay || isOneToOne || isManyToOne) {
                // TODO: optimize with one updata at the end

                if (!entry[key]) {
                  continue;
                }
                let hasEntry = false;
                if (Object.keys(relationDuplicates).includes(finalCollectionName) && key === relationDuplicates[finalCollectionName]) {
                  const entryData = await knex(finalCollectionName).where(key, idMap.get(entry[key]));
                  hasEntry = entryData.length;
                }
                if (key && idMap.get(entry._id) && idMap.get(entry[key]) && !hasEntry) {
                  // logger.debug(`1: updating tablefor otherFk and fk ${finalCollectionName}- ${JSON.stringify(entry)}`);
                  await knex(finalCollectionName)
                    .update({
                      [key]: idMap.get(entry[key]),
                    })
                    .where('id', idMap.get(entry._id));
                }
                continue;
              }

              if (isOneToMany) {
                // nothing to do
                continue;
              }

              if (isManyWay) {
                const joinTableName =
                  attribute.collectionName || `${finalCollectionName}__${_.snakeCase(key)}`;

                const fk = `${singular(finalCollectionName)}_id`;
                let otherFk = `${singular(attribute.collection)}_id`;

                if (otherFk === fk) {
                  otherFk = `related_${otherFk}`;
                }

                const rows = entry[key].map((id) => {
                  return {
                    [otherFk]: idMap.get(id),
                    [fk]: idMap.get(entry._id),
                  };
                });

                if (rows.length > 0) {
                  logger.debug(`2: joining tablefor otherFk and fk ${joinTableName}- ${JSON.stringify(rows)}`)
                  if (joinTableName === 'components_custom_pages_components_recommended_readings__magazine_article_links') {
                    rows.forEach(async (row) => {
                      const entryData = await knex(joinTableName)
                        .where('components_custom_pages_components_recommended_reading_id', row['components_custom_pages_components_recommended_reading_id'])
                        .where('magazine-article_id', row['magazine-article_id']);
                      if (!entryData.length) {
                        await knex(joinTableName).insert(rows);
                      }
                    });
                  } else if (joinTableName === 'components_custom_pages_components_something_else_links__something_else_links') {
                    rows.forEach(async (row) => {
                      if (row['components_custom_pages_components_something_else_link_id'] && row['something-else-link_id']) {
                        const entryData = await knex(joinTableName)
                          .where('components_custom_pages_components_something_else_link_id', row['components_custom_pages_components_something_else_link_id'])
                          .where('something-else-link_id', row['something-else-link_id']);
                        if (!entryData.length) {
                          await knex(joinTableName).insert(rows);
                        }
                      }
                    });
                  } else {
                    await knex(joinTableName).insert(rows);
                  }
                }

                continue;
              }

              if (isManyToMany) {
                if (attribute.dominant) {
                  const joinTableName = getCollectionName(attribute, targetAttribute);

                  let fk = `${singular(targetAttribute.collection)}_id`;
                  let otherFk = `${singular(attribute.collection)}_id`;

                  if (otherFk === fk) {
                    otherFk = `${singular(targetAttribute.via)}_id`;
                  }

                  const rows = entry[key].map((id) => {
                    return {
                      [otherFk]: idMap.get(id),
                      [fk]: idMap.get(entry._id),
                    };
                  });

                  if (rows.length > 0) {
                    logger.debug(`3: joining tablefor otherFk and fk ${joinTableName}- ${JSON.stringify(rows)}`)
                    await knex(joinTableName).insert(rows);
                  }
                }

                continue;
              }

              continue;
            }

            // get relations
          }
        }
        completedTables.push(finalCollectionName)
        await cursor.close();

        await dialect.afterMigration?.(knex);
      }
    }
    logger.info("Post-migration steps complete.")
  }
  catch (err) {
    logger.error(err)
  }
  finally {
    logger.info("Cleaning Up...")
    await mongo.close();
    await knex.destroy();
  }
  logger.info('Migration Complete');
}

run()
