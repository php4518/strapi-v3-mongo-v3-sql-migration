require('dotenv').config();

const knex = require('../src/knex');

const updateMembershipEntries = async () => {
  const records = await knex('cmss_membership_plan_nuttons').select();
    for (const record of records) {
      const memberships = await knex('memberships')
        .where({ membership_plan: record.mongoid.replaceAll('"', "") })
        .update({ membership_plan: record.id });
        console.log(memberships.length);
      const gifted_memberships = await knex('gifted_memberships')
        .where({ membership_plan: record.mongoid.replaceAll('"', "") })
        .update({ membership_plan: record.id });
        console.log(gifted_memberships.length);
    }
  await knex.raw('ALTER TABLE memberships ALTER COLUMN membership_plan TYPE integer USING membership_plan::integer');
  await knex.raw('ALTER TABLE gifted_memberships ALTER COLUMN membership_plan TYPE integer USING membership_plan::integer');
}

updateMembershipEntries()