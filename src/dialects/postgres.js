module.exports = (knex, inspector) => ({
  async delAllTables() {
    const tableList = await inspector.tables();

    // clear all tables
    for (const table of tableList) {
      await knex(table).del();
    }

    return tableList;
  },

  async beforeMigration() {
    // do nothing for postgres
  },

  async afterMigration() {
    const tableList = await inspector.tables();

    // restart sequence for tables
    for (const table of tableList) {
      let result = await knex.raw('select max(id) from ??', [table]);
      const max = result.rows[0].max;

      const nextVal =`${table.slice(0, 56)}_id_seq`; 
      if (max) {
        await knex.raw('ALTER SEQUENCE ?? RESTART WITH ??;', [nextVal, max + 1]);
      }
    }
  },
});
