const hash = require('string-hash-64');
const LruCache = require('lru-cache');

/**
 * Available types
 */
const availableTypes = [
  'varchar',
  'int',
  'smallint',
  'mediumint',
  'bigint',
  'text',
  'mediumtext',
  'float',
  'decimal',
  'double',
  'double precision',
  'real',
  'boolean',
  'date',
  'datetime',
  'time',
  'timestamp',
  'binary',
  'varbinary',
  'json',
  'jsonb',
  'uuid',
  'bit'
];

/**
 * Mysql dialect adapter
 */
class Mysql {

  /**
   * Create a new adapter instance
   * 
   * @param {Object} db 
   */
  constructor(db, dbSchema = {}) {
    this.db = db;
    this.dbSchema = dbSchema;
    this.cache = new LruCache({ max: 500, maxAge: 1000 * 60 * 60 * 5 });
  }

  /**
   * Get available types from the RDMS
   */
  static getAvailableTypes() {
    return availableTypes;
  }

  /**
   * Get Graphql type from database column data type
   * 
   * @param {String} columnname 
   * @param {Object} attrs 
   */
  mapDbColumnToGraphqlType(columnname, attrs) {
    let graphqlType = '';

    switch(attrs.data_type) {
      case 'tinyint':
      case 'bit':
      case 'bool':
      case 'boolean':
        graphqlType = 'Boolean';
        break;
      case 'numeric':
      case 'float':
      case 'double':
      case 'double precision':
      case 'real':
      case 'decimal':
        graphqlType = 'Float';
        break;
      case 'int':
      case 'integer':
      case 'smallint':
      case 'mediumint':
      case 'bigint':
        graphqlType = 'Int';
        break;
      case 'varchar':
      case 'char':
      case 'text':
      case 'mediumtext':
      case 'binary':
      case 'varbinary':
      case 'bytea':
      case 'tinyblob':
      case 'tinytext':
      case 'blob':
      case 'mediumtext':
      case 'mediumblob':
      case 'longtext':
      case 'longblob':
      case 'enum':
      case 'set':
      case 'json':
      case 'point':
      case 'USER-DEFINED':
        graphqlType = 'String';
        break;
      case 'date':
      case 'datetime':
      case 'timestamp':
      case 'time':
      case 'year':
        graphqlType = 'String';
        break;
      default: throw new Error('Undefined column type: ' + attrs.data_type + ' of column '+ columnname);
    }
    return graphqlType;
  }

  /**
   * Get a page of the table
   * 
   * @param {String} tablename 
   * @param {Object} args 
   */
  async page(tablename, args) {

    // Load from cache
    const key = this.getCacheKey(tablename, 'page', [], args);
    if (args._cache !== false && this.cache.peek(key)) {
      if (args._debug) console.log('cache hit:', key);
      return this.cache.get(key);
    }

    // Load items
    let query = this.db(tablename);
    (args) && this.addWhereFromArgs(tablename, query, args);
    (args) && this.addWhereFromArgsWhere(query, args);
    (args) && this.addPaginationFromArgs(tablename, query, args);
    if (args._debug) console.log('db hit:', query.toSQL().sql, query.toSQL().bindings);
    const items = await query;
    this.cache.set(key, items);
    return items;
  }

  /**
   * Get pagination total
   * 
   * @param {String} tablename 
   * @param {Object} args 
   */
  async pageTotal(tablename, args) {
    const query = this.db(tablename);
    (args) && this.addWhereFromArgs(tablename, query, args);
    (args) && this.addWhereFromArgsWhere(query, args);
    const totalResult = await query.count();
    let result = JSON.parse(JSON.stringify(totalResult));
    return parseInt(result[0]['count(*)'], 10);
  }

  /**
   * Get one record
   * 
   * @param {String} tablename 
   * @param {Object} args
   */
  async firstOf(tablename, args) {

    // Load item
    let query = this.db(tablename);
    (args) && this.addWhereFromArgs(tablename, query, args);
    (args) && this.addWhereFromArgsWhere(query, args);
    if (args._debug) console.log('db hit:', query.toSQL().sql, query.toSQL().bindings);
    return await query.first();
  }

  /**
   * Insert or update record
   * 
   * @param {String} tablename 
   * @param {Object} data 
   */
  async putItem(tablename, data) {
    const pk = this.getPrimaryKeyFromSchema(tablename);
    let result = null;

    // Check exists
    let count = [{"count(*)": "0"}];
    if (data.input[pk]) count = await this.db(tablename).where(pk, data.input[pk]).count();

    // Insert or update
    if (parseInt(count[0]['count(*)'], 10) === 0) {
      let query = this.db(tablename);
      const lastIdresult = await query.insert(data.input);
      result = [lastIdresult[0]];
      if (data._debug) console.log('db insert:', query.toSQL().sql, query.toSQL().bindings);
    } else {
      let query = this.db(tablename)
      query.where(pk, data.input[pk])
      result = await query.update(data.input);
      if (data._debug) console.log('db update:', query.toSQL().sql, query.toSQL().bindings);
    }
    return result;
  }

  /**
   * Load knex query with condition
   * 
   * @param {Function} query 
   * @param {Object} condition 
   */
  convertConditionToWhereClause(query, condition) {
    const column = condition[1];
    let op = condition[0];
    let value = condition[2];
    switch(op) {
      case '~':
        op = 'ilike';
        value = '%'+value.replace(' ', '%')+'%';
        query.where(column, 'ilike', value);
        break;
      case '#':
        query.whereIn(column, value.split(','));
        break;
      case '<=>':
        query.whereRaw(column + ' = ' + value);
        break;
      default:
        query.where(column, op, value);
    }
  }

  /**
   * Load knex query with all filters
   * 
   * @param {Function} query 
   * @param {Object} args 
   */
  addWhereFromArgs(tablename, query, args) {

    // Validate filter arguments
    if (!args.filter) return;
    let conditions = args.filter[tablename];
    if (!conditions) return;

    // Apply filters
    for (let i = 0; i < conditions.length; i++) {
      this.convertConditionToWhereClause(query, conditions[i]);
    }
  }

  /**
   * Load knex query with where condition
   * 
   * @param {Function} query 
   * @param {Object} args 
   */
  addWhereFromArgsWhere(query, args) {

    // Validate filter arguments
    if (!args.where) return;

    // Apply filters
    const { sql, val } = args.where;
    query.whereRaw(sql, val);
  }

  /**
   * Load knex query with all pagination
   * 
   * @param {Function} query 
   * @param {Object} args 
   */
  addPaginationFromArgs(tablename, query, args) {

    // Validate pagination arguments
    if (!args.pagination) return;
    let pagination = args.pagination[tablename];
    if (!pagination) return;

    // Apply pagination to query
    for (let i = 0; i < pagination.length; i++) {
      const op = pagination[i][0];
      let value = pagination[i][1];
      switch(op) {
        case 'limit':
          query.limit(value);
          break;
        case 'offset':
          query.offset(value);
          break;
        case 'orderby':
          value = value.split(' ')
          query.orderBy(value[0], value[1]);
          break;
      }
    }
  }

  /**
   * Run a raw SQL query
   * 
   * @param {String} sql 
   * @param {Object} params 
   */
  async query(sql, params) {
    let result = (await this.db.raw(sql, params))[0];
    return JSON.parse(JSON.stringify(result));
  }

  /**
   * Generate cache key
   * 
   * @param {String} tablename 
   * @param {String} columnname 
   * @param {Array} ids 
   * @param {Object} args 
   */
  getCacheKey(tablename, columnname, ids, args) {
    const filteredArgs = { filter: args.filter, pagination: args.pagination };
    let key = tablename + columnname + ids.join(',') + JSON.stringify(filteredArgs);
    return String(hash(key));
  }

  /**
   * Get exclude SQL condition when loading
   * database table names
   * 
   * @param {Array} exclude 
   */
  getExcludeCondition(exclude) {
    let sql = '';
    if (!exclude || exclude.length === 0) return sql;
    const placeholders = exclude.map(v => '?').join(',');
    sql += `AND table_name NOT IN (${placeholders})`;
    return sql;
  }

  /**
   * Get database tables
   * 
   * @param {String} schemaname 
   * @param {Array} exclude 
   */
  async getTables(schemaname, exclude = []) {
    const tables = [];

    // Build getTable SQL query
    let sql = `
      SELECT table_name as name 
      FROM information_schema.tables 
      WHERE table_schema = ?
      ${this.getExcludeCondition(exclude)}
    `;

    // Get table names
    let rows = await this.query(sql, [schemaname, ...exclude]);
    for (let i = 0; i < rows.length; i++) {
      tables.push({ name: rows[i].name });
    }
    return tables; 
  }

  /**
   * Get table columns
   * and create an object representation
   * 
   * @param {String} schemaname 
   * @param {String} tablename 
   */
  async getColumns(schemaname, tablename) {
    const columns = [];
    const sql = `
      SELECT column_name as name, is_nullable, data_type
      FROM information_schema.columns
      WHERE table_schema = ? AND table_name = ?
    `;
    let rows = await this.query(sql, [schemaname, tablename]);
    for (let j = 0; j < rows.length; j++) {
      columns.push({
        name: rows[j].name,
        is_nullable: rows[j]['IS_NULLABLE'] || rows[j]['is_nullable'],
        data_type: rows[j]['DATA_TYPE'] || rows[j]['data_type']
      });
    }
    return columns;
  }

  /**
   * Get foreign key constraints
   * for a database table
   * 
   * @param {String} schemaname 
   * @param {String} tablename 
   */
  async getForeignKeys(schemaname, tablename) {
    const fkeys = [];
    const sql = `
      SELECT 
        tc.CONSTRAINT_SCHEMA as schemaname,
        tc.CONSTRAINT_NAME as constraint_name,
        tc.TABLE_NAME as tablename,
        ccu.COLUMN_NAME as columnname,
        ccu.REFERENCED_TABLE_SCHEMA as ftableschema,
        ccu.REFERENCED_TABLE_NAME as ftablename,
        ccu.REFERENCED_COLUMN_NAME as fcolumnname
      FROM information_schema.TABLE_CONSTRAINTS tc 
      LEFT JOIN information_schema.KEY_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME 
      WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY' 
        AND tc.CONSTRAINT_SCHEMA = ccu.REFERENCED_TABLE_SCHEMA
        AND tc.TABLE_SCHEMA = ?
        AND tc.TABLE_NAME = ?;
    `;
    let rows = await this.query(sql, [schemaname, tablename]);
    for (let j = 0; j < rows.length; j++) {
      fkeys.push(rows[j]); 
    }
    return fkeys;
  }

  /**
   * Get primary key constraint
   * for a database table
   * 
   * @param {String} schemaname 
   * @param {String} tablename 
   */
  async getPrimaryKey(schemaname, tablename) {
    let pk = null;
    const sql = `
      SELECT 
        kcu.column_name as columnname 
      FROM 
        information_schema.table_constraints AS tc 
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        AND tc.table_schema = ?
        AND tc.table_name = ?
        AND kcu.table_name = ?;
    `;
    let rows = await this.query(sql, [schemaname, tablename, tablename]);
    return rows.length ? rows[0].columnname : pk;
  }

  /**
   * Helper to get table names
   * from current database schema
   */
  getTablesFromSchema() {
    return Object.keys(this.dbSchema);
  }

  /**
   * Helper to get column names for a table
   * from current database schema
   * 
   * @param {String} tablename 
   */
  getTableColumnsFromSchema(tablename) {
    return Object.keys(this.dbSchema[tablename]).filter(c => c !== '__reverse' && c !== '__pk');
  }

  /**
   * Helper to get primary key for a table
   * from current database schema
   * 
   * @param {String} tablename 
   */
  getPrimaryKeyFromSchema(tablename) {
    return this.dbSchema[tablename].__pk;
  }

  /**
   * Build and return the database schema
   * Use exclude parameter to exclude indesired tables
   * 
   * @param {String} schemaname 
   * @param {Array} exclude 
   */
  async getSchema(schemaname, exclude = []) {

    let dbSchema = {};
    
    // Get tables
    let tables = await this.getTables(schemaname, exclude);
    for (let i = 0; i < tables.length; i++) {
      let tablename = tables[i].name;
      let pk = await this.getPrimaryKey(schemaname, tablename);
      dbSchema[tablename] = {
        __pk: pk,
        __reverse: []
      };

      // Get columns
      let columns = await this.getColumns(schemaname, tablename);
      for (let j = 0; j < columns.length; j++) {
        let columnname = columns[j].name; 
        dbSchema[tablename][columnname] = columns[j];
      }
    }

    // Get foreign keys
    for (let tablename in dbSchema) {
      const fkeys = await this.getForeignKeys(schemaname, tablename);
      for (let j = 0; j < fkeys.length; j++) {

        // Assign foreign key definition to column
        dbSchema[tablename][fkeys[j].columnname]['__foreign'] = {
          schemaname: fkeys[j].ftableschema,
          tablename: fkeys[j].ftablename,
          columnname: fkeys[j].fcolumnname
        };

        // Assign inverse relations to table
        dbSchema[fkeys[j].ftablename]['__reverse'].push({
          fschemaname: fkeys[j].schemaname,
          ftablename: fkeys[j].tablename,
          fcolumnname: fkeys[j].columnname,
          columnname: fkeys[j].fcolumnname
        });
      }
    }

    // Return database schema
    this.dbSchema = dbSchema;
    return this.dbSchema;
  }
}

module.exports = Mysql;