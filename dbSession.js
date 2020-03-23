/* dbSession - A library that provides some general functions like create, load, and query for a given table */
const dbQuery = require('./tableQuery'),
  dbClient = require('./dbClient').init('us-east-1', 'http://localhost:8000'),
  util = require('./general')

function init(dbConfig) {
  const queryBuilder = dbQuery.init(dbConfig)
  const maintable = queryBuilder.getQueryParams()

  function getConfig() { return dbConfig }
  async function tableExists() {
    try {
      const ok = await dbClient.hasTable(dbConfig.TableName)
      return ok
    }
    catch (err) {
      console.log('Trying to determine existence of table', err);
      return false
    }
  }
  async function listTables() {
    try {
      const data = await dbClient.listTables()
      console.log('Current tables:', data.TableNames);
    }
    catch (err) { console.log('Cannot list tables', err); }
  }

  function showIndexes(collection, type = 'Global') {
    if (!collection || collection.length===0) console.log(`NO ${type} Secondary indexes`);
    else {
      collection.forEach((idx, counter) => {
        let pk=idx.KeySchema.find(item => item.KeyType=='HASH'); let pkname=pk?pk.AttributeName:'?'
        let sk=idx.KeySchema.find(item => item.KeyType=='RANGE'); let skname=sk?sk.AttributeName:'?'
        console.log(counter, `${type} Index "${idx.IndexName}" PK="${pkname}", SK="${skname}"`);
      });
    }
  }

  function listIndexes() {
    showIndexes(dbConfig.LocalSecondaryIndexes, 'Local')
    showIndexes(dbConfig.GlobalSecondaryIndexes)
  }

  async function createTable() {
    try {
      const stats = await dbClient.createTable(dbConfig)
      console.log(`Table "${dbConfig.TableName}" created:`, stats);
    }
    catch (err) {
      console.log(`Unable to create table ${dbConfig.TableName}:`, err.message);
    }
  }

  async function reinitTable() {
    try {
      await dbClient.deleteTable(dbConfig.TableName)
      await createTable()
    }
    catch (err) {
      console.log('Unable to re-initialize table', err.message);
    }
  }

  async function deleteTable() {
    try {
      await dbClient.deleteTable(dbConfig.TableName)
      console.log(`Table "${dbConfig.TableName}" deleted!`);
    }
    catch (err) {
      console.log('Unable to remove table', err.message);
    }
  }

  async function queryTable(pkvalue, skvalue, comparison = 'E') {
    if (!pkvalue) { console.log('Please provide PK value'); return; }
    const sortKeyDisplay = (skvalue || 'No sort key value provided')
    const query = getQuery(pkvalue, skvalue, comparison)
    const qparams = query.get()
    const descrip =  query.explain()
    try {
      const data = await dbClient.query(qparams)
      if (data.Items && data.Items.length>0) {
        console.log(`"${descrip}" -> succeeded: ${data.Items.length} objects. Params=`, qparams);
        console.log(util.shortView(data.Items, ['pk', 'sk', 'itemType', 'data']));
      }
      else console.log(`${descrip} found NO objects. Params=`, qparams);
    }
    catch (err) {
      console.log('Error getting item by key', pkvalue, sortKeyDisplay, util.obj2str(err));
    }
  }

  async function queryLocalIndex(pkvalue, skvalue, comparison = 'E', ordinal = 0) {
    if (!pkvalue) { console.log('Please provide PK value'); return; }
    const sortKeyDisplay = (skvalue || 'No sort key value provided')
    const secIndex = queryBuilder.getLocalIndexQueryByOrdinal(ordinal)
    const query = getQuery(pkvalue, skvalue, comparison, secIndex)
    const qparams = query.get()
    const descrip =  query.explain()
    if (secIndex) console.log('Using', secIndex.toString());
    try {
      const data = await dbClient.query(qparams)
      if (data.Items && data.Items.length>0) {
        console.log(`"${descrip}" -> succeeded: ${data.Items.length} objects. Params=`, qparams);
        console.log(util.shortView(data.Items, ['pk', 'sk', 'itemType', 'data']));
      }
      else console.log(`${descrip} found NO objects. Params=`, qparams);
    }
    catch (err) {
      console.log('Error getting item by key', pkvalue, sortKeyDisplay, util.obj2str(err));
    }
  }

  async function queryGlobalIndex(pkvalue, skvalue, comparison = 'E', ordinal = 0) {
    if (!pkvalue) { console.log('Please provide PK value'); return; }
    const sortKeyDisplay = (skvalue || 'No sort key value provided')
    const secIndex = queryBuilder.getGlobalIndexQueryByOrdinal(ordinal)
    const query = getQuery(pkvalue, skvalue, comparison, secIndex)
    const qparams = query.get()
    const descrip =  query.explain()
    if (secIndex) console.log('Using', secIndex.toString());
    try {
      const data = await dbClient.query(qparams)
      if (data.Items && data.Items.length>0) {
        console.log(`"${descrip}" -> succeeded: ${data.Items.length} objects. Params=`, qparams);
        console.log(util.shortView(data.Items, ['pk', 'sk', 'itemType', 'data']));
      }
      else console.log(`${descrip} found NO objects. Params=`, qparams);
    }
    catch (err) {
      console.log('Error getting item by key', pkvalue, sortKeyDisplay, util.obj2str(err));
    }
  }

  async function loadTable(datafile) {
    const jsonObj = util.readObject(datafile)
    if (!jsonObj) {
      console.log('Unable to read', datafile)
      return
    }
    if (Array.isArray(jsonObj)) {
      console.log(`Importing data in ${datafile} into table ${dbConfig.TableName}`)
      const records = jsonObj.map(item => { return { TableName: dbConfig.TableName, Item: item }})
      dbClient.insertRecords(records)
    } else {
      dbClient.insert({ TableName: dbConfig.TableName, Item: jsonObj })
    }
  }

  function getQuery(pkvalue, skvalue, comparison, secIndex) {
    const op = comparison.toUpperCase()
    if (secIndex) {
      if (op==='E') return secIndex.eq(pkvalue, skvalue)
      else if (op==='B') return secIndex.beginsWith(pkvalue, skvalue)
      else if (op==='C') return secIndex.contains(pkvalue, skvalue)
      else return secIndex.eq(pkvalue, skvalue)
    } else {
      if (op==='E') return maintable.eq(pkvalue, skvalue)
      else if (op==='B') return maintable.beginsWith(pkvalue, skvalue)
      else if (op==='C') return maintable.contains(pkvalue, skvalue)
      else return maintable.eq(pkvalue, skvalue)
    }
  }
  return {
    tableExists: tableExists,
    createTable: createTable,
    loadTable: loadTable,
    deleteTable: deleteTable,
    reinitTable: reinitTable,
    queryTable: queryTable,
    queryLocalIndex: queryLocalIndex,
    queryGlobalIndex: queryGlobalIndex,
    listTables: listTables,
    listIndexes: listIndexes,
    getConfig: getConfig
  }
} // init
module.exports = { init: init }
