/* Low level commands to Document Client */
const AWS = require('aws-sdk')

function init(region, endpoint) {
  let docClient = null
  let dynamodb = null
  AWS.config.getCredentials(function(err) {
    if (err) {
      console.log('Configuration error: Unable to set region and endpoint', err.stack);
      return
    } // credentials not loaded

    AWS.config.update({
      'region': region,
      'endpoint': endpoint
    });
    docClient = new AWS.DynamoDB.DocumentClient();
    dynamodb = new AWS.DynamoDB();
  })
  // console.log('docClient:', docClient?'Yes':'No');

  async function query(params) {
    try {
      const data = await docClient.query(params).promise()
      return data
    }
    catch (err) { return err }
  }

  // Like query() but does not use DocumentClient
  async function querydb(params) {
    try {
      const data = await dynamodb.query(params).promise()
      return data
    }
    catch (err) { return err }
  }

  async function insert(params) {
    try { await docClient.put(params).promise() }
    catch (err) { return err }
  }

  async function insertRecords(collection) {
    try {
      collection.forEach(async (item) => {
        await docClient.put(item).promise()
      });
    }
    catch (err) { console.log('Unable to insert records', err); }
  }

  async function update(params) {
    try {
      const data = await docClient.update(params).promise()
      return data
    }
    catch (err) { return err }
  }

  async function batchUpdate(params) {
    try { await docClient.batchWrite(params).promise() }
    catch (err) { return err }
  }

  async function remove(params) {
    try { await docClient.delete(params).promise() }
    catch (err) { return err }
  }

  async function transaction(params) {
    try {
      const data = await docClient.transactWrite(params).promise()
      return data
    }
    catch (err) { return err }
  }

  async function createTable(params) {
    const data = await dynamodb.createTable(params).promise()
    return data
  }

  async function deleteTable(tablename) {
    const data = await dynamodb.deleteTable({TableName: tablename}).promise()
    return data
  }

  async function listTables() {
    const data = await dynamodb.listTables({}).promise()
    return data
  }

  async function hasTable(tableName) {
    const data = await listTables()
    return data.TableNames.includes(tableName)
  }

  return {
    query: query,
    querydb: querydb,
    insert: insert,
    insertRecords: insertRecords,
    update: update,
    transaction: transaction,
    batchUpdate: batchUpdate,
    batchInsert: batchUpdate,
    remove: remove,
    createTable: createTable,
    deleteTable: deleteTable,
    listTables: listTables,
    hasTable: hasTable
  }
}

module.exports = {
  init: init,
  version: '0.0.3'
}