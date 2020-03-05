/* Low level commands to Document Client */
const AWS = require('aws-sdk')

function init() {
  const docClient = new AWS.DynamoDB.DocumentClient();

  async function insert(params) {
    try { await docClient.put(params).promise() }
    catch (err) { return err }
  }

  async function update(params) {
    try { await docClient.update(params).promise() }
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

  async function listTables() {
    const dynamodb = new AWS.DynamoDB();
    try {
      const data = await dynamodb.listTables({}).promise()
      return data
    }
    catch (err) { return err }
  }

  async function hasTable(tableName) {
    try {
      const data = await listTables()
      return data.TableNames.includes(tableName)
    }
    catch (err) { return err }
  }

  return {
    insert: insert,
    update: update,
    batchUpdate: batchUpdate,
    remove: remove,
    hasTable: hasTable
  }
}

module.exports = {
  init: init,
  version: '0.0.1'
}