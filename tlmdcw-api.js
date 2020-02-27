const AWS = require('aws-sdk'),
  _ = require('lodash'),
  dbConfigs = require('./db-configs'),
  dbQuery = require('./tableQuery'),
  util = require('./general'),
  tfm = require('./datatransformer')

const cwQuery = dbQuery.init(dbConfigs.tlmdCW)
const indexes = {
  'Q1': cwQuery.getLocalIndexQuery('Itemtype'),
  'Q2': cwQuery.getGlobalIndexQuery('GSI1')
}

const datafile = util.arg(1)
if (!datafile) console.log('Usage: node tlmdcw-api.js jsonfile');
else {
  const jsonPayload = util.readObject(datafile)
  if (!jsonPayload) {
    console.log('Problem reading', datafile)
    return
  }
  init(jsonPayload)
}

function init(payload) {
  AWS.config.getCredentials(function(err) {
    if (err) {
      console.log(err.stack);
      return
    } // credentials not loaded

    AWS.config.update({
      region: 'us-east-1',
      endpoint: 'http://localhost:8000'
    });
    listTables().then(function(tables) {
      // console.log('Tables in environment:', tables);
      if (tables.includes(dbConfigs.tlmdCW.TableName)) {
        readPayload(payload)
      } else {
        console.log(`Table ${dbConfigs.tlmdCW.TableName} to be created`);
        createTable(dbConfigs.tlmdCW)
      }
    }, function(error) {
      console.log('Unable to list tables');
    })
  })
}

function listTables() {
  return new Promise((resolve, reject) => {
    const dynamodb = new AWS.DynamoDB();
    dynamodb.listTables({}, function(err, data) { if (err) reject(err); resolve(data.TableNames) })
  })
}

function createTable(params) {
  const dynamodb = new AWS.DynamoDB();
  dynamodb.createTable(params, function(err, data) {
    if (err) console.error('Unable to create table:', util.obj2str(err));
    else console.log(`Created table '${params.TableName}'`, util.obj2str(data));
  })
}

function readPayload(payload) {
  const action = payload.action
  if (!action) console.log('No action specified for', payload.uuid, payload.itemType);
  else if (action=='insert') addObject(payload)
  else if (action=='update') updateObject(payload)
  else if (action=='delete') deleteObject(payload)
  else if (action=='x')  console.log('option x', payload);
  else console.log('Action NOT recognized', action);
}

function addObject(payload) {
  const rootLevelFields = ['itemType', 'slug']
  if (payload.references) {
    createBatchInsert(payload, rootLevelFields)
    payload.references.forEach(item => { addMetadata(item) }); // add metadata records for child records
  } else {
    const record = setObjectKeyByItemtype(payload, rootLevelFields)
    console.log('Insert single record', record);
    insertRecord(cwQuery.insertParams(record)).then(function(data) {
      console.log('Insert OK');
    }, function(error) {
      console.log('Unable to insert object', util.obj2str(error));
    })
  }
}

function createBatchInsert(payload, rootLevelFields) {
  const parent = setObjectKeyByItemtype(payload, rootLevelFields)
  const children = payload.references.reduce((orig, curr) => {
    let item = util.copy(curr) // Object.assign({}, curr)
    item.childUuid = curr.uuid // child
    item.uuid = payload.uuid   // parent
    orig.push(setObjectKeyByItemtype(item, rootLevelFields))
    return orig
  }, [])
  delete parent.data.references
  const params = { RequestItems: {}}
  params.RequestItems[dbConfigs.tlmdCW.TableName] = [parent].concat(children).map(obj => { return {PutRequest: {Item: obj}}; } )
  console.log('BatchWrite params', util.obj2str(params));
  batchPut(params).then(function(data) { console.log('batchPut OK'); }, function(error) { console.log('Error with batchPut', error); })
}

function addMetadata(payload) {
  const dtf = tfm.init(dbConfigs.tlmdCW)
  const record = util.copy(payload) // Object.assign({}, payload)
  record[dtf.pk] = payload.uuid
  record[dtf.sk] = payload.uuid
  delete record.uuid
  const putParams = cwQuery.getInsertParams(record).setCondition('attribute_not_exists(#pk)').setName('pk').get()
  console.log('Create metadata object for child id ', putParams);
  insertRecord(putParams).then(function(data) { console.log('Insert OK', util.obj2str(data)); },
    function(error) { console.log('Insert Error', util.obj2str(error)); })
}

function updateObject(payload) {
  const record = setObjectKeyByItemtype(payload)
  const docClient = new AWS.DynamoDB.DocumentClient();
  const updateParams = cwQuery.getUpdateQuery(record.pk, record.sk).setExpr('set #data.#datePublished = :dt').names(['data', 'datePublished']).exprValue('dt', record.data.datePublished).get()
  console.log('Update existing object', updateParams);
  docClient.update(updateParams, function(err, data) {
    if (err) console.log('Error on update', util.obj2str(err));
    else console.log('Update OK', data);
  })
}

function deleteObject(payload) {
  const record = setObjectKeyByItemtype(payload)
  const docClient = new AWS.DynamoDB.DocumentClient();
  const params = {
    TableName: cwQuery.getTable(),
    Key: {
      'pk': record.pk,
      'sk': record.sk
    }
  }
  console.log('Delete object', params);
  docClient.delete(params, function(err, data) {
    if (err) console.log('Error on updatd', util.obj2str(err));
    else console.log('Delete OK', data);
  })
}

function setObjectKeyByItemtype(payload, keyfields = []) {
  let record = Object.keys(payload).reduce((orig, curr) => {
    if (keyfields.includes(curr)) orig[curr] = payload[curr]
    else                          orig.data[curr] = payload[curr]
    return orig
  }, { data: {} })
  if (payload.itemType==='node') {
    record.pk = payload.uuid
    record.sk = `${util.convertUnixDate(payload.datePublished)}#${payload.itemType}`
  }
  else {
    record.pk = payload.uuid
    record.sk = (payload.childUuid || payload.title)
  }
  record['GSI1-PK'] = record.sk
  record['GSI1-SK'] = record.pk
  return record
}

function insertRecord(params) {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.put(params, function(err, data) { if (err) reject(err); else resolve(data) })
  })
}

function batchPut(params) {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.batchWrite(params, function(err, data) { if (err) reject(err); else resolve(data) })
  })
}

function getMetadata(pkvalue) {
  const qparams=cwQuery.getTableQuery(pkvalue, pkvalue) // the metadata record has same value for PK and SK
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.query(qparams, function (err, data) { if (err) reject(err); else resolve(data) })
  })
}
