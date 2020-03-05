const AWS = require('aws-sdk'),
  dbConfigs = require('./db-configs'),
  dbQuery = require('./tableQuery'),
  dbClient = require('./dbClient').init(),
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
  const recordOk = validate(payload)
  if (!recordOk) {
    console.log('Payload fails validation!', Object.keys(payload));
    return
  }
  const action = payload.action
  if (action=='insert') addObject(payload)
  else if (action=='update') updateObject(payload)
  else if (action=='xupdate') updateRevision(payload)
  else if (action=='delete') deleteObject(payload)
  else if (action=='x') {
    console.log('Payload', payload)
  }
}

function validate(payload) {
  if (!payload) return false
  else if (!payload.action) return false
  else if (!payload.uuid) return false
  else if (!payload.itemType) return false
  else return true
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

function updatePublishdateParams(payload) {
  const record = setObjectKeyByItemtype(payload)
  return cwQuery.getUpdateQuery(record.pk, record.sk).setExpr('set #data.#datePublished = :dt').names(['data', 'datePublished']).exprValue('dt', record.data.datePublished).get()
}

async function updateRevision(payload) {
  const record = await setObjectKeyByRevision(payload)
  if (!(record)) { // && record.changes
    console.log('Problem updating revision!', record);
    return
  }
  insertRecord(cwQuery.insertParams(record)).then(function(data) {
    console.log('Insert revision OK', record);
  }, function(error) {
    console.log('Unable to insert revision', util.obj2str(error));
  })
}

async function setObjectKeyByRevision(payload, keyfields = []) {
  let record = Object.keys(payload).reduce((orig, curr) => {
    if (keyfields.includes(curr)) orig[curr] = payload[curr]
    else                          orig.data[curr] = payload[curr]
    return orig
  }, { data: {} })
  record.pk = payload.uuid
  // Set the SK with a versioned prefix, 'v0' or 'vN'; get current latest version otherwise use v0
  return getPartition(payload.uuid).then(function(data) {
    console.log('Found partition', data)
    let v0revision = data.Items.find(item => item.latest>0)
    let revisionIndex = 1
    if (v0revision) {
      // Update the v0 record with the new revision
      revisionIndex = v0revision.latest + 1
      const uParams = cwQuery.getUpdateQuery(record.pk, `v0_${payload.itemType}`).setExpr('set #latest = :ver, #data = :data').names(['latest', 'data']).values({'ver': revisionIndex, 'data': record.data}).get()
      updateRecord(uParams).then(function(data) {
        console.log('v0 record updated with rev.', revisionIndex, data);
      }).catch(function(err) {
        console.log('Problem with v0 record update', uParams, err);
      })
    } else {
      // Insert a 'v0' record
      v0revision = util.copy(record)
      v0revision.sk = `v0_${payload.itemType}`
      v0revision.latest = 1
      insertRecord(cwQuery.insertParams(v0revision)).then(function(data) {
        console.log('Inserted new v0 revision record for', payload.uuid);
      })
    }
    record.sk = `v${revisionIndex}_${payload.itemType}`
    return record
  }).catch(function(err) {
    console.log('Caught error in getPartition', err);
    return null
  })
}

function updateObject(payload) {
  const params = updatePublishdateParams(payload)
  console.log('Update existing object', params);
  if (!params) {
    console.log('Invalid params for update');
    return
  }
  updateRecord(params).then(function(data) {
    console.log('Update OK', data);
  }).catch(function(err) {
    console.log('Error with Update', util.obj2str(err));
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
    record.sk = (payload.childUuid || payload.uuid)
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

function updateRecord(params) {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.update(params, function(err, data) { if (err) reject(err); else resolve(data) })
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

function getPartition(pkvalue) {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.query(cwQuery.getTableQuery(pkvalue), function (err, data) { if (err) reject(err); else resolve(data) })
  })
}

async function add(payload) {
  const rootLevelFields = ['itemType', 'slug']
  try {
    const record = setObjectKeyByItemtype(payload, rootLevelFields)
    await dbClient.insert(cwQuery.insertParams(record))
    console.log('Insert single record OK', record);
  }
  catch (err) {
    console.log('Error on add', err);
  }
}

async function removeObject(payload) {
  try {
    const record = setObjectKeyByItemtype(payload)
    await dbClient.remove(cwQuery.getDeleteParams(record))
    console.log('Delete record OK', record);
  }
  catch (err) {
    console.log('Error on remove', err);
  }
}
