const AWS = require('aws-sdk'),
  dbConfigs = require('./db-configs'),
  dbQuery = require('./tableQuery'),
  dbClient = require('./dbClient').init('us-east-1', 'http://localhost:8000'),
  util = require('./general')

const transformer = require('./datatransformer').init(dbConfigs.tlmdCW)
const cwQuery = dbQuery.init(dbConfigs.tlmdCW)
const indexes = {
  'Q1': cwQuery.getLocalIndexQuery('Itemtype'),
  'Q2': cwQuery.getGlobalIndexQuery('GSI1')
}

const datafile = util.arg(1)
if (!datafile) console.log('Usage: node tlmdcw-api.js jsonfile');
else {
  const jsonPayload = util.readObject(datafile)
  if (jsonPayload) init(jsonPayload)
  else console.log('Problem reading', datafile)
}

async function init(payload) {
  try {
    const tableExists = await dbClient.hasTable(dbConfigs.tlmdCW.TableName)
    if (tableExists) {
      readPayload(payload)
    } else {
      const stats = await dbClient.createTable(dbConfigs.tlmdCW)
      console.log(`Table ${dbConfigs.tlmdCW.TableName} created:`, stats);
    }
  } catch (err) {
    console.log('Unable to list tables');
  }
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
  else if (action=='delete') deleteObjectTree(payload)
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

async function updateObject(payload) {
  const params = updatePublishdateParams(payload)
  console.log('Update existing object', params);
  if (!params) {
    console.log('Invalid params for update');
    return
  }
  try {
    const data = await dbClient.update(params)
    console.log('Update OK', data);
  }
  catch (err) {
    console.log('Error with Update', util.obj2str(err));
  }
}

async function updateRevision(payload) {
  const record = await setObjectKeyByRevision(payload)
  if (!(record)) { // && record.changes
    console.log('Problem updating revision!', record);
    return
  }
  /* insertRecord(cwQuery.insertParams(record)).then(function(data) { }, function(error) { }) */
  try {
    await dbClient.insert(cwQuery.insertParams(record))
    console.log('Insert revision OK', record);
  }
  catch (err) {
    console.log('Unable to insert revision', util.obj2str(error));
  }
}

function updatePublishdateParams(payload) {
  const record = setObjectKeyByItemtype(payload)
  return cwQuery.getUpdateQuery(record.pk, record.sk).setExpr('set #data.#datePublished = :dt').names(['data', 'datePublished']).exprValue('dt', record.data.datePublished).get()
}

async function setObjectKeyByRevision(payload, keyfields = []) {
  let record = Object.keys(payload).reduce((orig, curr) => {
    if (keyfields.includes(curr)) orig[curr] = payload[curr]
    else                          orig.data[curr] = payload[curr]
    return orig
  }, { data: {} })
  record.pk = payload.uuid
  // Set the SK with a versioned prefix, 'v0' or 'vN'; get current latest version otherwise use v0
  try {
    const data = await dbClient.query(cwQuery.getTableQuery(pkvalue))
    console.log('Found partition', data)
    let v0revision = data.Items.find(item => item.latest>0)
    let revisionIndex = 1
    if (v0revision) {
      // Update the v0 record with the new revision
      revisionIndex = v0revision.latest + 1
      const uParams = cwQuery.getUpdateQuery(record.pk, `v0_${payload.itemType}`).setExpr('set #latest = :ver, #data = :data').names(['latest', 'data']).values({'ver': revisionIndex, 'data': record.data}).get()
      try {
        await dbClient.update(uParams)
        console.log('v0 record updated with rev.', revisionIndex, data);
      }
      catch (err) {
        console.log('Problem with v0 record update', uParams, err);
      }
    } else {
      // Insert a 'v0' record
      v0revision = util.copy(record)
      v0revision.sk = `v0_${payload.itemType}`
      v0revision.latest = 1
      try {
        await dbClient.insert(cwQuery.insertParams(v0revision))
        console.log('Inserted new v0 revision record for', payload.uuid);
      }
      catch (err) {
        console.log('Problem with v0 record insert', uParams, err);
      }
    }
    record.sk = `v${revisionIndex}_${payload.itemType}`
    return record
  }
  catch (err) {
    console.log('Problems trying to set a revision key', err);
    return null
  }
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

async function addObject(payload) {
  const rootLevelFields = ['itemType', 'slug']
  if (payload.references) {
    try { await addBatch(payload, rootLevelFields) }
    catch (err) { console.log('Unable to insert batch', err); }
  } else {
    try { await add(payload) }
    catch (err) { console.log('Unable to insert object', err);  }
  }
}

async function addBatch(payload, rootLevelFields) {
  payload.references.forEach(async function(item) {
    await addPrimaryRecord(item)
  }); // add metadata records for child records
  const parent = setObjectKeyByItemtype(payload, rootLevelFields)
  const children = payload.references.reduce((orig, curr) => {
    let item = util.copy(curr)
    item.childUuid = curr.uuid // child
    item.uuid = payload.uuid   // parent
    orig.push(setObjectKeyByItemtype(item, rootLevelFields))
    return orig
  }, [])
  delete parent.data.references
  try {
    const args = cwQuery.getBatchWriteParams([parent].concat(children))
    console.log('batchInsert params', util.obj2str(args));
    await dbClient.batchInsert(args)
    console.log('batchInsert OK');
  }
  catch (err) {
    console.log('Error with batchInsert', err);
  }
}

async function addPrimaryRecord(payload) {
  const record = transformer.setPrimaryRecord(payload, 'uuid')
  const putParams = cwQuery.getInsertParams(record).setCondition('attribute_not_exists(#pk)').setName('pk').get()
  try {
    await dbClient.insert(putParams)
    console.log('Create metadata for child object', putParams);
  }
  catch (err) {
    console.log('Error on addPrimaryRecord', err);
  }
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

async function deleteObject(payload) {
  try {
    const record = setObjectKeyByItemtype(payload)
    await dbClient.remove(cwQuery.getDeleteParams(record))
    console.log('Delete record OK');
  }
  catch (err) {
    console.log('Error on remove', err);
  }
}

// Delete an object and its children
async function deleteObjectTree(payload) {
  const record = setObjectKeyByItemtype(payload)
  try {
    const data = await dbClient.query(cwQuery.queryParams(record))
    data.Items.forEach(async function(item) {
      console.log('Remove child', item.sk);
      await dbClient.remove(cwQuery.getDeleteParams(item))
    })
  }
  catch (err) {
    console.log('Error on cascade delete', err);
  }
}
