const AWS = require('aws-sdk'),
  _ = require('lodash'),
  dbConfigs = require('./db-configs'),
  dbQuery = require('./tableQuery'),
  util = require('./general')

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
  else console.log('Action NOT recognized', action);
}

function addObject(payload) {
  const objAlias = util.propstr(payload, ['uuid', 'itemType']) // display only subset of attributes
  const record = setObjectKeyByItemtype(payload)
  insertRecord(cwQuery.insertParams(record)).then(function(data) {
    console.log('Insert OK', objAlias);
    addChildMetadata(record)
  }, function(error) {
    console.log('Unable to insert object '+objAlias, util.obj2str(error));
  })
}

function addChildMetadata(payload) {
  if (payload.data.childUuid) {
    let childMetadata = {
      pk: payload.data.childUuid,
      sk: payload.data.childUuid,
      data: payload.data
    }
    const putParams = cwQuery.getInsertParams(childMetadata).setCondition('attribute_not_exists(#pk)').setName('pk').get()
    console.log('Create metadata object for child id ', payload.data.childUuid, putParams);
    insertRecord(putParams).then(function(data) { console.log('Insert OK', util.obj2str(data)); },
      function(error) { console.log('Insert Error', util.obj2str(error)); })
  }
  else console.log('No child record contained in this payload', payload);
}

function updateObject(payload) {
  const record = setObjectKeyByItemtype(payload)
  const docClient = new AWS.DynamoDB.DocumentClient();
  const updateParams = cwQuery.getUpdateQuery(record.pk, record.sk).setExpr('set #data.#datePublished = :dt').names(['data', 'datePublished']).exprValue('dt', record.data.datePublished).getParams()
  const uparams = {
    TableName: cwQuery.getTable(),
    Key: {
      'pk': record.pk,
      'sk': record.sk
    },
    UpdateExpression: 'set #data.#datePub = :dt',
    ExpressionAttributeNames: {
      '#data': 'data',
      '#datePub': 'datePublished'
    },
    ExpressionAttributeValues: {
      ':dt': record.data.datePublished
    },
    ReturnValues: 'UPDATED_NEW'
  }
  console.log('Update existing object', updateParams);
  docClient.update(updateParams, function(err, data) {
    if (err) console.log('Error on updatd', util.obj2str(err));
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

function setObjectKeyByItemtype(payload) {
  if (payload.itemType==='node') return {
    'pk': payload.uuid,
    'sk': `${util.convertUnixDate(payload.datePublished)}#${payload.itemType}`,
    'data': payload
  }
  // else if (itemType==='video') return { 'pk': 0 }
  else return {
    'pk': payload.uuid,
    'sk': (payload.childUuid || payload.title),
    'data': payload
  }
}

function insertRecord(params) {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.put(params, function(err, data) { if (err) reject(err); resolve(data) })
  })
}
