const AWS = require('aws-sdk'),
  path = require('path'),
  fs = require('fs'),
  YAML = require('yaml'),
  _ = require('lodash'),
  dbConfigs = require('./db-configs')

const cmdlineParams = arg(1).toUpperCase()
if (!cmdlineParams) {
  console.log('usage: node tmundo.js options');
  console.log(' Options');
  console.log('  C = create table');
  console.log('  D = delete table');
  console.log('  Demo = run demo scenario');
  console.log('  L filename = load the given json file into the database');
  console.log('  R filename = read the given json file');
  console.log('  Q pk sk = query table by PK and SK');
  console.log('  Q1 pk sk = query index1 by PK and SK');
  console.log('  Q2 pk = query index2 by PK');
} else {
  hitDB(cmdlineParams)
}

function hitDB(cmd = 'Q') {
  AWS.config.getCredentials(function(err) {
    if (err) { console.log(err.stack);  } // credentials not loaded
    else {
      AWS.config.update({
        region: 'us-east-1',
        endpoint: 'http://localhost:8000'
      });
      if (cmd==='C') {
        createTable(dbConfigs.telemundo, 'TelemundoContent')
      } else if (cmd==='D') {
        deleteTelemundoTable()
      } else if (cmd==='L') {
        const datafile = arg(2)
        if (datafile) loadTelemundoTable(datafile)
      } else if (cmd==='R') {
        const datafile = arg(2)
        if (datafile) readTelemundoDatafile(datafile)
      }
      else if (cmd==='DEMO')  { runDemo() }
      else if (cmd==='Q')  { queryTelemundoTable(arg(2), arg(3)) }
      else if (cmd==='Q1') { queryTelemundoIndex(arg(2), arg(3)) }
      else if (cmd==='Q2') { queryTelemundoIndex(arg(2)) }
      else {
        console.log('Unrecognized option:', cmd);
      }
    }
  })
}

function runDemo() {
  const docClient = new AWS.DynamoDB.DocumentClient();
  const params = setIndex2params('none')
  const topLevelObjectId = '0003001'
  docClient.query(params, function (err, data) {
    console.log('*** View all top-level objects and pick nid', topLevelObjectId);
    if (err) console.log('Error getting item by PK+SK', obj2str(err));
    else if (data.Items) {
      console.log('Got item by PK', data.Items);
      let topLevelObj = data.Items.find(item => item.nid == topLevelObjectId)
      console.log('Found obj and add attribute to data', topLevelObj);
      topLevelObj.data.color='greenish'
      console.log('Changed object:', topLevelObj);
      console.log('Find objects CONTAINING nid', topLevelObjectId);
      queryTelemundoIndex(topLevelObjectId)
    }
    else console.log('NO item found by this index');
  })
  /* queryTelemundoIndex('none')
  console.log('*** Now editing nid 0004001');
  queryTelemundoTable('0004001', 'none')
  */
}

function createTable(params, tablename) {
  console.log(`Create table '${tablename}'`, params);
  const dynamodb = new AWS.DynamoDB();
  dynamodb.createTable(params, function(err, data) {
    if (err) {
      console.error('Unable to create table. Error JSON:', JSON.stringify(err, null, 2));
    } else {
      console.log('Created table. Table description JSON:', JSON.stringify(data, null, 2));
    }
  })
}

function loadTelemundoTable(datafile) {
  const p7content = readObject(datafile)
  if (!p7content) {
    console.log('Problem reading', datafile)
    return
  }
  const docClient = new AWS.DynamoDB.DocumentClient();
  if (Array.isArray(p7content)) {
    console.log(`Importing P7 data in ${datafile} into DynamoDB`)
    p7content.forEach(function(record, index) {
      docClient.put({
        TableName: 'TelemundoContent',
        Item: record
      }, function(err, data) {
        if (err) console.error('Unable to add P7 record. Error JSON:', obj2str(err));
        else console.log('Putitem succeeded for record', index);
      })
    })
  } else {
    docClient.put({
      TableName: 'TelemundoContent',
      Item: p7content
    }, function(err, data) {
      if (err) console.error('Unable to add P7 record. Error JSON:', obj2str(err));
      else console.log('Putitem succeeded for single record');
    })
  }
}

function readTelemundoDatafile(datafile) {
  const p7content = readObject(datafile)
  if (!p7content) {
    console.log('Problem reading', datafile)
    return
  } else if (Array.isArray(p7content)) {
    console.log(`Read import data in ${datafile}`)
    p7content.forEach(function(record, index) {
      // if (doTransform) transformP7(record, index)
      console.log('Record', index, obj2str(record));
    })
  } else {
    console.log(`Read import object in ${datafile}`)
    // if (doTransform) transformP7(p7content, 0)
    console.log('Record', obj2str(p7content));
  }
}

function queryTelemundoTable(pkvalue, skvalue) {
  // if (!(pkvalue && skvalue)) { console.log('Please provide values for PK and SK'); return; }
  if (!pkvalue) { console.log('Please provide PK value'); return; }
  const params = { TableName: 'TelemundoContent', Key: { 'nid': pkvalue, 'child': skvalue } }
  // const params = { TableName: 'TelemundoContent', Key: { 'PK': pkvalue }}
  const docClient = new AWS.DynamoDB.DocumentClient();
  docClient.get(params, function (err, data) {
    console.log('Query main table by:', pkvalue, skvalue);
    if (err) console.log('Error getting item by PK+SK', pkvalue, skvalue, obj2str(err));
    else if (data.Item) console.log('Got item by PK', pkvalue, skvalue, data.Item);
    else console.log('NO item by PK', pkvalue, skvalue);
  })
}

function queryTelemundoIndex(pkvalue, skvalue) {
  if (!pkvalue) { console.log('Please provide PK value'); return; }
  const docClient = new AWS.DynamoDB.DocumentClient();
  const params = (skvalue ? setIndex1params(pkvalue, skvalue) : setIndex2params(pkvalue))
  docClient.query(params, function (err, data) {
    console.log('Query secondary index by:', params);
    if (err) console.log('Error getting item by PK+SK', obj2str(err));
    else if (data.Items) console.log('Got item by PK', data.Items);
    else console.log('NO item found by this index');
  })
}

function setIndex1params(pkvalue, skvalue) {
  return {
    TableName: 'TelemundoContent',
    IndexName: 'GSI-1',
    KeyConditionExpression: '#child = :c and #nid = :n',
    ExpressionAttributeNames: {
      '#child': 'child',
      '#nid': 'nid'
    },
    ExpressionAttributeValues: {
     ':c': pkvalue,
     ':n': skvalue
    }
  }
}

function setIndex2params(pkvalue) {
  return {
    TableName: 'TelemundoContent',
    IndexName: 'GSI-2',
    KeyConditionExpression: '#child = :c',
    ExpressionAttributeNames: { '#child': 'child' },
    ExpressionAttributeValues: { ':c': pkvalue }
  }
}

function deleteTelemundoTable() {
  console.log('Removing the TelemundoContent table...');
  const dynamodb = new AWS.DynamoDB();
  dynamodb.deleteTable({TableName: 'TelemundoContent'}, function(err, data) {
    if (err) {
      console.error('Unable to delete table. Error JSON:', obj2str(err));
    } else {
      console.log('Deleted table.');
    }
  })
}

function transformP7(record, index) {
  let p7item = {}
  if (record.uid && record.nid && record.type) {
    p7item.PK = record.uid + '-' + record.nid // user + contentid
    p7item.SK = record.type
    p7item.type = record.type
    p7item.uid = record.uid
    p7item.uuid = record.uuid
    p7item.nid = record.nid
  } else {
    console.log(`Record ${index || ''} has no uid`, record.uid, record.nid, record.type);
    return
  }
  if (record.title) p7item.title = record.title
  // else console.log(`Record ${index} has no title`);
  if (record.status) p7item.status = record.status
  // else console.log(`Record ${index} has no status`);
  if (record.created) {
    p7item.Create_DT = new Date(record.created * 1000).toLocaleString()
  }
  // else console.log(`Record ${index} has no create date`);
  if (record.changed) {
    p7item.Updated_DT = new Date(record.changed * 1000).toLocaleString()
  }
  // else console.log(`Record ${index} has no updated date`);
  if (_.has(record, 'field_publish_date.und[0]')) {
    p7item.Published_DT = record.field_publish_date.und[0].value
  }
  // else console.log(`Record ${index} has no publish date`);
  if (_.has(record, 'field_byline.und[0]')) {
    p7item.Byline = record.field_byline.und[0].value
  }
  // else console.log(`Record ${index} has no byline`);
  if (_.has(record, 'field_categories.und')) {
    p7item.Categories = record.field_categories.und.map(item => item.tid).join(',')
  }
  // else console.log(`Record ${index} has no categories`);
  if (_.has(record, 'field_keywords.und')) {
    p7item.Keywords = record.field_keywords.und.map(item => item.tid).join(',')
  }
  // else console.log(`Record ${index} has no keywords`);
  if (_.has(record, 'field_show_season_episode.und[0]')) {
    p7item.Show = _.get(record, 'field_show_season_episode.und[0].show', '-')
    p7item.Season = _.get(record, 'field_show_season_episode.und[0].season', '-')
    p7item.Episode = _.get(record, 'field_show_season_episode.und[0].episode', '-')
  }
  // else console.log(`Record ${index} has no show/season/episode`);
  // console.log(`Record ${index} =`, propstr(p7item, ['PK', 'SK']))
  console.log(`Record ${index || ''} =`, obj2str(p7item)); console.log('');
  return p7item
}

/* --- General functions --- */
function readFile(filepath) {
  let contents = null;
  try {
    contents = fs.readFileSync(filepath, 'utf8')
  } catch (err) { console.error(err) }
  return contents
}

function readConfig(filepath) {
  let contents = null;
  try {
    contents = YAML.parse(fs.readFileSync(filepath, 'utf8'))
  } catch (err) { console.error(err) }
  return contents
}

function readObject(filepath) {
  let contents = null;
  try {
    contents = JSON.parse(fs.readFileSync(filepath, 'utf8'))
  } catch (err) { console.error(err) }
  return contents
}

function obj2str(obj) { return JSON.stringify(obj, null, 2) }

function arg(i) {
  if (i<0 || i>=process.argv.length-1) return '';
  return process.argv[i+1];
}

function propstr(sourceObj, propList) {
  return propList.reduce((orig, curr) => {
    if (sourceObj[curr]) orig.push(`${curr}: ${sourceObj[curr]}`)
    return orig
  }, []).join(', ')
}
