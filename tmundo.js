const AWS = require('aws-sdk'),
  _ = require('lodash'),
  dbConfigs = require('./db-configs'),
  dbQuery = require('./tableQuery'),
  util = require('./general')

const cjp=require('./data/utils.js')

const mainTable = 'TelemundoCW'
const cwQuery = dbQuery.init(dbConfigs.telemundoCW)
const gsi1Index = cwQuery.getIndexQuery('ParentNode')
const gsi2Index = cwQuery.getIndexQuery('ItemTypeParentIndex')
const gsi3Index = cwQuery.getIndexQuery('ChildNodeIndex')

const tlmdQuery = dbQuery.init(dbConfigs.telemundo)
const titleIndex = tlmdQuery.getIndexQuery('SeriesTypeTitle')
const statusIndex = tlmdQuery.getIndexQuery('SeriesTypeStatus')
const childIndex = tlmdQuery.getIndexQuery('ChildNode')
const cmdlineParams = util.arg(1).toUpperCase()
if (!cmdlineParams) {
  console.log('usage: node tmundo.js options');
  console.log(' Options');
  console.log('  C tablename = create table');
  console.log('  D tablename = delete table');
  console.log('  Demo = run demo scenario');
  console.log('  L filename tablename = load the given json file into a table');
  console.log('  R filename = read the given json file');
  console.log('  Q pk sk = query table by PK and SK');
  console.log('  Q1 pk sk = query ChildNode index');
  console.log('  QT pk sk = query SeriesTypeTitle index');
  console.log('  QS pk sk = query SeriesTypeStatus index');
  console.log('  QSF pk sk = query SeriesTypeStatus index using Filter (Unpublished)');
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
        const tablename=util.arg(2)
        if (tablename) {
          if (dbConfigs[tablename]) createTable(dbConfigs[tablename], tablename)
          else console.log('No table defined in configuration:', tablename);
        }
      } else if (cmd==='D') {
        const configname=util.arg(2)
        if (configname) {
          if (dbConfigs[configname]) deleteTable(dbConfigs[configname].TableName)
          else console.log('No table defined in configuration:', tablename);
        }
      } else if (cmd==='L') {
        const datafile = util.arg(2)
        // if (datafile) loadTable(datafile, util.arg(3))
        if (datafile) loadCWTable(datafile)
      } else if (cmd==='R') {
        const datafile = util.arg(2)
        if (datafile) readTelemundoDatafile(datafile)
      }
      else if (cmd==='DEMO') { runDemo(util.arg(2) || 'taxonomy') }
      else if (cmd==='Q') {
        const pk=util.arg(2)
        // let sk=util.arg(3); if (!sk) sk = pk
        const shortView = util.arg(3)
        const cols = '#uuid, datePublished, itemType, title, slug'
        queryTelemundoTable(pk, pk, shortView?cols:null)
      }
      else if (cmd==='Q1') {
        const pk=util.arg(2)
        const sk=util.arg(3)
        // queryTelemundoIndex(setIndexParams(pk, sk))
        // queryTelemundoIndex(childIndex.eq(pk, sk))
        console.log('*** Find parent records ***');
        let qparams = gsi1Index.eq(pk, sk, '#uuid, isParent, datePublished, itemType, title')
        // qparams.FilterExpression = 'isParent = false'
        queryTelemundoIndex(qparams)
      }
      else if (cmd==='Q3') {
        const pk=util.arg(2)
        const sk=util.arg(3)
        console.log('*** Find child records ***');
        queryTelemundoIndex(gsi3Index.eq(util.arg(2), null, '#uuid, programUuid, isParent, datePublished, itemType, title, slug'))
      }
      else if (cmd==='QS') {
        // queryTelemundoIndex(statusIndex.beginsWith(util.arg(2), util.arg(3)))
        console.log('*** Find content types under a record ***');
        queryTelemundoIndex(gsi2Index.eq(util.arg(2), util.arg(3), '#uuid, datePublished, itemType, title, slug'))

        // console.log('*** Find records containing a record ***');
        // queryTelemundoIndex(gsi3Index.eq(util.arg(2), null, 'datePublished, itemType, title, slug'))
      }
      else if (cmd==='QSF') {
        let qparams = statusIndex.beginsWith(util.arg(2), util.arg(3))
        qparams.FilterExpression = '#status = :stat'
        qparams.ExpressionAttributeNames['#status'] = 'status'
        qparams.ExpressionAttributeValues[':stat'] = util.arg(4) || '0'
        queryTelemundoIndex(qparams)
      }
      else if (cmd==='QT') {
        // queryTelemundoIndex(setIndex2Params(util.arg(2), util.arg(3)))
        queryTelemundoIndex(titleIndex.beginsWith(util.arg(2), util.arg(3)))
      }
      else if (cmd==='X') {
        const pk=util.arg(2)
        const sk=util.arg(3)
        let qparams = gsi1Index.$eq(pk, sk).$project('#uuid, isParent, datePublished, itemType, title').$filter('#status = :status', 'status', util.arg(4)).getParams()
        console.log('Our index param obj:', qparams);
      }
      else {
        console.log('Unrecognized option:', cmd);
      }
    }
  })
}

function runDemo(searchTerm) {
  const gsi = tlmdQuery.getIndexQuery('ChildNode')
  if (gsi) {
    getQueryIndex(gsi.eq('none')).then(function(data) {
      console.log(`Query returned ${data.Items.length} with content type ${searchTerm}`, data.Items.find(item => item.ctype==searchTerm));
    }, function(error) {
      console.log('Query failed:', util.obj2str(error));
    })
  }
}

function createTable(params, tablename) {
  const dynamodb = new AWS.DynamoDB();
  dynamodb.createTable(params, function(err, data) {
    if (err) console.error('Unable to create table:', util.obj2str(err));
    else console.log(`Created table '${tablename}'`, util.obj2str(data));
  })
}

function loadCWTable(datafile) {
  const p7content = util.readObject(datafile)
  if (!p7content) {
    console.log('Problem reading', datafile)
    return
  }
  const docClient = new AWS.DynamoDB.DocumentClient();
  const tablename = 'TelemundoCW'
  let parentRecord = {}
  const newitem = doTransform(p7content)
  if (newitem.isParent) parentRecord = null
  else { // Create a parent record
    // delete newitem.isParent
    Object.assign(parentRecord, newitem)
  }
  docClient.put({
    TableName: tablename,
    Item: newitem
  }, function(err, data) {
    if (err) console.error('Unable to add record. Error JSON:', util.obj2str(err));
    else console.log('Putitem succeeded for single record');
  })
  if (parentRecord) {
    parentRecord.programUuid = parentRecord.uuid
    parentRecord.isParent = true
    docClient.put({ TableName: tablename, Item: parentRecord }, function(err, data) {
      if (err) console.error('Unable to add record. Error JSON:', util.obj2str(err));
      else console.log('Putitem succeeded for single record');
    })
  }
}

function loadTable(datafile, tablename) {
  const p7content = util.readObject(datafile)
  if (!p7content) {
    console.log('Problem reading', datafile)
    return
  }
  const docClient = new AWS.DynamoDB.DocumentClient();
  if (Array.isArray(p7content)) {
    console.log(`Importing data in ${datafile} into table ${tablename}`)
    p7content.forEach(function(record, index) {
      docClient.put({
        TableName: tablename,
        Item: record
      }, function(err, data) {
        if (err) console.error('Unable to add P7 record. Error JSON:', util.obj2str(err));
        else console.log('Putitem succeeded for record', index);
      })
    })
  } else {
    docClient.put({ TableName: tablename, Item: p7content }, function(err, data) {
      if (err) console.error('Unable to add record. Error JSON:', util.obj2str(err));
      else console.log('Putitem succeeded for single record');
    })
  }
}

function loadTransformTable(datafile) {
  const p7content = util.readObject(datafile)
  if (!p7content) {
    console.log('Problem reading', datafile)
    return
  }
  const docClient = new AWS.DynamoDB.DocumentClient();
  console.log(`Importing P7 data in ${datafile} into DynamoDB`)
  p7content.forEach(function(record, index) {
    docClient.put({
      TableName: 'TelemundoContent',
      Item: transformAppend(record)
    }, function(err, data) {
      if (err) console.error('Unable to add P7 record. Error JSON:', util.obj2str(err));
      else console.log('Putitem succeeded for record', index);
    })
  })
}

function readTelemundoDatafile(datafile) {
  const p7content = util.readObject(datafile)
  if (!p7content) {
    console.log('Problem reading', datafile)
    return
  } else if (Array.isArray(p7content)) {
    console.log(`Read import data in ${datafile}`)
    p7content.forEach(function(record, index) {
      console.log('Record', index, util.obj2str(record));
    })
  } else {
    console.log(`Read import object in ${datafile}`, Object.keys(p7content))
    const newRecord = doTransform(p7content)
    cjp.survey(newRecord)
  }
}

function doTransform(record) {
  const mainFields = util.copyFields(record, ['data', 'itemType', 'uuid', 'slug', 'title', 'media', 'categories', 'tags', 'published', 'relatedSeries', 'currentSeason', 'datePublished', 'references', 'frontends'])
  if (!mainFields.data) {
    console.log('No data attrib present, now adding...')
    mainFields.data = util.copyFields(record, ['title', 'short_description', 'long_description', 'promoDescription', 'promoTitle', 'seriesType', 'promoKicker', 'genre', 'links', 'customFields'])
  }
  mainFields.programUuid = (record.program && record.program.programUuid) ? record.program.programUuid  : record.uuid
  mainFields.isParent = (mainFields.programUuid == mainFields.uuid)
  if (mainFields.datePublished) mainFields.datePublished = util.convertUnixDate(mainFields.datePublished)
  return util.noblanks(mainFields)
}

function queryTelemundoTable(pkvalue, skvalue, projection) {
  if (!pkvalue) { console.log('Please provide PK value'); return; }
  const sortKeyDisplay = (skvalue || 'No sort key value provided')
  if (!mainTable) return
  let params = { TableName: mainTable, Key: { 'uuid': pkvalue }, ExpressionAttributeNames: {'#uuid': 'uuid'}}
  if (skvalue) { params.Key.programUuid = skvalue }
  if (projection) params.ProjectionExpression = projection
  const docClient = new AWS.DynamoDB.DocumentClient();
  docClient.get(params, function (err, data) {
    console.log('Query main table by:', pkvalue, sortKeyDisplay);
    if (err) console.log('Error getting item by key', pkvalue, sortKeyDisplay, util.obj2str(err));
    else if (data.Item) console.log('Got item by PK', pkvalue, sortKeyDisplay, data.Item);
    else console.log('NO item by PK', pkvalue, sortKeyDisplay);
  })
}

function queryTelemundoIndex(queryParams) {
  const docClient = new AWS.DynamoDB.DocumentClient();
  docClient.query(queryParams, function (err, data) {
    console.log('Query secondary index by:', queryParams);
    if (err) console.log('Error getting item by key', util.obj2str(err));
    else if (data.Items) console.log('Got item by key', data.Items);
    else console.log('NO item found by this index');
  })
}

function getQueryIndex(queryParams) {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.query(queryParams, function (err, data) { if (err) reject(err); resolve(data) })
  })
}

function setIndexParams(pkvalue, skvalue) {
  let params = {
    TableName: 'TelemundoContent',
    IndexName: 'ChildNode'
  }
  if (skvalue) {
    params.KeyConditionExpression = '#child = :c and #nid = :n'
    params.ExpressionAttributeNames = { '#child': 'child', '#nid': 'nid' }
    params.ExpressionAttributeValues = { ':c': pkvalue, ':n': skvalue }
  } else {
    params.KeyConditionExpression = '#child = :c'
    params.ExpressionAttributeNames = { '#child': 'child' }
    params.ExpressionAttributeValues = { ':c': pkvalue }
  }
  return params
}

function setIndex2Params(pkvalue, skvalue) {
  let params = {
    TableName: 'TelemundoContent',
    IndexName: 'SeriesTypeTitle'
  }
  if (skvalue) {
    params.KeyConditionExpression = '#section = :pk AND begins_with(seriesCtypeTitle, :sk)'
    params.ExpressionAttributeNames = { '#section': 'section' }
    params.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
  } else {
    params.KeyConditionExpression = '#section = :pk'
    params.ExpressionAttributeNames = { '#section': 'section' }
    params.ExpressionAttributeValues = { ':pk': pkvalue }
  }
  return params
}

function deleteTable(tablename) {
  console.log(`Removing the ${tablename} table...`);
  const dynamodb = new AWS.DynamoDB();
  dynamodb.deleteTable({TableName: tablename}, function(err, data) {
    if (err) {
      console.error('Unable to delete table. Error JSON:', util.obj2str(err));
    } else {
      console.log('Deleted table.');
    }
  })
}

function transformAppend(record, index) {
  record.seriesCtypeTitle = `${record.series}#${record.ctype}#${record.title}`.toUpperCase()
  record.seriesCtypeStatus = `${record.series}#${record.ctype}#${record.status}`.toUpperCase()
  return record
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
  if (record.status) p7item.status = record.status
  if (record.created) {
    p7item.Create_DT = new Date(record.created * 1000).toLocaleString()
  }
  if (record.changed) {
    p7item.Updated_DT = new Date(record.changed * 1000).toLocaleString()
  }
  if (_.has(record, 'field_publish_date.und[0]')) {
    p7item.Published_DT = record.field_publish_date.und[0].value
  }
  if (_.has(record, 'field_byline.und[0]')) {
    p7item.Byline = record.field_byline.und[0].value
  }
  if (_.has(record, 'field_categories.und')) {
    p7item.Categories = record.field_categories.und.map(item => item.tid).join(',')
  }
  if (_.has(record, 'field_keywords.und')) {
    p7item.Keywords = record.field_keywords.und.map(item => item.tid).join(',')
  }
  if (_.has(record, 'field_show_season_episode.und[0]')) {
    p7item.Show = _.get(record, 'field_show_season_episode.und[0].show', '-')
    p7item.Season = _.get(record, 'field_show_season_episode.und[0].season', '-')
    p7item.Episode = _.get(record, 'field_show_season_episode.und[0].episode', '-')
  }
  // console.log(`Record ${index} =`, util.propstr(p7item, ['PK', 'SK']))
  console.log(`Record ${index || ''} =`, util.obj2str(p7item)); console.log('');
  return p7item
}
