const AWS = require('aws-sdk'),
  _ = require('lodash'),
  dbConfigs = require('./db-configs'),
  dbQuery = require('./tableQuery'),
  util = require('./general')

const cjp=require('./data/utils.js')

const mainTable = 'TelemundoCW'
const cwQuery = dbQuery.init(dbConfigs.telemundoCW)
const typeIndex = cwQuery.getLocalIndexQuery('Itemtype')
const typeStatusDateIndex = cwQuery.getGlobalIndexQuery('StatusDate')
const gsi2Index = cwQuery.getGlobalIndexQuery('GSI2')
// const gsi3Index = cwQuery.getGlobalIndexQuery('GSI3')
const cmdlineParams = util.arg(1).toUpperCase()
if (!cmdlineParams) {
  console.log('usage: node tmundo.js options');
  console.log(' Options');
  console.log('  C tablename = create table');
  console.log('  D tablename = delete table');
  console.log('  Demo = run demo scenario');
  console.log('  I = list indexes on this table');
  console.log('  L filename tablename = load the given json file into a table');
  console.log('  R filename = read the given json file');
  console.log('  Q pk sk = query table by PK and SK');
  console.log('  Q1 pk sk = query GSI 1 index');
  console.log('  Q2 pk sk = query GSI 2 index');
  console.log('  Q3 pk sk = query GSI 3 index');
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
      else if (cmd==='I') {
        console.log(typeIndex.toString())
        console.log(gsi2Index.toString())
        // console.log(gsi3Index.toString())
        console.log(typeStatusDateIndex.toString())
      }
      else if (cmd==='Q') {
        const pk=util.arg(2)
        const sk=util.arg(3)
        const shortView = util.arg(4)
        const cols = '#pk, datePublished, itemType, title, slug'
        queryTelemundoTable(pk, sk, shortView?cols:null)
      }
      else if (cmd==='Q1') {
        const pk=util.arg(2) // 'fad8df82-55eb-4452-89e5-c546874a7212'
        const sk=util.arg(3)
        const shortView=util.arg(4)
        console.log('*** Find by uuid, itemType ***');
        let idx = typeIndex.$eq(pk, sk).$project('#uuid, programUuid, itemType, title, statusDate, slug').$addAttribute('uuid')
        // if (shortView) idx.$project('uuid, itemType, title, statusDate, slug')
        console.log(idx.explain());
        queryTelemundoIndex(idx.getParams())
      }
      else if (cmd==='Q2') {
        const pk=util.arg(2)
        const sk=util.arg(3)
        const idx = gsi2Index.$beginsWith(pk)
        console.log(idx.explain());
        queryTelemundoIndex(idx.getParams())
      }
      else if (cmd==='Q3') {
        let gsi = gsi3Index.$beginsWith(util.arg(2), util.arg(3))
        queryTelemundoIndex(gsi.getParams())

        // let idx = typeStatusDateIndex.$beginsWith(util.arg(2), util.arg(3))
        // console.log(idx.explain());
        // if (util.arg(3)) idx.$project('statusDate, itemType, title, slug')
        // queryTelemundoIndex(idx.getParams())
      }
      else if (cmd==='QSF') {
        /*
        let qparams = tlmdQuery.getIndexQuery('SeriesTypeStatus').beginsWith(util.arg(2), util.arg(3))
        qparams.FilterExpression = '#status = :stat'
        qparams.ExpressionAttributeNames['#status'] = 'status'
        qparams.ExpressionAttributeValues[':stat'] = util.arg(4) || '0'
        queryTelemundoIndex(qparams)
        */
      }
      else if (cmd==='X') {
        const pk=util.arg(2)
        const sk=util.arg(3)
        let qparams = typeTitleIndex.$eq(pk, null).$project('title, datePublished, itemType, slug, frontends').$filter('contains(#frontends, :frontends)', 'frontends', util.arg(3))

        // console.log('Our index param obj:', qparams);
        // queryTelemundoIndex(qparams.getParams())
        console.log('Query translated:', qparams.getParams(), qparams.explain());
      }
      else {
        console.log('Unrecognized option:', cmd);
      }
    }
  })
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
  const newitem = doTransform(p7content)
  insertRecord(mainTable, newitem).then(function(data) {
    console.log('Putitem succeeded for single record', newitem.uuid);
    const relatedObjects = (newitem.itemType==='series') ? expandSeriesRelations(newitem) : expandRelations(newitem)
    relatedObjects.forEach(item => {
      item.sourcefile = datafile
      insertRecord(mainTable, item).then(function(data) {
        console.log('  Add related item', item.programUuid);
      }, function(error) {
        console.log('Error adding related objecgt', error);
      })
    })
  }, function(error) {
    console.error('Unable to add record. Error JSON:', util.obj2str(error));
  })
}

function insertRecord(tablename, record) {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.put({ TableName: tablename, Item: record }, function(err, data) { if (err) reject(err); resolve(data) })
  })
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

// Like loadTable() but transform incoming data
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

// Read data only, without actually importing
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
    console.log(`Read import object in ${datafile}`); // , Object.keys(p7content))
    const newRecord = doTransform(p7content)
    if (newRecord.itemType==='series') expandSeriesRelations(newRecord)
    else expandRelations(newRecord)
  }
}

function queryTelemundoTable(pkvalue, skvalue, projection) {
  if (!pkvalue) { console.log('Please provide PK value'); return; }
  const sortKeyDisplay = (skvalue || 'No sort key value provided')
  if (!mainTable) return
  const pk = cwQuery.getTablePK()
  const sk = cwQuery.getTableSK()
  let params = {
    TableName: mainTable,
    // KeyConditionExpression: skvalue ? '#pk = :pk AND #sk = :sk' : '#pk = :pk',
    KeyConditionExpression: skvalue ? '#pk = :pk AND begins_with(#sk, :sk)' : '#pk = :pk',
    ExpressionAttributeNames: skvalue ? {'#pk': pk, '#sk': sk} : {'#pk': pk},
    ExpressionAttributeValues: skvalue ? {':pk': pkvalue, ':sk': skvalue} : {':pk': pkvalue}
  }
  if (projection) params.ProjectionExpression = projection
  console.log('Query table:', params);
  const docClient = new AWS.DynamoDB.DocumentClient();
  docClient.query(params, function (err, data) {
    if (err) console.log('Error getting item by key', pkvalue, sortKeyDisplay, util.obj2str(err));
    else if (data.Items) console.log('Objects found:', data.Items.length);
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

// Provide an abbreviation for a uuid
function abbrevUuid(id) {
  const parts=id.split('-');
  const last=parts.length-1;
  return '...' + parts[last].substr(-6)
}

// Generate new records from a given (series) record
function expandSeriesRelations(record) {
  let ctr = 0
  const categoryTitle = `A category created from ${abbrevUuid(record.uuid)}, a ${record.itemType}`
  const refTitle =  `Reference created from ${abbrevUuid(record.uuid)}, a ${record.itemType}`
  const relatedCategories = record.categories.reduce((orig, curr) => {
    ctr++
    // console.log('id '+record.uuid+' contains', curr);
    orig.push({
      uuid: record.uuid,
      programUuid: curr,
      itemType: 'taxonomy',
      title: categoryTitle,
      datePublished: '2019-12-11',
      publishDateItemtypeTitle: '2019-12-11#taxonomy#'+categoryTitle,
      data: { id: ctr }
    })
    return orig
  }, [])
  const relatedRefs = record.references.reduce((orig, curr) => {
    ctr++
    // console.log('id '+record.uuid+' contains', curr.uuid);
    orig.push({
      uuid: record.uuid,
      programUuid: curr.uuid,
      itemType: curr.itemType,
      title: refTitle,
      datePublished: '2019-12-0'+ctr,
      publishDateItemtypeTitle: `2019-12-0${ctr}#${curr.itemType}#${refTitle}`,
      data: { id: ctr }
    })
    return orig
  }, [])
  return relatedCategories.concat(relatedRefs)
}

// Generate new records from a given record
function expandRelations(record) {
  let ctr = 0
  const tagTitle = `A tag created from ${abbrevUuid(record.uuid)}, a ${record.itemType}`
  const relatedTags = record.tags.reduce((orig, curr) => {
    ctr++
    // console.log('id '+record.uuid+' contains', curr);
    orig.push({
      uuid: record.uuid,
      programUuid: curr,
      itemType: 'taxonomy',
      title: tagTitle,
      datePublished: '2019-11-0'+ctr,
      publishDateItemtypeTitle: `2019-11-0${ctr}#taxonomy#${tagTitle}`,
      data: { id: ctr }
    })
    return orig
  }, [])
  return relatedTags
}

function doTransform(record) {
  const mainFields = util.copyFields(record, ['data', 'itemType', 'uuid', 'slug', 'title', 'media', 'categories', 'tags', 'published', 'relatedSeries', 'currentSeason', 'datePublished', 'references', 'frontends'])
  // mainFields.subtype = _.has(record, 'customFields.bundle') ? record.customFields.bundle : _.has(record, 'customFields.metatags.og:type') ? record.customFields.metatags['og:type'] : 'UNKNOWN'
  if (_.has(record, 'customFields.mpxAdditionalMetadata.mpxCreated')) mainFields.createDT = util.convertUnixDate(record.customFields.mpxAdditionalMetadata.mpxCreated)
  if (_.has(record, 'customFields.mpxAdditionalMetadata.mpxUpdated')) mainFields.updateDT = util.convertUnixDate(record.customFields.mpxAdditionalMetadata.mpxUpdated)

  if (!mainFields.data) {
    mainFields.data = util.copyFields(record, ['title', 'short_description', 'long_description', 'promoDescription', 'promoTitle', 'seriesType', 'promoKicker', 'genre', 'links', 'customFields'])
  }
  mainFields.programUuid = (record.program && record.program.programUuid) ? record.program.programUuid  : record.uuid
  if (mainFields.datePublished) mainFields.datePublished = util.convertUnixDate(mainFields.datePublished)
  // Create composite fields for indexes
  mainFields.statusDate = `${(mainFields.published ? '1' : '0')}#${mainFields.datePublished}`.toUpperCase()
  mainFields.publishDateItemtypeTitle = `${mainFields.datePublished}#${mainFields.itemType}#${mainFields.title}`.toUpperCase()
  return util.noblanks(mainFields)
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

function runDemo(searchTerm) {
  const tlmdQuery = dbQuery.init(dbConfigs.telemundo)
  const gsi = tlmdQuery.getIndexQuery('ChildNode')
  if (gsi) {
    getQueryIndex(gsi.eq('none')).then(function(data) {
      console.log(`Query returned ${data.Items.length} with content type ${searchTerm}`, data.Items.find(item => item.ctype==searchTerm));
    }, function(error) {
      console.log('Query failed:', util.obj2str(error));
    })
  }
}
