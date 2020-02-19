const AWS = require('aws-sdk'),
  path = require('path'),
  fs = require('fs'),
  YAML = require('yaml'),
  _ = require('lodash'),
  dbConfigs = require('./db-configs'),
  dbQuery = require('./tableQuery'),
  util = require('./general')

const filmQuery = dbQuery.init(dbConfigs.films)
const localIndex = filmQuery.getLocalIndexQuery('LOC-1')

const cmdlineParams = util.arg(1).toUpperCase()
if (!cmdlineParams) {
  console.log('usage: node starthere.js options');
  console.log(' Options');
  console.log('  C = create table');
  console.log('  D = delete table');
  console.log('  L filename = load the given json file into the database');
  console.log('  R filename = read the given json file');
  console.log('  Q pk sk = query table by PK and SK');
  console.log('  QL pk sk = query local index by PK and SK');
} else {
  // showCredentials()
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
        const tableConfig=arg(2)
        if (tableConfig) createTable(readObject(tableConfig), arg(3))
        else createTable(dbConfigs.films, 'Films')
      } else if (cmd==='D') {
        deleteTable('Films')
      } else if (cmd==='L') {
        // loadMoviesTable()
        const datafile = arg(2)
        if (datafile) loadTable(datafile, 'Films')
      } else if (cmd==='R') {
        const datafile = arg(2)
        if (datafile) readTelemundoDatafile(datafile, (arg(3) ? false : true))
      } else if (cmd==='Q') {
        const pk=util.arg(2)
        const sk=util.arg(3)
        // let params = { TableName: 'Films', Key: { 'actor': pk }}; if (sk) params.Key.film = sk;
        let qparams = {
          TableName: 'Films',
          KeyConditionExpression: sk ? '#pk = :pk AND #sk = :sk' : '#pk = :pk',
          ExpressionAttributeNames: sk ? {'#pk': 'actor', '#sk': 'film'} : {'#pk': 'actor'},
          ExpressionAttributeValues: sk ? {':pk': pk, ':sk': sk} : {':pk': pk}
        }
        queryTable(qparams)
      } else if (cmd==='QL') {
        const pk=util.arg(2)
        const sk=Number(util.arg(3))
        const idx = localIndex.$eq(pk, sk)
        console.log(idx.explain());
        queryIndex(idx.getParams())
      } else {
        console.log('Unrecognized option:', cmd);
      }
    }
  })
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

function loadTable(datafile, tablename) {
  const content = readObject(datafile)
  if (!content) {
    console.log('Problem reading', datafile)
    return
  }
  const docClient = new AWS.DynamoDB.DocumentClient();
  if (Array.isArray(content)) {
    console.log(`Importing data in ${datafile} into ${tablename}...`)
    content.forEach(function(record, index) {
      docClient.put({
        TableName: tablename,
        Item: record
      }, function(err, data) {
        if (err) console.error('Unable to add record. Error JSON:', obj2str(err), record);
        else console.log('Putitem succeeded for record', index);
      })
    })
  } else {
    docClient.put({
      TableName: tablename,
      Item: content
    }, function(err, data) {
      if (err) console.error('Unable to add record. Error JSON:', obj2str(err));
      else console.log('Putitem succeeded for single record');
    })
  }
}

function queryTable(qparams) {
  const docClient = new AWS.DynamoDB.DocumentClient()
  // Initialize parameters needed to call DynamoDB
  // docClient.get(qparams, function(err, data) {
  docClient.query(qparams, function(err, data) {
    if (err) {
      console.error('Unable to read item. Error JSON:', JSON.stringify(err, null, 2));
    } else {
      console.log('GetItem succeeded:', JSON.stringify(data, null, 2));
    }
  })
}

function queryIndex1(tablename, pkvalue, skvalue) {
  if (!pkvalue) { console.log('Please provide PK value'); return; }
  const params2 = {
    TableName: tablename,
    IndexName: 'LOC-1',
    KeyConditionExpression: 'actor = :a and #year = :y',
    ExpressionAttributeNames: { '#year': 'year' },
    ExpressionAttributeValues: { ':a': pkvalue, ':y' : Number(skvalue) }
  }
  const docClient = new AWS.DynamoDB.DocumentClient();
  docClient.query(params2, function (err, data) {
    console.log('Query secondary index by:', pkvalue, skvalue);
    if (err) console.log('Error getting item by PK+SK', pkvalue, skvalue, obj2str(err));
    else if (data.Items) console.log('Got item by PK', pkvalue, skvalue, data.Items);
    else console.log('NO item by PK', pkvalue, skvalue, params, data);
  })
}

function queryIndex(queryParams) {
  const docClient = new AWS.DynamoDB.DocumentClient();
  docClient.query(queryParams, function (err, data) {
    console.log('Query index by:', queryParams);
    if (err) console.log('Error getting item by key', util.obj2str(err));
    else if (data.Items) console.log('Got item by key', data.Items);
    else console.log('NO item found by this index');
  })
}

function deleteTable(tablename) {
  console.log(`Removing the ${table} table...`);
  const dynamodb = new AWS.DynamoDB();
  dynamodb.deleteTable({TableName: tablename}, function(err, data) {
    if (err) {
      console.error('Unable to delete table. Error JSON:', obj2str(err));
    } else {
      console.log('Deleted table.');
    }
  })
}

function loadTelemundoTable(datafile, doTransform = true) {
  const p7content = readObject(datafile)
  if (!p7content) {
    console.log('Problem reading', datafile)
    return
  }
  const docClient = new AWS.DynamoDB.DocumentClient();
  if (Array.isArray(p7content)) {
    console.log(`Importing P7 data in ${datafile} into DynamoDB (transform data? ${doTransform?'Y':'N'})...`)
    p7content.forEach(function(record, index) {
      docClient.put({
        TableName: 'TelemundoContent',
        Item: doTransform ? transformP7(record) : record
      }, function(err, data) {
        if (err) console.error('Unable to add P7 record. Error JSON:', obj2str(err));
        else console.log('Putitem succeeded for record', index);
      })
    })
  } else {
    docClient.put({
      TableName: 'TelemundoContent',
      Item: doTransform ? transformP7(p7content) : p7content
    }, function(err, data) {
      if (err) console.error('Unable to add P7 record. Error JSON:', obj2str(err));
      else console.log('Putitem succeeded for single record');
    })
  }
}

function readTelemundoDatafile(datafile, doTransform = true) {
  const p7content = readObject(datafile)
  if (!p7content) {
    console.log('Problem reading', datafile)
    return
  } else if (Array.isArray(p7content)) {
    console.log(`Read import data in ${datafile} (do transform? ${doTransform?'Y':'N'})...`)
    p7content.forEach(function(record, index) {
      if (doTransform) transformP7(record, index)
      else console.log('Record', index, obj2str(record));
    })
  } else {
    console.log(`Read import object in ${datafile} (do transform? ${doTransform?'Y':'N'})...`)
    if (doTransform) transformP7(p7content, 0)
    else console.log('Record', obj2str(p7content));
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
  const params = {
    TableName: 'TelemundoContent',
    IndexName: 'GSI-1',
    KeyConditionExpression: 'child = :c and nid = :n',
    ExpressionAttributeValues: {
      ':c': {S: pkvalue},
      ':n' : {S: skvalue}
    },
    ProjectionExpression: 'child, nid'
  }
  const params2 = {
    TableName: 'TelemundoContent', IndexName: 'GSI-1',
    KeyConditionExpression: 'child = :c and begins_with(#ctype, :t) ',
    ExpressionAttributeNames: { '#ctype': 'ctype'},
    ExpressionAttributeValues: {
     ':c': {S: pkvalue},
     ':t': {S: skvalue}
    },
    ProjectionExpression: 'child, ctype'
  }
  // const docClient = new AWS.DynamoDB.DocumentClient();
  const docClient = new AWS.DynamoDB();
  docClient.query(params2, function (err, data) {
    console.log('Query secondary index by:', pkvalue, skvalue);
    if (err) console.log('Error getting item by PK+SK', pkvalue, skvalue, obj2str(err));
    else if (data.Item) console.log('Got item by PK', pkvalue, skvalue, data.Item);
    else console.log('NO item by PK', pkvalue, skvalue);
  })
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

function loadMoviesTable() {
  const docClient = new AWS.DynamoDB.DocumentClient();
  console.log('Importing movies into DynamoDB. Please wait.');
  const allMovies = readObject('./data/moviedata.json')
  if (allMovies) {
    allMovies.forEach(function(movie) {
      var params = {
        TableName: 'Movies',
        Item: {
          'year':  movie.year,
          'title': movie.title,
          'info':  movie.info
          }
      };
      docClient.put(params, function(err, data) {
        if (err) {
          console.error('Unable to add movie', movie.title, '. Error JSON:', JSON.stringify(err, null, 2));
        } else {
          console.log('PutItem succeeded:', movie.title);
        }
      });
    })
  } // insert each movie
}

function getQParams(tablename, keymap) {
  let params = {
    TableName: tablename,
    Key: {}
  }
  Object.keys(keymap).forEach(attr => params.Key[attr] = keymap[attr])
}

function showCredentials() {
  AWS.config.getCredentials(function(err) {
    if (err) console.log(err.stack);
    // credentials not loaded
    else {
      console.log('Access key:', AWS.config.credentials.accessKeyId);
      console.log('Secret access key:', AWS.config.credentials.secretAccessKey);
      AWS.config.update({region: 'us-east-1'});
      console.log('Region', AWS.config.region, obj2str({
        dir: __dirname,
        filespec: path.join(__dirname, '/settings.yaml'),
        filetxt: readFile('./settings.yaml')
      }))
      console.log('Config', readConfig('./settings.yaml'))
    }
  })
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
