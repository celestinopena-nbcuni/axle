const AWS = require('aws-sdk'),
  path = require('path'),
  fs = require('fs'),
  YAML = require('yaml'),
  _ = require('lodash'),
  dbConfigs = require('./db-configs')

const cmdlineParams = arg(1)
if (!cmdlineParams) {
  console.log('usage: node starthere.js [C | L | Q]');
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
        // console.log('Telemundo cfg', dbConfigs.telemundo);
        createTable(dbConfigs.telemundo, 'TelemundoContent')
      } else if (cmd==='L') {
        // loadMoviesTable()
        loadTelemundoTable()
      } else {
        queryTable('Movies', {
          Key: {
            'year': 2000,
            'title': 'Castaway'
          }
        })
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

function loadTelemundoTable() {
  const docClient = new AWS.DynamoDB.DocumentClient();
  console.log('Importing P7 data into DynamoDB...');
  const p7content = readObject('./data/p7data.json')
  if (p7content) {
    p7content.forEach(inspectP7)
    /*
    p7content.forEach(function(record) {
      var params = {
        TableName: 'TelemundoContent',
        Item: {
          'type':  record.type,
          'status': record.status,
          'series':  record.series
        }
      };
      docClient.put(params, function(err, data) {
        if (err) {
          console.error('Unable to add P7 record', record.type, '. Error JSON:', JSON.stringify(err, null, 2));
        } else {
          console.log('Putitem succeeded:', record.type);
        }
      });
    })
    */
  } // insert each P7 record
}

function inspectP7(record, index) {
  console.log('Item at index', index, record.type);
  if (_.has(record, 'field_byline.und[0]')) {
    console.log('Byline', record.field_byline.und[0].value)
  } else {
    console.log(`Record ${index} has no byline`);
  }
  if (_.has(record, 'field_categories.und')) {
    console.log('Categories', record.field_categories.und.map(item => item.tid).join(','))
  } else {
    console.log(`Record ${index} has no categories`);
  }
}

function queryTable(tablename, options) {
  const docClient = new AWS.DynamoDB.DocumentClient()
  // Initialize parameters needed to call DynamoDB
  let params = {
    TableName: tablename
  }
  Object.keys(options).forEach(key => params[key] = options[key])
  docClient.get(params, function(err, data) {
    if (err) {
      console.error('Unable to read item. Error JSON:', JSON.stringify(err, null, 2));
    } else {
      console.log('GetItem succeeded:', JSON.stringify(data, null, 2));
    }
  })
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
