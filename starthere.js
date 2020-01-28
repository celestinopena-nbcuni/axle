const AWS = require('aws-sdk'),
  path = require('path'),
  fs = require('fs'),
  YAML = require('yaml'),
  dbConfigs = require('./db-configs')

// showCredentials()
const cmdlineParams = arg(1)
if (!cmdlineParams) {
  console.log('usage: node starthere.js [C | Q]');
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
        // createTable(dbConfigs.telemundo)
        console.log('Telemundo cfg', dbConfigs.telemundo);
      } else {
        queryDB()
      }
    }
  })
}

function createTable(params, tablename) {
  console.log(`Create table "${tablename}"`);
  const dynamodb = new AWS.DynamoDB();
  dynamodb.createTable(params, function(err, data) {
    if (err) {
      console.error('Unable to create table. Error JSON:', JSON.stringify(err, null, 2));
    } else {
      console.log('Created table. Table description JSON:', JSON.stringify(data, null, 2));
    }
  })
}

function queryDB() {
  const docClient = new AWS.DynamoDB.DocumentClient()
  // Initialize parameters needed to call DynamoDB
  const params = {
    TableName: 'tlmd-crud-dynamodb-taximg-dqstore',
    Key: {
      'dqid': '8155'
    } // ,  ProjectionExpression: 'filter'
  }
  docClient.get(params, function(err, data) {
    if (err) {
      console.error('Unable to read item. Error JSON:', JSON.stringify(err, null, 2));
    } else {
      console.log('GetItem succeeded:', JSON.stringify(data, null, 2));
    }
  })
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

function obj2str(obj) { return JSON.stringify(obj, null, 2) }
function arg(i) {
  if (i<0 || i>=process.argv.length-1) return '';
  return process.argv[i+1];
}
