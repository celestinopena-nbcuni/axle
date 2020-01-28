const AWS = require("aws-sdk"),
  path = require('path'),
  fs = require('fs'),
  YAML = require('yaml')


AWS.config.getCredentials(function(err) {
  if (err) console.log(err.stack);
  // credentials not loaded
  else {
    console.log("Access key:", AWS.config.credentials.accessKeyId);
    console.log("Secret access key:", AWS.config.credentials.secretAccessKey);
    AWS.config.update({region: 'us-east-1'});
    console.log('Region', AWS.config.region, JSON.stringify({
      dir: __dirname,
      filespec: path.join(__dirname, '/settings.yaml'),
      filetxt: readFile('./settings.yaml')
    }, null, 2));
    console.log('Config', readConfig('./settings.yaml'));
  }
});

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

function showObj(obj) { JSON.stringify(obj, null, 2) }

/*
function hitDB(params) {
  // Initialize parameters needed to call DynamoDB
  var slotParams = {
    Key : {'slotPosition' : {N: '0'}},
    TableName : 'slotWheels',
    ProjectionExpression: 'imageFile'
  };

  // prepare request object for call to DynamoDB
  var request = new AWS.DynamoDB({region: 'us-west-2', apiVersion: '2012-08-10'}).getItem(slotParams);
  // log the name of the image file to load in the slot machine
  request.on('success', function(response) {
    // logs a value like "cherries.jpg" returned from DynamoDB
    console.log(response.data.Item.imageFile.S);
  });
  // submit DynamoDB request
  request.send();
}
*/