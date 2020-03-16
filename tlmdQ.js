/* tlmdQ -- a module for querying tlmdCW table */
const AWS = require('aws-sdk'),
  dbConfigs = require('./db-configs'),
  dbQuery = require('./tableQuery'),
  dbClient = require('./dbClient').init('us-east-1', 'http://localhost:8000'),
  util = require('./general')

const cwQuery = dbQuery.init(dbConfigs.tlmdCW)
const indexes = {
  'Q1': cwQuery.getLocalIndexQuery('Itemtype'),
  'Q2': cwQuery.getGlobalIndexQuery('GSI1')
}

const cmdlineParams = util.arg(1).toUpperCase()
if (!cmdlineParams) {
  console.log('tlmdQ - Query the TlmdCW table');
  console.log('usage: node tlmdQ.js options');
  console.log(' Options');
  console.log('  Q pk sk = query table by PK and SK');
  console.log('  Q1 pk sk = query GSI 1 index');
  console.log('  Q2 pk sk = query GSI 2 index');
  console.log('  Q3 pk sk = query GSI 3 index');
} else {
  init(cmdlineParams)
}

async function init(cmd) {
  console.log('tlmdQ >>', new Date().toLocaleString());
  const pk=util.arg(2)
  const sk=util.arg(3)
  const op=util.arg(4)
  try {
    const tableExists = await dbClient.hasTable(dbConfigs.tlmdCW.TableName)
    if (tableExists) {
      if (cmd==='Q') {
        queryTable(pk, sk, op?op.toUpperCase():null)
      }
      else if (cmd==='Q1') {
        let idx = indexes.Q1.$eq(pk, sk).$project('#uuid, programUuid, itemType, title, statusDate, slug').$addAttribute('uuid')
        // queryTelemundoIndex(idx.get())
        queryIndex(idx)
      }
      else if (cmd==='Q2') {
        queryIndex(indexes.Q2.$beginsWith(pk, sk).$project('#uuid, programUuid, itemType, title, slug').$addAttribute('uuid'))
      }
      else if (cmd==='Q3') {
        queryIndex(indexes.Q3.$beginsWith(pk, sk).$project('#uuid, programUuid, itemType, title, slug').$addAttribute('uuid'))
      }
      else if (cmd==='X') {
        let qparams = indexes.Q1.$eq(pk, null).$project('title, datePublished, itemType, slug, frontends').$filter('contains(#frontends, :frontends)', 'frontends', sk)
        console.log('Query translated:', qparams.get(), qparams.explain());
      }
      else console.log('Unrecognized option:', cmd);
    } else {
      const stats = await dbClient.createTable(dbConfigs.tlmdCW)
      console.log(`Table ${dbConfigs.tlmdCW.TableName} created:`, stats);
    }
  } catch (err) {
    console.log('Unable to query table');
  }
}

async function queryTable(pkvalue, skvalue, comparison = 'B') {
  if (!pkvalue) { console.log('Please provide PK value'); return; }
  const sortKeyDisplay = (skvalue || 'No sort key value provided')
  const query = getQuery(pkvalue, skvalue, comparison)
  const qparams = query.get()
  const descrip =  query.explain()
  try {
    const data = await dbClient.query(qparams)
    if (data.Items && data.Items.length>0) {
      console.log(`"${descrip}" -> succeeded: ${data.Items.length} objects. Params=`, qparams);
      console.log(util.shortView(data.Items, ['pk', 'sk', 'itemType', 'data']));
    }
    else console.log(`${descrip} found NO objects. Params=`, qparams);
  }
  catch (err) {
    console.log('Error getting item by key', pkvalue, sortKeyDisplay, util.obj2str(err));
  }
}

function getQuery(pkvalue, skvalue, comparison) {
  if (comparison==='E') return cwQuery.getQueryParams().eq(pkvalue, skvalue)
  else if (comparison==='C') return cwQuery.getQueryParams().contains(pkvalue, skvalue)
  else return cwQuery.getQueryParams().beginsWith(pkvalue, skvalue)
}

function queryIndex(queryParams) {
  let paramObj = null
  if (queryParams.get) paramObj = queryParams.get()
  else paramObj = queryParams
  const docClient = new AWS.DynamoDB.DocumentClient();
  docClient.query(paramObj, function (err, data) {
    console.log(queryParams.toString(), JSON.stringify(paramObj,null,2));
    if (queryParams.explain) console.log(queryParams.explain());
    if (err) console.log('Error getting item by key', util.obj2str(err));
    else if (data.Items) console.log(`Got items by key (${data.Items.length})`, data.Items);
    else console.log('NO item found by this index');
  })
}
