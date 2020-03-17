/* tlmdQ -- a module for querying tlmdCW table */
const AWS = require('aws-sdk'),
  dbConfigs = require('./db-configs'),
  dbQuery = require('./tableQuery'),
  dbClient = require('./dbClient').init('us-east-1', 'http://localhost:8000'),
  util = require('./general')

const cwQuery = dbQuery.init(dbConfigs.tlmdCW)
const maintable = cwQuery.getQueryParams()
const indexes = {
  'Q1': cwQuery.getLocalIndexQuery('Itemtype'),
  'Q2': cwQuery.getGlobalIndexQuery('GSI1')
}

const cmdlineParams = util.arg(1).toUpperCase()
if (!cmdlineParams) {
  console.log('tlmdQ - Query the TlmdCW table');
  console.log('usage: node tlmdQ.js options');
  console.log(' Options');
  console.log('  Q pk sk op = query table by PK and SK using an operator, E,B, or C');
  console.log('  Q1 pk sk op = query GSI 1 index');
  console.log('  Q2 pk sk op = query GSI 2 index');
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
      if (cmd==='Q') { queryTable(pk, sk, op) }
      else if (cmd==='Q1') { queryTable(pk, sk, op, indexes.Q1) }
      else if (cmd==='Q2') { queryTable(pk, sk, op, indexes.Q2) }
      else if (cmd==='X') {
        console.log('Query translated:', indexes.Q1.eq(pk, null).project('pk, sk, GSI1-PK, GSI1-SK').filter('begins_with(#gsi1SK, :gsi1SK)', 'gsi1SK', sk).get())
      }
      else console.log('Unrecognized option:', cmd);
    } else {
      const stats = await dbClient.createTable(dbConfigs.tlmdCW)
      console.log(`Table ${dbConfigs.tlmdCW.TableName} created:`, stats);
    }
  } catch (err) {
    console.log('Unable to query table:', err);
  }
}

async function queryTable(pkvalue, skvalue, comparison = 'E', secIndex) {
  if (!pkvalue) { console.log('Please provide PK value'); return; }
  const sortKeyDisplay = (skvalue || 'No sort key value provided')
  const query = getQuery(pkvalue, skvalue, comparison, secIndex)
  const qparams = query.get()
  const descrip =  query.explain()
  if (secIndex) console.log('Using', secIndex.toString());
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

function getQuery(pkvalue, skvalue, comparison, secIndex) {
  const op = comparison.toUpperCase()
  if (secIndex) {
    if (op==='E') return secIndex.eq(pkvalue, skvalue)
    else if (op==='B') return secIndex.beginsWith(pkvalue, skvalue)
    else if (op==='C') return secIndex.contains(pkvalue, skvalue)
    else return secIndex.eq(pkvalue, skvalue)
  } else {
    if (op==='E') return maintable.eq(pkvalue, skvalue)
    else if (op==='B') return maintable.beginsWith(pkvalue, skvalue)
    else if (op==='C') return maintable.contains(pkvalue, skvalue)
    else return maintable.eq(pkvalue, skvalue)
  }
}
