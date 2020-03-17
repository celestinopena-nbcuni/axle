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
      if (cmd==='Q') {
        queryTable(pk, sk, op?op.toUpperCase():null)
      }
      else if (cmd==='Q1') {
        queryTable(pk, sk, op?op.toUpperCase():null, indexes.Q1)
      }
      else if (cmd==='Q2') {
        console.log('Using', indexes.Q2.toString());
        queryTable(pk, sk, op?op.toUpperCase():null, indexes.Q2)
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

async function queryTable(pkvalue, skvalue, comparison = 'E', secIndex) {
  if (!pkvalue) { console.log('Please provide PK value'); return; }
  const sortKeyDisplay = (skvalue || 'No sort key value provided')
  const query = getQuery(pkvalue, skvalue, comparison, secIndex)
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

function getQuery(pkvalue, skvalue, comparison, secIndex) {
  if (secIndex) {
    if (comparison==='E') return secIndex.eq(pkvalue, skvalue)
    else if (comparison==='B') return secIndex.beginsWith(pkvalue, skvalue)
    else if (comparison==='C') return secIndex.contains(pkvalue, skvalue)
    else return secIndex.eq(pkvalue, skvalue)
  } else {
    if (comparison==='E') return maintable.eq(pkvalue, skvalue)
    else if (comparison==='B') return maintable.beginsWith(pkvalue, skvalue)
    else if (comparison==='C') return maintable.contains(pkvalue, skvalue)
    else return maintable.eq(pkvalue, skvalue)
  }
}
