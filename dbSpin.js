/* dbSpin -- a module for creating, loading, and querying a given table */
const dbsession = require('./dbSession'),
  util = require('./general')

const dbConfigFile = util.arg(1)
if (dbConfigFile) init(dbConfigFile)
else              showhelp()

async function init(filename) {
  console.log('dbSpin >>', new Date().toLocaleString());
  const filepath = !filename.startsWith('.') ? './'+filename : filename
  let dbConfig = null
  try {
    dbConfig = require(filepath)
    useDb(dbsession.init(dbConfig))
  }
  catch (err) {
    console.log(`Error reading config ${filepath}`, err.code);
  }
}

async function useDb(sess) {
  const cmd = util.arg(2).toUpperCase()
  const pk=util.arg(3)
  const sk=util.arg(4)
  const op=util.arg(5)
  try {
    const tableCreated = await sess.tableExists()
    if (tableCreated) {
      if (!cmd) showhelp()
      else if (cmd==='Q') { sess.queryTable(pk, sk, op) }
      else if (cmd==='Q1') { sess.queryLocalIndex(pk, sk, op, 0) }
      else if (cmd==='Q2') { sess.queryGlobalIndex(pk, sk, op, 0) }
      else if (cmd==='D') { sess.deleteTable() }
      else if (cmd==='L') { sess.loadTable(util.arg(3)) }
      else if (cmd==='I') { sess.listIndexes() }
      else if (cmd==='R') { sess.reinitTable() }
      else if (cmd==='V') { console.log('Db config', util.obj2str(sess.getConfig())); }
      else if (cmd==='X') { sess.listTables(); }
      else console.log('Unrecognized option:', cmd);
    } else {
      sess.createTable().then(function() {
        sess.listTables()
      })
    }
  } catch (err) {
    console.log('Unable to query table:', err);
  }
}

function showhelp() {
  console.log('dbSpin - Create, load, and query a table');
  console.log('usage: node dbSpin.js dbConfigFile options');
  console.log(' Options');
  console.log('  R = re-init table');
  console.log('  D = delete table');
  console.log('  L filename = load data from file');
  console.log('  I = list indexes on this table');
  console.log('  V = view table config');
  console.log('  Q pk sk op = query table by PK and SK using an operator, E,B, or C');
  console.log('  Q1 pk sk op = query GSI 1 index');
  console.log('  Q2 pk sk op = query GSI 2 index');
}
