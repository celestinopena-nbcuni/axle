/* This library contains generic utility functions */
const util = require('./general')

function init(dbConfig) {
  const PK = getTablePK()
  const SK = getTableSK()
  function getTablePK() {
    const key = dbConfig.KeySchema.find(item => item.KeyType=='HASH')
    return key ? key.AttributeName : null
  }
  function getTableSK() {
    const key = dbConfig.KeySchema.find(item => item.KeyType=='RANGE')
    return key ? key.AttributeName : null
  }
  function getIndexPK(indexName) {
    if (!dbConfig.GlobalSecondaryIndexes) return null
    const idx = dbConfig.GlobalSecondaryIndexes.find(item => item.IndexName===indexName)
    return idx ? idx.KeySchema.find(item => item.KeyType=='HASH').AttributeName : null
  }
  function getIndexSK(indexName) {
    if (!dbConfig.GlobalSecondaryIndexes) return null
    const idx = dbConfig.GlobalSecondaryIndexes.find(item => item.IndexName===indexName)
    const key = idx.KeySchema.find(item => item.KeyType=='RANGE')
    return key ? key.AttributeName : null
  }
  function getLocalIndexPK(indexName) {
    if (!dbConfig.LocalSecondaryIndexes) return null
    const idx = dbConfig.LocalSecondaryIndexes.find(item => item.IndexName===indexName)
    return idx ? idx.KeySchema.find(item => item.KeyType=='HASH').AttributeName : null
  }
  function getLocalIndexSK(indexName) {
    if (!dbConfig.LocalSecondaryIndexes) return null
    const idx = dbConfig.LocalSecondaryIndexes.find(item => item.IndexName===indexName)
    const key = idx.KeySchema.find(item => item.KeyType=='RANGE')
    return key ? key.AttributeName : null
  }
  // Make a copy of an object and set its PK and SK to the same value
  function setPrimaryRecord(record, idfield) {
    const newobj = util.copy(record)
    newobj[PK] = record[idfield]
    newobj[SK] = record[idfield]
    delete newobj[idfield]
    return newobj
  }
  function setPK(payload, pkvalue) {
    const newobj = util.copy(payload)
    newobj[PK] = pkvalue
    return newobj
  }
  function setSK(payload, skvalue) {
    const newobj = util.copy(payload)
    if (skvalue) newobj[SK] = skvalue
    return newobj
  }
  function setObjectKey(payload, pkfield, skfield, keyfields) {
    const newobj = setRootFields(payload, keyfields)
    newobj[PK] = payload[pkfield]
    if (skfield) newobj[SK] = payload[skfield]
    return newobj
  }
  function setRootFields(payload, keyfields = []) {
    return Object.keys(payload).reduce((orig, curr) => {
      if (keyfields.includes(curr)) orig[curr] = payload[curr]
      else                          orig.data[curr] = payload[curr]
      return orig
    }, { data: {} })
  }
  function setObjectKeyByItemtype(payload, keyfields = []) {
    let record = setRootFields(payload, keyfields)
    if (payload.itemType==='node') {
      record[PK] = payload.uuid
      record[SK] = `${util.convertUnixDate(payload.datePublished)}#${payload.itemType}`
    }
    else {
      record[PK] = payload.uuid
      record[SK] = (payload.childUuid || payload.title)
    }
    const gsiPK = getIndexPK('GSI1')
    const gsiSK = getIndexSK('GSI1')
    record[gsiPK] = record[SK]
    record[gsiSK] = record[PK]
    return record
  }
  return {
    pk: PK,
    sk: SK,
    getIndexPK: getIndexPK,
    getIndexSK: getIndexSK,
    getLocalIndexPK: getLocalIndexPK,
    getLocalIndexSK: getLocalIndexSK,
    setPK: setPK,
    setSK: setSK,
    setPrimaryRecord: setPrimaryRecord,
    setRootFields: setRootFields,
    setObjectKey: setObjectKey,
    setObjectKeyByItemtype: setObjectKeyByItemtype
  }
}
module.exports = {
  init: init,
  version: '0.0.1'
}