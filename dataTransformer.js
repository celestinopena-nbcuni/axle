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
  function setObjectKeyByItemtype(payload, keyfields = []) {
    let record = Object.keys(payload).reduce((orig, curr) => {
      if (keyfields.includes(curr)) orig[curr] = payload[curr]
      else                          orig.data[curr] = payload[curr]
      return orig
    }, { data: {} })
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
    setObjectKeyByItemtype: setObjectKeyByItemtype
  }
}
module.exports = {
  init: init,
  version: '0.0.1'
}