module.exports = (function () {
  function init(dbConfig) {
    function getIndexQuery(indexName) {
      const gsi = dbConfig.GlobalSecondaryIndexes.find(item => item.IndexName===indexName)
      if (!gsi) return null
      const hkey = gsi.KeySchema.find(item => item.KeyType=='HASH')
      const skey = gsi.KeySchema.find(item => item.KeyType=='RANGE')
      const pk = hkey ? hkey.AttributeName : null
      const sk = skey ? skey.AttributeName : null
      if (!(pk && sk)) return null
      const pkPlaceholder = `#${pk}`
      const skPlaceholder = `#${sk}`
      let config = {
        TableName: dbConfig.TableName,
        IndexName: indexName,
        ExpressionAttributeNames: {}
      }
      config.ExpressionAttributeNames[pkPlaceholder] = pk
      config.ExpressionAttributeNames[skPlaceholder] = sk
      function beginsWith(pkvalue, skvalue) {
        if (skvalue) {
          config.KeyConditionExpression = `${pkPlaceholder} = :pk AND begins_with(${skPlaceholder}, :sk)`
          config.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
        } else {
          config.KeyConditionExpression = `${pkPlaceholder} = :pk`
          config.ExpressionAttributeValues = { ':pk': pkvalue }
        }
        return config
      }
      function contains(pkvalue, skvalue) {
        if (skvalue) {
          config.KeyConditionExpression = `${pkPlaceholder} = :pk AND contains(${skPlaceholder}, :sk)`
          config.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
        } else {
          config.KeyConditionExpression = `${pkPlaceholder} = :pk`
          config.ExpressionAttributeValues = { ':pk': pkvalue }
        }
        return config
      }
      function endsWith(pkvalue, skvalue) {
        if (skvalue) {
          config.KeyConditionExpression = `${pkPlaceholder} = :pk AND ends_with(${skPlaceholder}, :sk)`
          config.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
        } else {
          config.KeyConditionExpression = `${pkPlaceholder} = :pk`
          config.ExpressionAttributeValues = { ':pk': pkvalue }
        }
        return config
      }
      return {
        beginsWith: beginsWith,
        contains: contains,
        endsWith: endsWith
      }
    } // getIndexQuery
    return { getIndexQuery: getIndexQuery }
  } // init
  return { init: init }
})()