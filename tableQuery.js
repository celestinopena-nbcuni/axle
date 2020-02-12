/* This library provides an object which facilitates the generation of query parameter objects. */
function init(dbConfig) {
  function getIndexQuery(indexName) {
    const gsi = dbConfig.GlobalSecondaryIndexes.find(item => item.IndexName===indexName)
    if (!gsi) return null
    const hkey = gsi.KeySchema.find(item => item.KeyType=='HASH')
    const skey = gsi.KeySchema.find(item => item.KeyType=='RANGE')
    const pk = hkey ? hkey.AttributeName : null
    const sk = skey ? skey.AttributeName : null
    if (!pk) return null
    const pkPlaceholder = `#${pk}`
    const skPlaceholder = `#${sk}`
    function eq(pkvalue, skvalue, projection) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: indexName,
        ExpressionAttributeNames: {
          '#uuid': 'uuid'
        }
      }
      params.ExpressionAttributeNames[pkPlaceholder] = pk
      if (skvalue) {
        params.ExpressionAttributeNames[skPlaceholder] = sk
        params.KeyConditionExpression = `${pkPlaceholder} = :pk AND ${skPlaceholder} = :sk`
        params.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
      } else {
        params.KeyConditionExpression = `${pkPlaceholder} = :pk`
        params.ExpressionAttributeValues = { ':pk': pkvalue }
      }
      if (projection) params.ProjectionExpression = projection
      return params
    }
    function beginsWith(pkvalue, skvalue, projection) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: indexName,
        ExpressionAttributeNames: {'#uuid': 'uuid'}
      }
      params.ExpressionAttributeNames[pkPlaceholder] = pk
      if (skvalue) {
        params.ExpressionAttributeNames[skPlaceholder] = sk
        params.KeyConditionExpression = `${pkPlaceholder} = :pk AND begins_with(${skPlaceholder}, :sk)`
        params.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
      } else {
        params.KeyConditionExpression = `${pkPlaceholder} = :pk`
        params.ExpressionAttributeValues = { ':pk': pkvalue }
      }
      if (projection) params.ProjectionExpression = projection
      return params
    }
    function toString() {
      return `Index ${indexName} containing key ${pk}, ${sk}`
    }
    return {
      eq: eq,
      beginsWith: beginsWith,
      toString: toString
    }
  } // getIndexQuery
  return { getIndexQuery: getIndexQuery }
} // init
module.exports = { init: init }
