/* This library provides an object which facilitates the generation of query parameter objects. */
function init(dbConfig) {
  function getTable() { return dbConfig.TableName; }
  function getTablePK() {
    const key = dbConfig.KeySchema.find(item => item.KeyType=='HASH')
    return key ? key.AttributeName : null
  }
  function getTableSK() {
    const key = dbConfig.KeySchema.find(item => item.KeyType=='RANGE')
    return key ? key.AttributeName : null
  }
  function getTableQuery(pkvalue, skvalue, projection) {
    const pk = getTablePK() || 'pk'
    const sk = getTableSK() || 'sk'
    let config = {
      TableName: dbConfig.TableName,
      // KeyConditionExpression: skvalue ? '#pk = :pk AND #sk = :sk' : '#pk = :pk',
      KeyConditionExpression: skvalue ? '#pk = :pk AND begins_with(#sk, :sk)' : '#pk = :pk',
      ExpressionAttributeNames: skvalue ? {'#pk': pk, '#sk': sk} : {'#pk': pk},
      ExpressionAttributeValues: skvalue ? {':pk': pkvalue, ':sk': skvalue} : {':pk': pkvalue}
    }
    if (projection) config.ProjectionExpression = projection
    return config
  }
  function getLocalIndexQuery(indexName) {
    if (!dbConfig.LocalSecondaryIndexes) return null
    const locIdx = dbConfig.LocalSecondaryIndexes.find(item => item.IndexName===indexName)
    if (!locIdx) return null
    return configureIndex(locIdx)
  }

  function getGlobalIndexQuery(indexName) {
    if (!dbConfig.GlobalSecondaryIndexes) return null
    const gsi = dbConfig.GlobalSecondaryIndexes.find(item => item.IndexName===indexName)
    if (!gsi) return null
    return configureIndex(gsi)
  }

  function configureIndex(index) {
    const hkey = index.KeySchema.find(item => item.KeyType=='HASH')
    const skey = index.KeySchema.find(item => item.KeyType=='RANGE')
    const pk = hkey ? hkey.AttributeName : null
    const sk = skey ? skey.AttributeName : null
    if (!pk) return null
    const pkPlaceholder = `#${pk}`
    const skPlaceholder = `#${sk}`
    let config = {
      TableName: dbConfig.TableName,
      IndexName: index.IndexName,
      ExpressionAttributeNames: {}
    }
    function getParams() { return config }
    function addAttributeName(name, value) {
      const key = '#' + name
      config.ExpressionAttributeNames[key] = name
      if (value) {
        const valueKey = ':' + name
        config.ExpressionAttributeValues[valueKey] = value
      }
    }
    function eq(pkvalue, skvalue) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: index.IndexName,
        ExpressionAttributeNames: {}
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
      return params
    }
    function beginsWith(pkvalue, skvalue) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: index.IndexName,
        ExpressionAttributeNames: {}
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
      return params
    }
    function contains(pkvalue, skvalue) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: index.IndexName,
        ExpressionAttributeNames: {}
      }
      params.ExpressionAttributeNames[pkPlaceholder] = pk
      if (skvalue) {
        params.ExpressionAttributeNames[skPlaceholder] = sk
        params.KeyConditionExpression = `${pkPlaceholder} = :pk AND contains(${skPlaceholder}, :sk)`
        params.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
      } else {
        params.KeyConditionExpression = `${pkPlaceholder} = :pk`
        params.ExpressionAttributeValues = { ':pk': pkvalue }
      }
      return params
    }
    function eqChain(pkvalue, skvalue) {
      config = eq(pkvalue, skvalue)
      return this
    }
    function beginsWithChain(pkvalue, skvalue) {
      config = beginsWith(pkvalue, skvalue)
      return this
    }
    function containsChain(pkvalue, skvalue) {
      config = contains(pkvalue, skvalue)
      return this
    }
    function addAttributeNameChain(name, value) {
      addAttributeName(name, value)
      return this
    }
    function filterChain(expr, name, value) {
      config.FilterExpression = expr // '#status = :stat'
      addAttributeName(name, value)
      return this
    }
    function projectChain(projection) {
      if (projection) config.ProjectionExpression = projection
      return this
    }
    function toString() {
      return `Index ${index.IndexName} contains key ${pk}, ${sk}`
    }
    // Provide a plain language description of the query parameter object
    function explain() {
      const kce = Object.keys(config.ExpressionAttributeNames).reduce((orig, curr) => {
        return orig.replace(curr, config.ExpressionAttributeNames[curr])
      }, config.KeyConditionExpression)
      let criteria = Object.keys(config.ExpressionAttributeValues).reduce((orig, curr) => { return orig.replace(curr, `'${config.ExpressionAttributeValues[curr]}'`) }, kce)
      let description = `Search ${config.TableName}.${config.IndexName} WHERE ${criteria}`
      if (config.FilterExpression) {
        const filter = Object.keys(config.ExpressionAttributeNames).reduce((orig, curr) => { return orig.replace(curr, `'${config.ExpressionAttributeNames[curr]}'`) }, config.FilterExpression)
        criteria = Object.keys(config.ExpressionAttributeValues).reduce((orig, curr) => { return orig.replace(curr, `'${config.ExpressionAttributeValues[curr]}'`) }, filter)
        description += ` FILTER ON ${criteria}`
      }
      if (config.ProjectionExpression) description += ` SHOWING ONLY ${config.ProjectionExpression}`
      return description
    }
    return {
      eq: eq,
      beginsWith: beginsWith,
      contains: contains,
      $eq: eqChain,
      $beginsWith: beginsWithChain,
      $contains: containsChain,
      $filter: filterChain,
      $project: projectChain,
      $addAttribute: addAttributeNameChain,
      getParams: getParams,
      toString: toString,
      explain: explain
    }
  }
  return {
    getLocalIndexQuery: getLocalIndexQuery,
    getGlobalIndexQuery: getGlobalIndexQuery,
    getTableQuery: getTableQuery,
    getTable: getTable
  }
} // init
module.exports = { init: init }
