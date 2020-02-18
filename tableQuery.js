/* This library provides an object which facilitates the generation of query parameter objects. */
function init(dbConfig) {
  function getLocalIndex(indexName) {
    if (!dbConfig.LocalSecondaryIndexes) return null
    return dbConfig.LocalSecondaryIndexes.find(item => item.IndexName===indexName)
  }

  function getGlobalIndex(indexName) {
    if (!dbConfig.GlobalSecondaryIndexes) return null
    return dbConfig.GlobalSecondaryIndexes.find(item => item.IndexName===indexName)
  }

  function getLocalIndexQuery(indexName) {
    const locIdx = getLocalIndex(indexName)
    if (!locIdx) return null
    return configureIndex(locIdx)
  }

  function getGlobalIndexQuery(indexName) {
    const gsi = getLocalIndex(indexName)
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
    function addAttributeName(name, value) {
      const key = '#' + name
      config.ExpressionAttributeNames[key] = name
      if (value) {
        const valueKey = ':' + name
        config.ExpressionAttributeValues[valueKey] = value
      }
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
    function eq(pkvalue, skvalue, projection) {
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
      if (projection) params.ProjectionExpression = projection
      return params
    }
    function beginsWith(pkvalue, skvalue, projection) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: indexName,
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
      if (projection) params.ProjectionExpression = projection
      return params
    }
    function contains(pkvalue, skvalue) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: indexName,
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
    function toString() {
      return `Index ${indexName} containing key ${pk}, ${sk}`
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
      getParams: getParams,
      toString: toString,
      explain: explain
    }
  }

  function getIndexQuery(indexName) {
    // if (!dbConfig.GlobalSecondaryIndexes) return null; const gsi = dbConfig.GlobalSecondaryIndexes.find(item => item.IndexName===indexName)
    const gsi = getGlobalIndex(indexName)
    if (!gsi) return null
    const hkey = gsi.KeySchema.find(item => item.KeyType=='HASH')
    const skey = gsi.KeySchema.find(item => item.KeyType=='RANGE')
    const pk = hkey ? hkey.AttributeName : null
    const sk = skey ? skey.AttributeName : null
    if (!pk) return null
    const pkPlaceholder = `#${pk}`
    const skPlaceholder = `#${sk}`
    let config = {
      TableName: dbConfig.TableName,
      IndexName: indexName,
      ExpressionAttributeNames: {}
    }
    function getParams() { return config }
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
    function addAttributeName(name, value) {
      const key = '#' + name
      config.ExpressionAttributeNames[key] = name
      if (value) {
        const valueKey = ':' + name
        config.ExpressionAttributeValues[valueKey] = value
      }
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
    function eq(pkvalue, skvalue, projection) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: indexName,
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
      if (projection) params.ProjectionExpression = projection
      return params
    }
    function beginsWith(pkvalue, skvalue, projection) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: indexName,
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
      if (projection) params.ProjectionExpression = projection
      return params
    }
    function contains(pkvalue, skvalue) {
      let params = {
        TableName: dbConfig.TableName,
        IndexName: indexName,
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
    function toString() {
      return `Index ${indexName} containing key ${pk}, ${sk}`
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
      getParams: getParams,
      toString: toString,
      explain: explain
    }
  } // getIndexQuery
  return {
    getIndexQuery: getIndexQuery,
    getLocalIndexQuery: getLocalIndexQuery,
    getGlobalIndexQuery: getGlobalIndexQuery
  }
} // init
module.exports = { init: init }
