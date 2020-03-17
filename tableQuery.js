/* This library provides an object which facilitates the generation of parameter objects for database commands. */
function init(dbConfig) {
  const PK = getTablePK() || 'pk'
  const SK = getTableSK() || 'sk'
  function getTable() { return dbConfig.TableName; }
  function getTablePK() {
    const key = dbConfig.KeySchema.find(item => item.KeyType=='HASH')
    return key ? key.AttributeName : null
  }
  function getTableSK() {
    const key = dbConfig.KeySchema.find(item => item.KeyType=='RANGE')
    return key ? key.AttributeName : null
  }

  function queryParams(record, projection) { return getQueryParams().beginsWith(record[PK], null).project(projection).get() }
  // getQueryParams creates an object that allows table searches
  function getQueryParams() {
    const pkPlaceholder = `#${PK}`
    const skPlaceholder = `#${SK}`
    let config = { TableName: dbConfig.TableName }
    function get() { return config }
    function addAttributeName(name, value) {
      const key = '#' + name
      config.ExpressionAttributeNames[key] = name
      if (value) {
        const valueKey = ':' + name
        config.ExpressionAttributeValues[valueKey] = value
      }
      return this
    }
    function eq(pkvalue, skvalue) {
      config.ExpressionAttributeNames = {}
      config.ExpressionAttributeNames[pkPlaceholder] = PK
      if (skvalue) {
        config.KeyConditionExpression = `${pkPlaceholder} = :pk AND ${skPlaceholder} = :sk`
        config.ExpressionAttributeNames[skPlaceholder] = SK
        config.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
      } else {
        config.KeyConditionExpression = `${pkPlaceholder} = :pk`
        config.ExpressionAttributeValues = { ':pk': pkvalue }
      }
      return this
    }
    function beginsWith(pkvalue, skvalue) {
      config.ExpressionAttributeNames = {}
      config.ExpressionAttributeNames[pkPlaceholder] = PK
      if (skvalue) {
        config.KeyConditionExpression = `${pkPlaceholder} = :pk AND begins_with(${skPlaceholder}, :sk)`
        config.ExpressionAttributeNames[skPlaceholder] = SK
        config.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
      } else {
        config.KeyConditionExpression = `${pkPlaceholder} = :pk`
        config.ExpressionAttributeValues = { ':pk': pkvalue }
      }
      return this
    }
    function contains(pkvalue, skvalue) {
      config.ExpressionAttributeNames = {}
      config.ExpressionAttributeNames[pkPlaceholder] = PK
      if (skvalue) {
        config.KeyConditionExpression = `${pkPlaceholder} = :pk AND contains(${skPlaceholder}, :sk)`
        config.ExpressionAttributeNames[skPlaceholder] = SK
        config.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
      } else {
        config.KeyConditionExpression = `${pkPlaceholder} = :pk`
        config.ExpressionAttributeValues = { ':pk': pkvalue }
      }
      return this
    }
    function filter(expr, name, value) {
      config.FilterExpression = expr // '#status = :stat'
      addAttributeName(name, value)
      return this
    }
    function project(projection) {
      if (projection) config.ProjectionExpression = projection
      return this
    }
    function toString() {
      return `Table ${dbConfig.TableName} contains key ${PK}, ${SK}`
    }
    // Provide a plain language description of the query parameter object
    function explain() {
      const kce = Object.keys(config.ExpressionAttributeNames).reduce((orig, curr) => {
        return orig.replace(curr, config.ExpressionAttributeNames[curr])
      }, config.KeyConditionExpression)
      let criteria = Object.keys(config.ExpressionAttributeValues).reduce((orig, curr) => { return orig.replace(curr, `'${config.ExpressionAttributeValues[curr]}'`) }, kce)
      let description = `Search ${config.TableName} WHERE ${criteria}`
      if (config.FilterExpression) {
        const filter = Object.keys(config.ExpressionAttributeNames).reduce((orig, curr) => { return orig.replace(curr, `'${config.ExpressionAttributeNames[curr]}'`) }, config.FilterExpression)
        criteria = Object.keys(config.ExpressionAttributeValues).reduce((orig, curr) => { return orig.replace(curr, `'${config.ExpressionAttributeValues[curr]}'`) }, filter)
        description += ` FILTER ON ${criteria}`
      }
      if (config.ProjectionExpression) description += ` SHOWING ONLY ${config.ProjectionExpression}`
      return description
    }
    return {
      addAttribute: addAttributeName,
      eq: eq,
      beginsWith: beginsWith,
      contains: contains,
      filter: filter,
      project: project,
      get: get,
      toString: toString,
      explain: explain
    }
  }

  function getDeleteParams(record) {
    const pk = getTablePK() || 'pk'
    const sk = getTableSK() || 'sk'
    let config = {
      TableName: dbConfig.TableName,
      Key: {}
    }
    config.Key[pk] = record[pk]
    config.Key[sk] = record[sk]
    return config
  }

  // A convenience function for getInsertParams
  function insertParams(record) { return getInsertParams(record).get() }
  function getInsertParams(record) {
    let config = {
      TableName: dbConfig.TableName,
      Item: record
    }
    function get() { return config }
    function setCondition(expr) {
      config.ConditionExpression = expr
      return this
    }
    function names(namelist) {
      if (Array.isArray(namelist)) {
        config.ExpressionAttributeNames = {}
        namelist.forEach(item => { config.ExpressionAttributeNames[`#${item}`] = item })
      }
      return this
    }
    function setName(attrib, alias) {
      if (!alias) alias = attrib
      if (!config.ExpressionAttributeNames) config.ExpressionAttributeNames = {}
      config.ExpressionAttributeNames[`#${alias}`] = attrib
      return this
    }
    return {
      setCondition: setCondition,
      names: names,
      setName: setName,
      get: get
    }
  }

  function getTransactParams(record) {
    let config = { TransactItems: [] }
    function get() { return config }
    function addPut(param) {
      config.TransactItems.push(createPut(param))
      return this
    }
    function addUpdate(param) {
      config.TransactItems.push(createUpdate(param))
      return this
    }
    function createPut(param) {
      if (!param.TableName) param.TableName = dbConfig.TableName
      return { Put: param }
    }
    function createUpdate(param) {
      if (!param.TableName) param.TableName = dbConfig.TableName
      return { Update: param }
    }
    function toString() {
      return JSON.stringify(config, null, 2)
    }
    return {
      addPut: addPut,
      addUpdate: addUpdate,
      toString: toString,
      get: get
    }
  }
  function getBatchWriteParams(records) {
    const config = { RequestItems: {}}
    config.RequestItems[dbConfig.TableName] = records.map(obj => { return {PutRequest: {Item: obj}}; } )
    return config
  }

  function getUpdateQuery(pkvalue, skvalue) {
    const pk = getTablePK() || 'pk'
    const sk = getTableSK() || 'sk'
    let config = {
      TableName: dbConfig.TableName,
      Key: {},
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {},
      ReturnValues: 'UPDATED_NEW'
    }
    config.Key[pk] = pkvalue
    config.Key[sk] = skvalue
    function get() { return config }
    function setExpr(updateExpr) {
      config.UpdateExpression = updateExpr
      return this
    }
    function names(namelist) {
      if (Array.isArray(namelist)) namelist.forEach(item => { config.ExpressionAttributeNames[`#${item}`] = item })
      return this
    }
    function values(map) {
      Object.keys(map).forEach(key => { config.ExpressionAttributeValues[`:${key}`] = map[key] })
      return this
    }
    function exprValue(expr, value) {
      config.ExpressionAttributeValues[`:${expr}`] = value
      return this
    }
    return {
      setExpr: setExpr,
      names: names,
      values: values,
      exprValue: exprValue,
      get: get
    }
  }

  // getLocalIndexQuery creates an object that allows searches by a LSI
  function getLocalIndexQuery(indexName) {
    if (!dbConfig.LocalSecondaryIndexes) return null
    const locIdx = dbConfig.LocalSecondaryIndexes.find(item => item.IndexName===indexName)
    if (!locIdx) return null
    return configureIndex(locIdx)
  }

  // getGlobalIndexQuery creates an object that allows searches by a GSI
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
    const pkPlaceholder = `#${pk.replace('-', '').toLowerCase()}`
    const skPlaceholder = `#${sk.replace('-', '').toLowerCase()}`
    let config = {
      TableName: dbConfig.TableName,
      IndexName: index.IndexName,
      ExpressionAttributeNames: {}
    }
    config.ExpressionAttributeNames[pkPlaceholder] = pk
    function get() { return config }
    function addAttributeName(name, value) {
      const key = '#' + name
      config.ExpressionAttributeNames[key] = name
      if (value) {
        const valueKey = ':' + name
        config.ExpressionAttributeValues[valueKey] = value
      }
      return this
    }
    function eq(pkvalue, skvalue) {
      if (skvalue) {
        config.ExpressionAttributeNames[skPlaceholder] = sk
        config.KeyConditionExpression = `${pkPlaceholder} = :pk AND ${skPlaceholder} = :sk`
        config.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
      } else {
        config.KeyConditionExpression = `${pkPlaceholder} = :pk`
        config.ExpressionAttributeValues = { ':pk': pkvalue }
      }
      return this
    }
    function beginsWith(pkvalue, skvalue) {
      if (skvalue) {
        config.ExpressionAttributeNames[skPlaceholder] = sk
        config.KeyConditionExpression = `${pkPlaceholder} = :pk AND begins_with(${skPlaceholder}, :sk)`
        config.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
      } else {
        config.KeyConditionExpression = `${pkPlaceholder} = :pk`
        config.ExpressionAttributeValues = { ':pk': pkvalue }
      }
      return this
    }
    function contains(pkvalue, skvalue) {
      config.ExpressionAttributeNames[pkPlaceholder] = pk
      if (skvalue) {
        config.ExpressionAttributeNames[skPlaceholder] = sk
        config.KeyConditionExpression = `${pkPlaceholder} = :pk AND contains(${skPlaceholder}, :sk)`
        config.ExpressionAttributeValues = { ':pk': pkvalue, ':sk': skvalue }
      } else {
        config.KeyConditionExpression = `${pkPlaceholder} = :pk`
        config.ExpressionAttributeValues = { ':pk': pkvalue }
      }
      return this
    }
    function filter(expr, name, value) {
      config.FilterExpression = expr // '#status = :stat'
      addAttributeName(name, value)
      return this
    }
    function project(projection) {
      if (projection) config.ProjectionExpression = projection
      return this
    }
    function toString() {
      return `Index ${index.IndexName} contains key ${pk}, ${sk}`
    }
    // Provide a plain language description of the query parameter object
    function explain() {
      const exprAttributeNames = Object.keys(config.ExpressionAttributeNames)
      if (!(exprAttributeNames && config.ExpressionAttributeValues)) return 'No key values have been set'
      const exprAttributeValues = Object.keys(config.ExpressionAttributeValues)
      const kce = exprAttributeNames.reduce((orig, curr) => {
        return orig.replace(curr, config.ExpressionAttributeNames[curr])
      }, config.KeyConditionExpression)
      let criteria = exprAttributeValues.reduce((orig, curr) => { return orig.replace(curr, `'${config.ExpressionAttributeValues[curr]}'`) }, kce)
      let description = `Search ${config.TableName}.${config.IndexName} WHERE ${criteria}`
      if (config.FilterExpression) {
        const filter = exprAttributeNames.reduce((orig, curr) => { return orig.replace(curr, `'${config.ExpressionAttributeNames[curr]}'`) }, config.FilterExpression)
        criteria = exprAttributeValues.reduce((orig, curr) => { return orig.replace(curr, `'${config.ExpressionAttributeValues[curr]}'`) }, filter)
        description += ` FILTER ON ${criteria}`
      }
      if (config.ProjectionExpression) description += ` SHOWING ONLY ${config.ProjectionExpression}`
      return description
    }
    return {
      eq: eq,
      beginsWith: beginsWith,
      contains: contains,
      filter: filter,
      project: project,
      addAttribute: addAttributeName,
      get: get,
      toString: toString,
      explain: explain
    }
  } // configureIndex
  return {
    getLocalIndexQuery: getLocalIndexQuery,
    getGlobalIndexQuery: getGlobalIndexQuery,
    queryParams: queryParams,
    getQueryParams: getQueryParams,
    getUpdateQuery: getUpdateQuery,
    getBatchWriteParams: getBatchWriteParams,
    getTransactParams: getTransactParams,
    getInsertParams: getInsertParams,
    insertParams: insertParams,
    getDeleteParams: getDeleteParams,
    getTable: getTable
  }
} // init
module.exports = { init: init }
