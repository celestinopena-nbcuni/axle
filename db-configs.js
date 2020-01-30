module.exports = {
  'telemundo': {
    'TableName': 'TelemundoContent',
    KeySchema: [
      { AttributeName: 'PK', KeyType: 'HASH'},  // Partition key
      { AttributeName: 'SK', KeyType: 'RANGE' }  // Sort key
    ],
    AttributeDefinitions: [
      { AttributeName: 'PK', AttributeType: 'S' },
      { AttributeName: 'SK', AttributeType: 'S' }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10
    }
  },
  'movies': {
    TableName : 'Movies',
    KeySchema: [
      { AttributeName: 'year', KeyType: 'HASH'},  //Partition key
      { AttributeName: 'title', KeyType: 'RANGE' }  //Sort key
    ],
    AttributeDefinitions: [
      { AttributeName: 'year', AttributeType: 'N' },
      { AttributeName: 'title', AttributeType: 'S' }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10
    }
  }
}
