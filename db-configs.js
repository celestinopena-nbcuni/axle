module.exports = {
  'telemundo': {
    'TableName': 'TelemundoContent',
    KeySchema: [
      { AttributeName: 'vid', KeyType: 'HASH'},  //Partition key
      { AttributeName: 'type', KeyType: 'RANGE' }  //Sort key
    ],
    AttributeDefinitions: [
      { AttributeName: 'vid', AttributeType: 'N' },
      { AttributeName: 'type', AttributeType: 'S' }
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
