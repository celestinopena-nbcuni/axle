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
  'telemundoPlus': {
    TableName: 'TelemundoContent',
    AttributeDefinitions: [
      { AttributeName: 'nid', AttributeType: 'S' },
      { AttributeName: 'child', AttributeType: 'S' },
      { AttributeName: 'ctype', AttributeType: 'S' }
    ],
    KeySchema: [
      { AttributeName: 'nid', KeyType: 'HASH'},
      { AttributeName: 'child', KeyType: 'RANGE'}
    ],
    GlobalSecondaryIndexes: [{
      IndexName: 'GSI-1',
      KeySchema: [
        { AttributeName: 'child', KeyType: 'HASH' },
        { AttributeName: 'nid', KeyType: 'RANGE' }
      ],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: {
        ReadCapacityUnits: 10,
        WriteCapacityUnits: 10
      }
    }],
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
  },
  'films': {
    TableName: 'Films',
    AttributeDefinitions: [
      { AttributeName: 'actor', AttributeType: 'S' },
      { AttributeName: 'film', AttributeType: 'S' },
      { AttributeName: 'year', AttributeType: 'N' }
    ],
    KeySchema: [
      { AttributeName: 'actor', KeyType: 'HASH' },
      { AttributeName: 'film', KeyType: 'RANGE' }
    ],
    LocalSecondaryIndexes: [
      {
        IndexName: 'LOC-1',
        KeySchema: [
          { AttributeName: 'actor', KeyType: 'HASH' },
          { AttributeName: 'year', KeyType: 'RANGE' }
        ],
        Projection: { ProjectionType: 'ALL' }
      }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10
    }
  }
}
