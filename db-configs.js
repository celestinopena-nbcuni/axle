module.exports = {
  'telemundo': {
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
    ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 },
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
    },
    {
      IndexName: 'GSI-2',
      KeySchema: [{ AttributeName: 'child', KeyType: 'HASH' }],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: { ReadCapacityUnits: '10', WriteCapacityUnits: '10' }
    }]
  },
  'telemundo02': {
    TableName: 'TelemundoContent',
    AttributeDefinitions: [
      { AttributeName: 'nid', AttributeType: 'S' },
      { AttributeName: 'child', AttributeType: 'S' },
      { AttributeName: 'section', AttributeType: 'S' },
      { AttributeName: 'seriesCtypeTitle', AttributeType: 'S' }
    ],
    KeySchema: [
      { AttributeName: 'nid', KeyType: 'HASH'},
      { AttributeName: 'child', KeyType: 'RANGE'}
    ],
    ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 },
    GlobalSecondaryIndexes: [{
      IndexName: 'GSI-1',
      KeySchema: [
        { AttributeName: 'child', KeyType: 'HASH' },
        { AttributeName: 'nid', KeyType: 'RANGE' }
      ],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
    },
    {
      IndexName: 'GSI-2',
      KeySchema: [
        { AttributeName: 'section', KeyType: 'HASH' },
        { AttributeName: 'seriesCtypeTitle', KeyType: 'RANGE' }
      ],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
    }]
  },
  'movies': {
    TableName : 'Movies',
    KeySchema: [
      { AttributeName: 'year', KeyType: 'HASH' },
      { AttributeName: 'title', KeyType: 'RANGE' }
    ],
    AttributeDefinitions: [
      { AttributeName: 'year', AttributeType: 'N' },
      { AttributeName: 'title', AttributeType: 'S' }
    ],
    ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
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
    ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
  }
}
