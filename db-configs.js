module.exports = {
  'telemundoCW': {
    TableName: 'TelemundoCW',
    AttributeDefinitions: [
      { AttributeName: 'uuid', AttributeType: 'S' },
      { AttributeName: 'itemType', AttributeType: 'S' },
      { AttributeName: 'statusDate', AttributeType: 'S' },
      { AttributeName: 'statusCreateDateSubtype', AttributeType: 'S' }
    ],
    KeySchema: [
      { AttributeName: 'uuid', KeyType: 'HASH'}
    ],
    ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 },
    GlobalSecondaryIndexes: [{
      IndexName: 'Itemtype',
      KeySchema: [
        { AttributeName: 'itemType', KeyType: 'HASH' }
      ],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
    },
    {
      IndexName: 'StatusDate',
      KeySchema: [
        { AttributeName: 'itemType', KeyType: 'HASH' },
        { AttributeName: 'statusDate', KeyType: 'RANGE' }
      ],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
    },
    {
      IndexName: 'StatusCreateDateSubtype',
      KeySchema: [
        { AttributeName: 'itemType', KeyType: 'HASH' },
        { AttributeName: 'statusCreateDateSubtype', KeyType: 'RANGE' }
      ],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
    }
  ]
  },
  'telemundo': {
    TableName: 'TelemundoContent',
    AttributeDefinitions: [
      { AttributeName: 'nid', AttributeType: 'S' },
      { AttributeName: 'child', AttributeType: 'S' },
      { AttributeName: 'section', AttributeType: 'S' },
      { AttributeName: 'seriesCtypeTitle', AttributeType: 'S' },
      { AttributeName: 'seriesCtypeStatus', AttributeType: 'S' }
    ],
    KeySchema: [
      { AttributeName: 'nid', KeyType: 'HASH'},
      { AttributeName: 'child', KeyType: 'RANGE'}
    ],
    ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 },
    GlobalSecondaryIndexes: [{
      IndexName: 'ChildNode',
      KeySchema: [
        { AttributeName: 'child', KeyType: 'HASH' },
        { AttributeName: 'nid', KeyType: 'RANGE' }
      ],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
    },
    {
      IndexName: 'SeriesTypeTitle',
      KeySchema: [
        { AttributeName: 'section', KeyType: 'HASH' },
        { AttributeName: 'seriesCtypeTitle', KeyType: 'RANGE' }
      ],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
    },
    {
      IndexName: 'SeriesTypeStatus',
      KeySchema: [
        { AttributeName: 'section', KeyType: 'HASH' },
        { AttributeName: 'seriesCtypeStatus', KeyType: 'RANGE' }
      ],
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 }
    }
  ]
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
