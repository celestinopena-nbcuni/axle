module.exports = {
  TableName: 'demotable',
  AttributeDefinitions: [
    { AttributeName: 'pk', AttributeType: 'S' },
    { AttributeName: 'sk', AttributeType: 'S' }
  ],
  KeySchema: [
    { AttributeName: 'pk', KeyType: 'HASH'},
    { AttributeName: 'sk', KeyType: 'RANGE' }
  ],
  ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 10 },
  GlobalSecondaryIndexes: [{
    IndexName: 'GSI-1',
    KeySchema: [
      { AttributeName: 'sk', KeyType: 'HASH' },
      { AttributeName: 'pk', KeyType: 'RANGE' }
    ],
    Projection: { ProjectionType: 'ALL' },
    ProvisionedThroughput: { ReadCapacityUnits: '10', WriteCapacityUnits: '10' }
  }]
}
