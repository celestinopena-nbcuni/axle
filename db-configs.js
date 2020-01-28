module.exports = {
  'telemundo': {
    'TableName': 'TelemundoContentparams',
    'KeyAttributesparams': {
      'PartitionKeyparams': {
        'AttributeNameparams': 'contentId',
        'AttributeType': 'S'
      }
    },
    'NonKeyAttributes': [
      {
        'AttributeName': 'itemType',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'title',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'datePublished',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'source',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'authorPhotoLink',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'byline',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'body',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'coverImage',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'coverImageDescription',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'mpxVideo',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'mpxVideoReference',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'videoCredit',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'featuredGallery',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'promoTitle',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'promoImage',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'promoKicker',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'promoDescription',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'showSeason',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'brand',
        'AttributeType': 'SS'
      },
      {
        'AttributeName': 'category',
        'AttributeType': 'SS'
      },
      {
        'AttributeName': 'keywords',
        'AttributeType': 'SS'
      },
      {
        'AttributeName': 'people',
        'AttributeType': 'SS'
      },
      {
        'AttributeName': 'roleProfile',
        'AttributeType': 'SS'
      },
      {
        'AttributeName': 'status',
        'AttributeType': 'S'
      }
    ]
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
