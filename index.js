var config = require('histograph-config')
var R = require('ramda')
var H = require('highland')
var Redis = require('redis')
var redisClient = Redis.createClient(config.redis.port, config.redis.host)
var normalize = require('histograph-uri-normalizer').normalize
var fuzzyDates = require('fuzzy-dates')

var graphmalizer = require('histograph-db-graphmalizer')

function preprocess (message) {
  if (message.type === 'pit' && (message.action === 'create' || message.action === 'update')) {
    if (message.payload.validSince) {
      message.payload.validSince = fuzzyDates.convert(message.payload.validSince)
    }

    if (message.payload.validUntil) {
      message.payload.validUntil = fuzzyDates.convert(message.payload.validUntil)
    }

    // normalize IDs/URIs
    var id = normalize(message.payload.id || message.payload.uri, message.meta.dataset)
    message.payload.id = id
    delete message.payload.uri
  } else if (message.type === 'relation' && (message.action === 'create' || message.action === 'update')) {
    // normalize IDs/URIs
    var from = normalize(message.payload.from, message.meta.dataset)
    var to = normalize(message.payload.to, message.meta.dataset)

    message.payload.from = from
    message.payload.to = to
  }

  return message
}

// Create a stream from Redis queue
var redis = H(function redisGenerator (push, next) {
  // Function called on each Redis message (or timeout)
  function redisCallback (err, data) {
    // Handle error
    if (err) {
      push(err)
      next()
      return
    }

    // Attempt parse or error
    try {
      var d = JSON.parse(data[1])
      push(null, d)
      next()
    } catch (e) {
      push(e)
      next()
    }
  }

  // Blocking pull from Redis
  redisClient.blpop(config.redis.queue, 0, redisCallback)
})

function logError (err) {
  console.error(err.stack || err)
}

var dbs = [
  'postgis',
  'elasticsearch'
]

var commands = redis
  .errors(logError)
  .map(preprocess)
  .compact()

dbs.forEach((db) => {
  var dbModule = require(`histograph-db-${db}`)

  var pipeline = H.pipeline(
    H.map(R.clone),
    H.batchWithTimeOrCount(config.core.batchTimeout, config.core.batchSize),
    H.map(H.wrapCallback((messages, callback) => dbModule.bulk(messages, callback))),
    H.errors(logError),
    H.sequence(),
    H.each((f) => {})
  )

  commands
    .fork()
    .pipe(pipeline)
})

graphmalizer.fromStream(commands.fork())

console.log(config.logo.join('\n'))
console.log('Histograph Core ready!')
