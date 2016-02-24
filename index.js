var config = require('histograph-config')
var R = require('ramda')
var H = require('highland')
var Redis = require('redis')
var redisClient = Redis.createClient(config.redis.port, config.redis.host)

var graphmalizer = require('histograph-db-graphmalizer')

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

// TODO: do URI normalizing here!?
// TODO: fuzzyDates here! var fuzzyDates = require('fuzzy-dates')

var commands = redis
    .errors(logError)

dbs.forEach((db) => {
  var dbModule = require(`histograph-db-${db}`)

  var pipeline = H.pipeline(
    H.map(R.clone),
    H.batchWithTimeOrCount(config.core.batchTimeout, config.core.batchSize),
    H.map(H.wrapCallback((items, callback) => dbModule.bulk(items, callback))),
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
