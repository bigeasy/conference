// Common utilities.
var assert = require('assert')
var util = require('util')
var coalesce = require('extant')

// Control-flow utilities.
var cadence = require('cadence')

var Cliffhanger = require('cliffhanger')

var Procession = require('procession')

var Monotonic = require('monotonic').asString

var logger = require('prolific.logger').createLogger('conference')

var Operation = require('operation/redux')

// Invoke round trip requests into an evented message queue.
var Requester = require('conduit/requester')

// Respond to requests from an evented message queue.
var Responder = require('conduit/responder')

var Client = require('conduit/client')
var Server = require('conduit/server')

function keyify () { return JSON.stringify(Array.prototype.slice.call(arguments)) }

// The patterns below take my back to my efforts to create immutable
// constructors when immutability was all the rage in Java-land. It would have
// pained me to create an object that continues to initialize the object after
// the constructor, but at the same time I'd have no real problem with reponding
// the headers in these streams. It was something that I abandoned after having
// spend some time with it, with rationale that I cannot remember. I can only
// remember the ratoinale that led me to adopt it.
//
// It did lead me to consider the folly of ORM, though.

// A `Responder` class specific to the Conference that will respond to
// directives from a Colleague.

// Update: Actually, passing in the builder function feels somewhat immutable,
// about as immutable as JavaScript is going to get.

//
function Constructor (conference, properties, object, operations) {
    this._object = object
    this._operations = operations
    this._properties = properties
    this._setOperation(keyify(true, 'receive', 'naturalized'), { object: conference, method: '_naturalized' })
    this._setOperation(keyify(true, 'request', 'backlog'), { object: conference, method: '_backlog' })
}

Constructor.prototype._setOperation = function (key, operation) {
    if (typeof operation == 'string') {
        assert(this._object, 'object cannot be null')
        operation = { object: this._object, method: operation }
    }
    this._operations[key] = Operation(operation)
}

Constructor.prototype.setProperty = function (name, value) {
    this._properties[name] = value
}

Constructor.prototype.setProperties = function (properties) {
    for (var name in properties) {
        this._properties[name] = properties[name]
    }
}

Constructor.prototype.bootstrap = function (method) {
    this._setOperation(keyify('bootstrap'), coalesce(method, 'bootstrap'))
}

Constructor.prototype.join = function (method) {
    this._setOperation(keyify('join'), coalesce(method, 'join'))
}

Constructor.prototype.immigrate = function (method) {
    this._setOperation(keyify('immigrate'), coalesce(method, 'immigrate'))
}

Constructor.prototype.naturalized = function (method) {
    this._setOperation(keyify('naturalized'), coalesce(method, 'naturalized'))
}

Constructor.prototype.exile = function (method) {
    this._setOperation(keyify('exile'), coalesce(method, 'exile'))
}

Constructor.prototype.government = function (method) {
    this._setOperation(keyify('government'), coalesce(method, 'government'))
}

Constructor.prototype.receive = function (name, method) {
    this._setOperation(keyify(false, 'receive', name), coalesce(method, name))
}

Constructor.prototype.reduced = function (name, method) {
    this._setOperation(keyify(false, 'reduced', name), coalesce(method, name))
}

Constructor.prototype.request = function (name, method) {
    this._setOperation(keyify(false, 'request', name), coalesce(name, method))
}

Constructor.prototype.socket = function (method) {
    this._setOperation(keyify('socket'), coalesce(method, 'socket'))
}

Constructor.prototype.method = function (name, method) {
    this._setOperation(keyify('method', name), coalesce(name, method))
}

function Dispatcher (conference) {
    this._conference = conference
}

Dispatcher.prototype.request = cadence(function (async, envelope) {
    async(function () {
        this._conference._request(envelope, async())
    }, function (response) {
        return [ response ]
    })
})

Dispatcher.prototype.fromBasin = function (envelope, callback) {
    this._conference._fromBasin(envelope, callback)
}

Dispatcher.prototype.fromSpigot = function (envelope, callback) {
    this._conference._fromSpigot(envelope, callback)
}

function Conference (object, constructor) {
    logger.info('constructed', {})
    this.isLeader = false
    this.colleague = null
    this.replaying = false
    this.id = null
    this._cookie = '0'
    this._boundary = '0'
    this._broadcasts = {}
    this._backlogs = {}

    this._records = new Procession
    this._replays = this._records.shifter()

    // Currently exposed for testing, but feeling that these method should be
    // public for general testing, with one wrapper that hooks it up to the
    // colleague's streams and another that lets you send mock events.

    //
    this._dispatcher = new Dispatcher(this)

    this.read = new Procession
    this.write = new Procession

    this._client = new Client('outgoing', this.write, this.read)
    var server = new Server({ object: this, method: '_connect' }, 'incoming', this._client.read, this._client.write)
    var responder = new Responder(this._dispatcher, 'colleague', server.read, server.write)
    this._requester = new Requester('colleague', responder.read, responder.write)

    this._write = this._requester.write
    this._requester.read.pump([ this, '_entry' ])
    this._requester.read.pump([ this, '_play' ])

    this._cliffhanger = new Cliffhanger

    constructor(new Constructor(this, this._properties = {}, object, this._operations = {}))
}

// An ever increasing identify to adorn our broadcasts so that it's key will be
// unique when we combine our immigration promise with the cookie.

//
Conference.prototype._nextCookie = function () {
    return this._cookie = Monotonic.increment(this._cookie, 0)
}

Conference.prototype._nextBoundary = function () {
    return this._boundary = Monotonic.increment(this._boundary, 0)
}

Conference.prototype._play = cadence(function (async, envelope) {
    if (envelope == null) {
        return
    }
    switch (envelope.method) {
    case 'record':
        this._records.enqueue(envelope, async())
        break
    case 'invoke':
        this._invoke(envelope.body.method, envelope.body.body, async())
        break
    }
})

Conference.prototype._entries = cadence(function (async, envelope) {
    if (envelope == null) {
        return
    }
    switch (envelope.method) {
    case 'entry':
        this._write.push({
            module: 'conference',
            method: 'boundary',
            id: this._nextBoundary(),
            entry: envelope.body.promise
        })
        this._entry(envelope.body, async())
        break
    }
})

// Run the given operation if we are not replaying a log. If we are not
// replaying then we are performing actions that generate out-of-band log
// entries. If we are replaying we want to replay those out-of-band log entries.

//
Conference.prototype._ifNotReplaying = cadence(function (async, operation) {
    if (this.replaying) {
        throw new Error(1)
    }
    if (!this.replaying) {
        Operation(operation)(async())
    }
})

Conference.prototype.ifNotReplaying = function () {
    this.boundary()
    if (arguments.length == 2) {
        this._ifNotReplaying(arguments[0], arguments[1])
    } else {
        var async = arguments[0], conference = this
        return function () {
            if (!conference.replaying) {
                async.apply(null, Array.prototype.slice.call(arguments))
            }
        }
    }
}

Conference.prototype._record_ = cadence(function (async, operation) {
    async(function () {
        var id = this._boundary = Monotonic.increment(this._boundary, 0)
        if (conference.replaying) {
            async(function () {
                this._records.dequeue(async())
            }, function (envelope) {
                assert(envelope.id == envelope.id)
                return envelope.body
            })
        } else {
            async.apply(null, Array.prototype.slice.call(arguments))
        }
    }, function (result) {
        result = coalesce(result)
        this._write.push({
            module: 'conference',
            method: 'record',
            id: id,
            body: result
        })
        return [ result ]
    })
})

Conference.prototype.record_ = function () {
    if (arguments.length == 2) {
        this._record_(arguments[0], arguments[1])
    } else {
        var async = arguments[0], conference = this
        return function () {
            var id = conference._nextBoundary()
            var steps = Array.prototype.slice.call(arguments)
            async(function () {
                if (conference.replaying) {
                    async(function () {
                        conference._replays.dequeue(async())
                    }, function (envelope) {
                        assert(envelope.id == envelope.id)
                        return envelope.body
                    })
                } else {
                    async.apply(null, steps)
                }
            }, function (result) {
                result = coalesce(result)
                conference._write.push({
                    module: 'conference',
                    method: 'record',
                    id: id,
                    body: result
                })
                return [ result ]
            })
        }
    }
}

Conference.prototype.boundary = function () {
    this._write.push({
        module: 'conference',
        method: 'boundary',
        id: this._boundary = Monotonic.increment(this._boundary, 0),
        entry: null
    })
}

Conference.prototype._invoke = cadence(function (async, method, body) {
    this._write.push({
        module: 'conference',
        method: 'invoke',
        body: { method: method, body: coalesce(body) }
    })
    this._operate(keyify('method', method), [ this, body ], async())
})

Conference.prototype.invoke = function (method, message, callback) {
    this._invoke(false, method, message, callback)
}

// Get the properties for a particular id or promise.

//
Conference.prototype.getProperties = function (id) {
    id = coalesce(this.government.immigrated.id[id], id)
    return coalesce(this.government.properties[id])
}

// Respond an out of band request. If you want to return a status code you can
// just throw the integer value.
//
// Not sure if I want to use UNIX codes or HTTP status codes. Leaning toward
// HTTP status codes and throwing them as integer.

//
Conference.prototype._request = cadence(function (async, envelope) {
    switch (envelope.method) {
    case 'properties':
        this.id = envelope.body.id
        this.replaying = envelope.body.replaying
        return [ this._properties ]
    case 'outOfBand':
        envelope = envelope.body
        this._operate(keyify(envelope.internal, 'request', envelope.method), [ this, envelope.body ], async())
        break
    }
})

Conference.prototype._connect = function (socket, envelope) {
    var operation = this._operations[keyify('socket')]
    assert(operation != null)
    operation(this, socket, envelope)
}

Conference.prototype._backlog = cadence(function (async, conference, promise) {
    return [ this._backlogs[promise] ]
})

Conference.prototype._operate = cadence(function (async, key, vargs) {
    var operation = this._operations[key]
    if (operation == null) {
        return null
    }
    operation.apply(null, vargs.concat(async()))
})

Conference.prototype._getBacklog = cadence(function (async) {
    async(function () {
        console.log('GETTING BROADCASTS')
        this.record_(async)(function () {
            this._requester.request('colleague', {
                module: 'conference',
                method: 'outOfBand',
                to: this.government.majority[0],
                body: {
                    module: 'conference',
                    method: 'backlog',
                    internal: true,
                    body: this.government.promise
                }
            }, async())
        })
    }, function (broadcasts) {
        console.log('GOT BROADCASTS!!!', broadcasts)
        var entries = []
        for (var key in broadcasts) {
            var broadcast = broadcasts[key]
            entries.push({
                body: {
                    module: 'conference',
                    method: 'broadcast',
                    key: key,
                    body: {
                        method: broadcast.method,
                        body: broadcast.request
                    }
                }
            })
            for (var promise in broadcast.responses) {
                entries.push({
                    body: {
                        module: 'conference',
                        method: 'reduce',
                        from: promise,
                        key: key,
                        body: broadcast.responses[promise]
                    }
                })
            }
        }
        async.forEach(function (entry) {
            this._entry({ module: 'colleague', method: 'entry', body: entry }, async())
        })(entries)
    }, function () {
        // TODO Probably not a bad idea, but what was I thinking?
            console.log('trying to notify??')
        this._write.push({
            module: 'conference',
            method: 'naturalized',
            body: null
        })
    })
})

Conference.prototype._naturalized = cadence(function (async, conference, promise) {
    this._operate(keyify('naturalized'), [ this, promise ], async())
})

Conference.prototype._entry = cadence(function (async, envelope) {
    if (envelope == null || envelope.method != 'entry') {
        return []
    }
    var entry = envelope.body
    this._write.push({
        module: 'conference',
        method: 'boundary',
        id: this._nextBoundary(),
        entry: entry.promise
    })
    if (entry.method == 'government') {
        this.government = entry.body
        this.isLeader = this.government.majority[0] == this.id
        var properties = entry.properties
        async(function () {
            if (this.government.immigrate) {
                var immigrant = this.government.immigrate
                async(function () {
                    if (this.government.promise == '1/0') {
                        this._operate(keyify('bootstrap'), [ this ], async())
                    } else if (immigrant.id == this.id) {
                        this._operate(keyify('join'), [ this ], async())
                    }
                }, function () {
                    this._operate(keyify('immigrate'), [ this, immigrant.id ], async())
                }, function () {
                    console.log( "IMMIGRATE", this.id, immigrant)
                    if (immigrant.id != this.id) {
                        this._backlogs[this.government.promise] = JSON.parse(JSON.stringify(this._broadcasts))
                    } else if (this.government.promise != '1/0') {
                        this._getBacklog(async())
                    }
                }, function () {
                    console.log('BACKLOGGED')
                    if (this.government.promise == '1/0' || immigrant.id == this.id) {
                        this._broadcast(true, 'naturalized', this.government.promise)
                    }
                })
            } else if (this.government.exile) {
                var exile = this.government.exile
                async(function () {
                    this._operate(keyify('exile'), [ this ], async())
                }, function () {
                    var promise = this.government.exile.promise
                    var broadcasts = []
                    for (var key in this._broadcasts) {
                        delete this._broadcasts[key].responses[promise]
                        broadcasts.push(this._broadcasts[key])
                    }
                    delete this._backlogs[promise]
                    async.forEach(function (broadcast) {
                        this._checkReduced(broadcast, async())
                    })(broadcasts)
                })
            }
        }, function () {
            this._operate(keyify('government'), [ this ], async())
        })
    } else if (entry.body.body) {
        // Reminder that if you ever want to do queued instead async then the
        // queue should be external and a property of the object the conference
        // operates.

        //
        var envelope = entry.body.body
        switch (envelope.method) {
        case 'broadcast':
            this._broadcasts[envelope.key] = {
                key: envelope.key,
                internal: envelope.internal,
                method: envelope.body.method,
                request: envelope.body.body,
                responses: {}
            }
            prefix = envelope.internal ? '!' : ''
            async(function () {
                this._operate(keyify(envelope.internal, 'receive', envelope.body.method), [ this, envelope.body.body ], async())
            }, function (response) {
                this._write.push({
                    module: 'conference',
                    method: 'reduce',
                    key: envelope.key,
                    internal: envelope.internal,
                    from: this.government.immigrated.promise[this.id],
                    body: response
                })
            })
            break
        // Tally our responses and if they match the number of participants,
        // then invoke the reduction method.
        case 'reduce':
            var broadcast = this._broadcasts[envelope.key]

            broadcast.responses[envelope.from] = envelope.body


            this._checkReduced(broadcast, async())

            break
        case 'naturalized':
            delete this._backlogs[envelope.from]
            break
        }
    }
})

Conference.prototype._checkReduced = cadence(function (async, broadcast) {
    var complete = true
    for (var promise in this.government.immigrated.id) {
        if (!(promise in broadcast.responses)) {
            complete = false
            break
        }
    }

    if (complete) {
        var reduced = []
        for (var promise in broadcast.responses) {
            reduced.push({
                id: this.government.immigrated.id[promise],
                value: broadcast.responses[promise]
            })
        }
        this._operate(keyify(broadcast.internal, 'reduced', broadcast.method), [ reduced, broadcast.request ], async())
        delete this._broadcasts[broadcast.key]
    }
})

// TODO Save welcomes, or introductions, and have them expire when the welcome
// expires, and maybe that is the entirety of out-of-band.
//
// Any difficulties and this method will return `null`. Do not return `null` as
// a valid response from your request handler.

//
Conference.prototype.request = cadence(function (async, to, method, body) {
    if (arguments.length == 3) {
       body = method
       method = to
       to = this.government.majority[0]
    }
    // TODO More consideration as to what happens when the route `to` cannot
    // be found, and whether it makes sense to try to contact anyone but the
    // leader for initialization.
    this._requester.request('colleague', {
        module: 'conference',
        method: 'outOfBand',
        to: to,
        body: {
            module: 'conference',
            method: method,
            internal: false,
            body: body
        }
    }, async())
})

Conference.prototype.socket = function (to, header) {
    if (arguments.length == 1) {
        header = to
        to = this.government.majority[0]
    }
    var properties = this.getProperties(to)
    return this._client.connect({
        module: 'conference',
        method: 'socket',
        to: properties,
        body: header
    })
}

// Honoring back pressure here, but I've not considered if back pressure is
// going to cause deadlock. I'm sure it can. What happens when the queues
// between the parcipants fill?

// This bit of code here is disconcerting because it indicates an asynchronous
// call back into the system that is waiting for it to complete. At first, I
// want to make this call synchronous because we do not want block here.
// However, there is still error reporting. We can implement `publish` so that
// it doesn't necessarily block, give real thought to how we should deal with
// stream overflow, and keep a consistent interface. We're going to decide that
// a high-water mark is unrecoverable, so this call would return an error, and
// that error will crash this participant.

//
Conference.prototype._broadcast = function (internal, method, message) {
    var cookie = this._nextCookie()
    var uniqueId = this.government.immigrated.promise[this.id]
    var key = method + '[' + uniqueId + '](' + cookie + ')'
    this._write.push({
        module: 'conference',
        method: 'broadcast',
        internal: internal,
        key: key,
        body: {
            module: 'conference',
            method: method,
            body: message
        }
    })
}

Conference.prototype.broadcast = function (method, message) {
    this._broadcast(false, method, message)
}

module.exports = Conference
