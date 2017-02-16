// Common utilities.
var assert = require('assert')
var util = require('util')

// Control-flow utilities.
var abend = require('abend')
var cadence = require('cadence')

var Cliffhanger = require('cliffhanger')

var Monotonic = require('monotonic').asString

var coalesce = require('nascent.coalesce')

var logger = require('prolific.logger').createLogger('conference')

var Operation = require('operation')

// Invoke round trip requests into an evented message queue.
var Requester = require('conduit/requester')

// Emit events into an evented message queue.
var Spigot = require('conduit/spigot')

// Respond to requests from an evented message queue.
var Responder = require('conduit/responder')

// Consume events from an evented message queue.
var Basin = require('conduit/basin')

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
    this._setOperation('!receive', 'welcomed', { object: conference, method: 'welcomed' })
}

Constructor.prototype._setOperation = function (qualifier, name, operation) {
    if (typeof operation == 'string') {
        assert(this._object, 'object cannot be null')
        operation = { object: this._object, method: operation }
    }
    this._operations[qualifier + ':' + name] = new Operation(operation)
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
    this._setOperation('internal', 'join', coalesce(method, 'join'))
}

Constructor.prototype.join = function (method) {
    this._setOperation('internal', 'join', coalesce(method, 'join'))
}

Constructor.prototype.immigrate = function (method) {
    this._setOperation('internal', 'immigrate', coalesce(method, 'immigrate'))
}

Constructor.prototype.exile = function (method) {
    this._setOperation('internal', 'exile', coalesce(method, 'exile'))
}

Constructor.prototype.government = function (method) {
    this._setOperation('internal', 'government', coalesce(method, 'government'))
}

Constructor.prototype.receive = function (name, method) {
    this._setOperation('receive', name, coalesce(method, name))
}

Constructor.prototype.reduced = function (name, method) {
    this._setOperation('reduced', name, coalesce(method, name))
}

Constructor.prototype.request = function (name, method) {
    this._setOperation('request', name, coalesce(name, method))
}

Constructor.prototype.catalog = function (name, method) {
    this._setOperation('catalog', name, coalesce(name, method))
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

// TODO Everything should go through a single pipe to preserve replayability.
// Even out-of-band and recordings, which means that on-boarding could become
// costly.
Dispatcher.prototype.fromBasin = cadence(function (async, envelope) {
    if (envelope == null) {
        return
    }
    switch (envelope.method) {
    case 'join':
        this._conference._join(envelope.body, async())
        break
    case 'entry':
        this._conference.boundary('_entry', { promise: envelope.body.promise })
        this._conference._entry(envelope.body, async())
        break
    case 'replay':
        this._conference._replay(envelope.body, async())
        break
    }
})

Dispatcher.prototype.fromSpigot = cadence(function (async, envelope) {
})

function Conference (object, constructor) {
    logger.info('constructed', {})
    this.isLeader = false
    this.colleague = null
    this.replaying = false
    this.id = null
    this._welcomes = {}
    this._cookie = '0'
    this._boundary = '0'
    this._broadcasts = {}
    this._backlogs = {}

    // Currently exposed for testing, but feeling that these method should be
    // public for general testing, with one wrapper that hooks it up to the
    // colleague's streams and another that lets you send mock events.

    //
    this._dispatcher = new Dispatcher(this)

    // Events go first through a `Responder` which will invoke our out of band
    // method and return the result. If the message is not an out of band
    // requests it is a message that does not expect a response. It will flow
    // into the `Basin` where we'll dispatch it and send to response.
    var responder = new Responder(this._dispatcher, 'colleague')
    this._basin = new Basin(this._dispatcher)

    responder.spigot.emptyInto(this._basin)

    // The basin into which events flow form the network.
    this.basin = responder.basin

    // Round trip events used to perform requests for out-of-band data.
    this._requester = new Requester('colleague')
    this.spigot = this._requester.spigot

    // An internal spigot that flows thorugh the requester used to publish and
    // record.
    this._spigot = new Spigot(this._dispatcher)
    this._spigot.emptyInto(this._requester.basin)

    this._cliffhanger = new Cliffhanger

    constructor(new Constructor(this, this._properties = {}, object, this._operations = {}))
}

// An ever increasing identify to adorn our broadcasts so that it's key will be
// unique when we combine our immigration promise with the cookie.

//
Conference.prototype._nextCookie = function () {
    return this._cookie = Monotonic.increment(this._cookie, 0)
}

// Run the given operation if we are not replaying a log. If we are not
// replaying then we are performing actions that generate out-of-band log
// entries. If we are replaying we want to replay those out-of-band log entries.

//
Conference.prototype._ifNotReplaying = cadence(function (async, operation) {
    if (!this.replaying) {
        new Operation(operation).apply([ async() ])
    }
})

Conference.prototype.ifNotReplaying = function () {
    this.boundary('_ifNotReplaying')
    if (arguments.length == 2) {
        this._ifNotReplaying(arguments[0], arguments[1])
    } else {
        var async = arguments[0], conference = this
        return function () {
            if (!this.replaying) {
                async.apply(null, Array.prototype.slice.call(arguments))
            }
        }
    }
}

Conference.prototype.boundary = function (name, context) {
    var body = {}
    if (name != null) {
        body.name = name
        for (var key in context || (context = {})) {
            body[key] = context[key]
        }
    }
    this.spigot.requests.push({
        module: 'conference',
        method: 'boundary',
        id: this._boundary = Monotonic.increment(this._boundary, 0),
        body: body
    })
}

Conference.prototype.record = cadence(function (async, method, message) {
    this.spigot.requests.push({
        module: 'conference',
        method: 'record',
        body: { method: method, body: message }
    })
    this._replay({ method: method, body: message }, async())
})

// TODO Why sometimes wait? I don't want to wait on naturalized. I'm assuming
// that we're not going to publish much, and that we're not going to wait for
// the queue to empty. We don't have back-pressure and if we did have
// back-pressure, we would have deadlock. We should push, not enqueue.

//
Conference.prototype.naturalized = function () {
    this._spigot.requests.push({
        module: 'conference',
        method: 'naturalized',
        body: null
    })
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
        this._operate('request', envelope.method, [ envelope.body ], async())
        break
    case 'backlog':
        return [ coalesce(this._backlogs[envelope.from]) ]
    }
})

Conference.prototype._getOperation = function (qualifier, name) {
    return this._operations[qualifier + ':' + name]
}

Conference.prototype._operate = cadence(function (async, qualifier, method, vargs) {
    var operation = this._getOperation(qualifier, method)
    if (operation == null) {
        return null
    }
    operation.apply(vargs.concat(async()))
})

Conference.prototype._join = cadence(function (async, join) {
    this.replaying = join.replaying
    this.id = join.id
    this._operate('internal', 'join', [ this ], async())
})

Conference.prototype._getBacklog = cadence(function (async) {
    async(function () {
        this._requester.request('colleague', {
            module: 'conference',
            method: 'backlog',
            to: this.government.majority[0],
            from: this.government.promise,
            body: null
        }, 'conference', async())
    }, function (broadcasts) {
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
            this._entry(entry, async())
        })(entries)
    }, function () {
        // TODO Probably not a bad idea, but what was I thinking?
        this._spigot.requests.push({
            module: 'conference',
            method: 'naturalized',
            body: null
        })
    })
})

Conference.prototype.makeWelcome = function (welcome) {
    assert(this.government.immigrated != null, 'can only be called during immigration')
    this._welcomes[this.government.promise] = welcome
}

Conference.prototype.welcomed = cadence(function (async, conference, promise) {
    delete this._welcomes[promise]
})

Conference.prototype._entry = cadence(function (async, entry) {
    if (entry.method == 'government') {
        this.government = entry.body
        this.isLeader = this.government.majority[0] == this.id
        var properties = entry.properties
        async(function () {
            if (this.government.immigrated) {
                var immigrant = this.government.immigrated
                async(function () {
                    if (this.government.promise == '1/0') {
                        this._operate('internal', 'bootstrap', [ this ], async())
                    } else if (this.government.immigrant.id == this._kibitzer.paxos.id) {
                        this._operate('internal', 'join', [ this ], async())
                    }
                }, function () {
                    this._operate('internal', 'immigrate', [ this, immigrant.id ], async())
                }, function () {
                    if (immigrant.id != this.id) {
                        this._backlogs[this.government.promise] = JSON.parse(JSON.stringify(this._broadcasts))
                    } else if (this.government.promise != '1/0') {
                        this._getBacklog(async())
                    }
                }, function () {
                    if (this.government.promise == '1/0' || immigrant.id == this.id) {
                        this._broadcast(true, 'welcomed', this.government.promise)
                    }
                })
            } else if (this.government.exile) {
                var exile = this.government.exile
                async(function () {
                    this._operate('internal', 'exile', [ exile.id, exile.promise, exile.properties ], async())
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
            this._operate('internal', 'government', [ this ], async())
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
                this._operate(prefix + 'receive', envelope.body.method, [ this, envelope.body.body ], async())
            }, function (response) {
                this.spigot.requests.push({
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
        var prefix = broadcast.internal ? '!' : ''
        this._operate(prefix + 'reduced', broadcast.method, [ reduced, broadcast.request ], async())
        delete this._broadcasts[broadcast.key]
    }
})

// TODO Save welcomes, or introductions, and have them expire when the welcome
// expires, and maybe that is the entirety of out-of-band.

//
Conference.prototype.request = cadence(function (async, method, body) {
    this.spigot.requests.push({
        // Do not think it odd that this is nested and `'backlog'` is not.
        // It reflects that one is system internal and the other is four our
        // dear user.
        module: 'conference',
        method: 'request',
        body: {
            module: 'conference',
            method: method,
            cookie: this._cliffhanger.invoke(async()),
            from: this.government.promise,
            body: body
        }
    })
})

Conference.prototype._replay = cadence(function (async, record) {
    this._operate('catalog', record.method, [ record.body ], async())
})

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
    this.spigot.requests.push({
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
