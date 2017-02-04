// Common utilities.
var assert = require('assert')
var util = require('util')

// Control-flow utilities.
var abend = require('abend')
var cadence = require('cadence')


var Monotonic = require('monotonic').asString

var coalesce = require('nascent.coalesce')

var logger = require('prolific.logger').createLogger('conference')
var interrupt = require('interrupt').createInterrupter('conference')

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
function Constructor (object, operations) {
    this._object = object
    this._operations = operations
}

Constructor.prototype._setOperation = function (qualifier, name, operation) {
    if (typeof operation == 'string') {
        assert(this._object, 'object cannot be null')
        operation = { object: this._object, method: operation }
    }
    this._operations[qualifier + ':' + name] = new Operation(operation)
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

Dispatcher.prototype.request = function (envelope, callback) {
    this._conference._outOfBand(envelope, callback)
}

Dispatcher.prototype.fromBasin = function (envelope, callback) {
    switch (envelope.method) {
    case 'join':
        this._conference._join(envelope.body, callback)
        break
    case 'entry':
        this._conference._entry(envelope.body, callback)
        break
    case 'replay':
        this._conference._replay(envelope.body, callback)
        break
    }
}

function Conference (object, constructor) {
    this.isLeader = false
    this.colleague = null
    this.replaying = false
    this.id = null
    this.islandName = null
    this.islandId = null
    this.participantId = null
    this.properties = {}
    this._cookie = '0'
    this._participantIds = null
    this._broadcasts = {}
    this._backlogs = {}

    // Currently exposed for testing, but feeling that these method should be
    // public for general testing, with one wrapper that hooks it up to the
    // colleague's streams and another that lets you send mock events.
    this._dispatcher = new Dispatcher(this)

    // Events go first through a `Responder` which will invoke our out of band
    // method and return the result. If the message is not an out of band
    // requests it is a message that does not expect a response. It will flow
    // into the `Basin` where we'll dispatch it and send to response.
    var responder = new Responder(this._dispatcher, 'conference')
    var basin = new Basin(this._dispatcher)

    responder.spigot.emptyInto(basin)

    // The basin into which events flow form the network.
    this.basin = responder.basin

    // Round trip events used to perform requests for out-of-band data.
    this._requester = new Requester('conference')
    this.spigot = this._requester.spigot

    // An internal spigot that flows thorugh the requester used to publish and
    // record.
    this._spigot = new Spigot(this._dispatcher)
    this._spigot.emptyInto(this._requester.basin)

    constructor(new Constructor(object, this._operations = {}))
}

// An ever increasing identify to adorn our broadcasts so that it's key will be
// unique when we combine our immigration promise with the cookie.

//
Conference.prototype._nextCookie = function () {
    return this._cookie = Monotonic.increment(this._cookie, 0)
}

Conference.prototype.ifNotReplaying = cadence(function (async, operation) {
    if (!this.replaying) {
        new Operation(operation).apply([ async() ])
    }
})

// TODO Why sometimes wait? I don't want to wait on naturalized. I'm assuming
// that we're not going to publish much, and that we're not going to wait for
// the queue to empty. We don't have back-pressure and if we did have
// back-pressure, we would have deadlock. We should push, not enqueue.
Conference.prototype.naturalized = function () {
    this._spigot.requests.push({
        module: 'conference',
        method: 'naturalized',
        body: null
    })
}

Conference.prototype.getProperties = function (id) {
    return coalesce(this.government.properties[id])
}

// Respond an out of band request. If you want to return a status code you can
// just throw the integer value.
//
// Not sure if I want to use UNIX codes or HTTP status codes. Leaning toward
// HTTP status codes and throwing them as integer.

//
Conference.prototype._outOfBand = cadence(function (async, envelope) {
    switch (envelope.type) {
    case 'request':
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
        this._requester.request('conference', {
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
                    type: 'broadcast',
                    key: key,
                    method: broadcast.method,
                    body: broadcast.request
                }
            })
            for (var promise in broadcast.responses) {
                entries.push({
                    body: {
                        module: 'conference',
                        type: 'reduce',
                        from: promise,
                        key: key,
                        method: broadcast.method,
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

Conference.prototype._entry = cadence(function (async, entry) {
    if (entry.government) {
        this.government = entry.body
        this.isLeader = this.government.majority[0] == this.id
        var properties = entry.properties, immigrant
        if (immigrant = this.government.immigrant) {
            async(function () {
                this._operate('internal', 'immigrate', [ this.government.immigrant.id ], async())
            }, function () {
                if (immigrant.id != this.id) {
                    this._backlogs[this.government.promise] = JSON.parse(JSON.stringify(this._broadcasts))
                } else if (this.government.promise != '1/0') {
                    this._getBacklog(async())
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
    } else {
        // Reminder that if you ever want to do queued instead async then the
        // queue should be external and a property of the object the conference
        // operates.

        //
        var envelope = entry.body
        switch (envelope.type) {
        case 'broadcast':
            this._broadcasts[envelope.key] = {
                key: envelope.key,
                method: envelope.method,
                request: envelope.body,
                responses: {}
            }
            async(function () {
                this._operate('receive', envelope.method, [ envelope.body ], async())
            }, function (response) {
                this._spigot.requests.push({
                    module: 'conference',
                    type: 'reduce',
                    from: this.government.immigrated.promise[this.id],
                    key: envelope.key,
                    method: envelope.method,
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
        this._operate('reduced', broadcast.method, [ reduced, broadcast.request ], async())
        delete this._broadcasts[broadcast.key]
    }
})

// TODO Save welcomes, or introductions, and have them expire when the welcome
// expires, and maybe that is the entirety of out-of-band.
Conference.prototype.outOfBand = cadence(function (async, to, method, body) {
    this._colleague.outOfBand(to, {
        module: 'conference',
        type: 'request',
        body: {
            method: method,
            body: body
        }
    }, async())
})

// Note that we don't wait on enqueuing the request, but we do wait on replay.
// Replay is independent of the consensus algorithm.
Conference.prototype.record = cadence(function (async, method, message) {
    this._spigot.requests.push({
        module: 'conference',
        method: 'record',
        body: {
            method: method,
            body: message
        }
    })
    this._replay({ method: method, body: message }, async())
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
Conference.prototype.broadcast = function (method, message) {
    var cookie = this._nextCookie()
    var uniqueId = this.government.immigrated.promise[this.id]
    var key = method + '[' + uniqueId + '](' + cookie + ')'
    this._spigot.requests.push({
        module: 'conference',
        type: 'broadcast',
        key: key,
        method: method,
        body: message
    })
}

module.exports = Conference
