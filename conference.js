// Common utilities.
var assert = require('assert')
var util = require('util')
var coalesce = require('extant')
var url = require('url')

// Control-flow utilities.
var cadence = require('cadence')

var Cliffhanger = require('cliffhanger')

var Procession = require('procession')

var Monotonic = require('monotonic').asString

var logger = require('prolific.logger').createLogger('conference')

var Operation = require('operation/variadic')

// Respond to requests from an evented message queue.
var Procedure = require('conduit/procedure')

var Client = require('conduit/client')
var Server = require('conduit/server')
var Multiplexer = require('conduit/multiplexer')

var Keyify = require('keyify')

var Vizsla = require('vizsla')

var Cubbyhole = require('cubbyhole')

var Staccato = require('staccato')

var raiseify = require('vizsla/raiseify')
var jsonify = require('vizsla/jsonify')
var jsonsify = require('vizsla/jsonsify')

// The patterns below take my back to my efforts to create immutable
// constructors when immutability was all the rage in Java-land. It would have
// pained me to create an object that continues to initialize the object after
// the constructor, but at the same time I'd have no real problem with reponding
// the headers in these streams. It was something that I abandoned after having
// spend some time with it, with rationale that I cannot remember. I can only
// remember the ratoinale that led me to adopt it.
//
// It did lead me to consider the folly of ORM, though.

// A `Procedure` class specific to the Conference that will respond to
// directives from a Colleague.

// Update: Actually, passing in the builder function feels somewhat immutable,
// about as immutable as JavaScript is going to get.

//
function Constructor (conference, properties, object, operations) {
    this._object = object
    this._operations = operations
    this._properties = properties
    this._setOperation([ true, 'request', 'backlog' ], [ conference, '_backlog' ])
}

Constructor.prototype._setOperation = function (key, vargs) {
    if (vargs.length == 0) vargs.push(key[key.length - 1])
    this._operations[Keyify.stringify(key)] = Operation(vargs, { object: this._object })
}

Constructor.prototype.setProperty = function (name, value) {
    this._properties[name] = value
}

Constructor.prototype.setProperties = function (properties) {
    for (var name in properties) {
        this.setProperty(name, properties[name])
    }
}

Constructor.prototype.responder = function () {
    this._setOperation([ 'responder' ], Array.prototype.slice.call(arguments))
}

Constructor.prototype.bootstrap = function () {
    this._setOperation([ 'bootstrap' ], Array.prototype.slice.call(arguments))
}

Constructor.prototype.join = function () {
    this._setOperation([ 'join' ], Array.prototype.slice.call(arguments))
}

Constructor.prototype.arrive = function () {
    this._setOperation([ 'arrive' ], Array.prototype.slice.call(arguments))
}

Constructor.prototype.acclimated = function () {
    this._setOperation([ 'acclimated' ], Array.prototype.slice.call(arguments))
}

Constructor.prototype.depart = function () {
    this._setOperation([ 'depart' ], Array.prototype.slice.call(arguments))
}

Constructor.prototype.government = function () {
    this._setOperation([ 'government' ], Array.prototype.slice.call(arguments))
}

Constructor.prototype.receive = function (name) {
    this._setOperation([ false, 'receive', name ], Array.prototype.slice.call(arguments, 1))
}

Constructor.prototype.reduced = function (name) {
    this._setOperation([ false, 'reduced', name ], Array.prototype.slice.call(arguments, 1))
}

/*
Constructor.prototype.request = function (name) {
    this._setOperation([ false, 'request', name ], Array.prototype.slice.call(arguments, 1))
}
*/

Constructor.prototype.socket = function () {
    this._setOperation([ 'socket' ], Array.prototype.slice.call(arguments))
}

Constructor.prototype.method = function (name) {
    this._setOperation([ 'method', name ], Array.prototype.slice.call(arguments, 1))
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

function Conference (object, constructor) {
    this._ua = new Vizsla
    logger.info('constructed', {})
    this.reactor = object
    this.isLeader = false
    this.colleague = null
    this.replaying = false
    this.id = null
    this._cookie = '0'
    this._boundary = '0'
    this._broadcasts = {}

    this._records = new Procession
    this._replays = this._records.shifter()

    // Currently exposed for testing, but feeling that these method should be
    // public for general testing, with one wrapper that hooks it up to the
    // colleague's streams and another that lets you send mock events.

    //
    this._dispatcher = new Dispatcher(this)

    this._multiplexer = new Multiplexer({
        incoming: new Server(this, '_connect'),
        conference: new Procedure(this._dispatcher, 'request')
    })

    this.read = this._multiplexer.read
    this.write = this._multiplexer.write

    // TODO Producer and consumer or similar?
    this.write.shifter().pump(this, '_entry')
    this.write.shifter().pump(this, '_play')

    this._cliffhanger = new Cliffhanger
    this._backlogs = new Cubbyhole

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

function record (conference, async) {
    return function () {
        var id = conference._nextBoundary()
        var steps = Array.prototype.slice.call(arguments)
        async(function () {
            if (conference.replaying) {
                async(function () {
                    conference._replays.dequeue(async())
                }, function (envelope) {
                    assert(envelope.id == id)
                    return [ envelope.body ]
                })
            } else {
                async.apply(null, steps)
            }
        }, function (result) {
            result = coalesce(result)
            conference.read.push({
                module: 'conference',
                method: 'record',
                id: id,
                body: result
            })
            return [ result ]
        })
    }
}

Conference.prototype._record = cadence(function (async, method) {
    record(this, async)(function () { method.call(null, async()) })
})

Conference.prototype.record = function () {
    if (arguments.length == 2) {
        this._record(arguments[0], arguments[1])
    } else {
        return record(this, arguments[0])
    }
}

Conference.prototype.boundary = function () {
    this.read.push({
        module: 'conference',
        method: 'boundary',
        id: this._boundary = Monotonic.increment(this._boundary, 0),
        entry: null
    })
}

Conference.prototype._invoke = cadence(function (async, method, body) {
    this.read.push({
        module: 'conference',
        method: 'invoke',
        body: { method: method, body: coalesce(body) }
    })
    this._operate([ 'method', method ], [ this, body ], async())
})

Conference.prototype.invoke = function (method, body, callback) {
    this._invoke(method, body, callback)
}

// Get the properties for a particular id or promise.

//
Conference.prototype.getProperties = function (id) {
    id = coalesce(this.government.arrived.id[id], id)
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
    case 'backlog':
        this._backlogs.wait(envelope.body.promise, async())
        break
    case 'properties':
        this.id = envelope.body.id
        this.replaying = envelope.body.replaying
        return [ this._properties ]
    }
})

Conference.prototype._fetch = cadence(function (async, to, header, queue) {
    async(function () {
        if (!this.replaying) {
            async(function () {
                this._ua.fetch({
                    url: this.government.properties[to].url
                }, {
                    url: './request',
                    post: header,
                    gateways: [ raiseify(), jsonsify({}) ]
                }, async())
            }, function (stream) {
                return new Staccato.Readable(stream)
            })
        }
    }, function (readable) {
        var loop = async(function () {
            this.record(async)(function () { readable.read(async()) })
        }, function (json) {
            queue.push(json)
            if (json == null) {
                return [ loop.break ]
            }
        })()
    })
})

// TODO Make `responder` accept an Operation and then have this renamed to `request`.
Conference.prototype.request = function (to, header) {
    var queue = new Procession
    this._fetch(to, header, queue, abend)
    return queue
}

Conference.prototype._sendResponse = cadence(function (async, header, queue) {
    var operation = this._operations[Keyify.stringify([ 'responder' ])]
    async(function () {
        setImmediate(async())
    }, function () {
        operation(this, header, queue, async())
    }, function () {
        return []
    })
})

// TODO Abend only being used in this one place.
var abend = require('abend')
Conference.prototype._connect = function (envelope) {
    var socket = { read: new Procession, write: new Procession }
    this._sendResponse(envelope, socket.read, abend)
    return socket
}

Conference.prototype._operate = cadence(function (async, key, vargs) {
    var operation = this._operations[Keyify.stringify(key)]
    if (operation == null) {
        return null
    }
    operation.apply(null, vargs.concat(async()))
})

Conference.prototype._getBacklog = cadence(function (async) {
    async(function () {
        this.record(async)(function () {
            this._ua.fetch({
                url: this.government.properties[this.government.majority[0]].url
            }, {
                url: './backlog',
                post: { promise: this.government.promise },
                gateways: [ raiseify(), jsonify({}) ]
            }, async())
        })
    }, function (body) {
        async.forEach(function (broadcast) {
            async(function () {
                this._consume({
                    // paxos
                    body: {
                        // islander
                        body: {
                            module: 'conference',
                            method: 'broadcast',
                            internal: broadcast.internal,
                            key: broadcast.key,
                            body: {
                                method: broadcast.method,
                                body: broadcast.request
                            }
                        }
                    }
                }, async())
            }, function () {
                async.forEach(function (promise) {
                    this._consume({
                        // paxos
                        body: {
                            // islander
                            body: {
                                module: 'conference',
                                method: 'reduce',
                                from: promise,
                                key: broadcast.key,
                                body: broadcast.responses[promise]
                            }
                        }
                    }, async())
                })(Object.keys(broadcast.responses))
            })
        })(body)
    }, function () {
        // TODO Probably not a bad idea, but what was I thinking?
    })
})

Conference.prototype._entry = cadence(function (async, envelope) {
    if (envelope == null || envelope.method != 'entry') {
        return []
    }
    var entry = envelope.body
    this.read.push({
        module: 'conference',
        method: 'boundary',
        id: this._nextBoundary(),
        entry: entry.promise
    })
    async(function () {
        this._consume(entry, async())
    }, function () {
        this.read.push({
            module: 'conference',
            method: 'consumed',
            id: this._nextBoundary(),
            entry: entry.promise
        })
    })
})

Conference.prototype._consume = cadence(function (async, entry) {
    if (entry.method == 'government') {
        this.government = entry.government
        this.isLeader = this.government.majority[0] == this.id
        var properties = entry.properties
        async(function () {
            if (entry.body.arrive) {
                var arrival = entry.body.arrive
                async(function () {
                    if (entry.body.promise == '1/0') {
                        this._operate([ 'bootstrap' ], [ this ], async())
                    } else if (arrival.id == this.id) {
                        this._operate([ 'join' ], [ this ], async())
                    }
                }, function () {
                    this._operate([ 'arrive' ], [ this, arrival.id ], async())
                }, function () {
                    if (arrival.id != this.id) {
                        var broadcasts = []
                        for (var key in this._broadcasts) {
                            broadcasts.push(JSON.parse(JSON.stringify(this._broadcasts[key])))
                        }
                        this._backlogs.set(this.government.promise, null, broadcasts)
                    } else if (this.government.promise != '1/0') {
                        this._getBacklog(async())
                    }
                }, function () {
                    this.read.push({
                        module: 'conference',
                        method: 'acclimated',
                        body: null
                    })
                })
            } else if (entry.body.departed) {
                var depart = entry.body.departed
                async(function () {
                    this._operate([ 'depart' ], [ this, depart.id, depart.promise, depart.properties ], async())
                }, function () {
                    var promise = depart.promise
                    var broadcasts = []
                    for (var key in this._broadcasts) {
                        delete this._broadcasts[key].responses[promise]
                        broadcasts.push(this._broadcasts[key])
                    }
                    this._backlogs.remove(promise)
                    async.forEach(function (broadcast) {
                        this._checkReduced(broadcast, async())
                    })(broadcasts)
                })
            }
        }, function () {
            if (entry.body.acclimate != null) {
                this._operate([ 'acclimated' ], [ this, entry.body.acclimate ], async())
            }
        }, function () {
            this._operate([ 'government' ], [ this ], async())
        })
    } else {
        assert(entry.body.body)
        // Reminder that if you ever want to do queued instead async then the
        // queue should be external and a property of the object the conference
        // operates.

        //
        var envelope = entry.body.body
        switch (envelope.method) {
        case 'broadcast':
            this._broadcasts[envelope.key] = {
                key: envelope.key,
                internal: coalesce(envelope.internal, false),
                method: envelope.body.method,
                request: envelope.body.body,
                responses: {}
            }
            async(function () {
                this._operate([
                    envelope.internal, 'receive', envelope.body.method
                ], [
                    this, envelope.body.body
                ], async())
            }, function (response) {
                this.read.push({
                    module: 'conference',
                    method: 'reduce',
                    key: envelope.key,
                    internal: coalesce(envelope.internal, false),
                    from: this.government.arrived.promise[this.id],
                    body: coalesce(response)
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
        }
    }
})

Conference.prototype._checkReduced = cadence(function (async, broadcast) {
    var complete = true
    for (var promise in this.government.arrived.id) {
        if (!(promise in broadcast.responses)) {
            complete = false
            break
        }
    }

    if (complete) {
        var reduced = []
        for (var promise in broadcast.responses) {
            reduced.push({
                promise: promise,
                id: this.government.arrived.id[promise],
                value: broadcast.responses[promise]
            })
        }
        this._operate([
            broadcast.internal, 'reduced', broadcast.method
        ], [
            this, {
                request: broadcast.request,
                arrayed: reduced,
                mapped: broadcast.responses
            }
        ], async())
        delete this._broadcasts[broadcast.key]
    }
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
    var uniqueId = this.government.arrived.promise[this.id]
    var key = method + '[' + uniqueId + '](' + cookie + ')'
    this.read.push({
        module: 'conference',
        method: 'broadcast',
        internal: coalesce(internal, false),
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
