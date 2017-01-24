var abend = require('abend')

var assert = require('assert')
var util = require('util')

var Monotonic = require('monotonic').asString

var coalesce = require('nascent.coalesce')

var cadence = require('cadence')
var logger = require('prolific.logger').createLogger('conference')
var interrupt = require('interrupt').createInterrupter('conference')

var Operation = require('operation')

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

//
function Responder (conference) {
    this._conference = conference
}

Responder.prototype.join = function (colleague, entry, callback) {
    this._conference._join(colleague, entry, callback)
}

Responder.prototype.entry = cadence(function (async, envelope) {
    this._conference._entry(envelope, async())
})

Responder.prototype.outOfBand = cadence(function (async, envelope) {
    this._conference._outOfBand(envelope, async())
})

function Conference (object, constructor) {
    this.isLeader = false
    this.colleague = null
    this.responder = new Responder(this)
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

    constructor(new Constructor(object, this._operations = {}))
}

Conference.prototype._nextCookie = function () {
    return this._cookie = Monotonic.increment(this._cookie, 0)
}

Conference.prototype.ifNotReplaying = function (f, callback) {
    if (!this.replaying) {
        if (callback) {
            f(callback)
        } else {
            f()
        }
    } else if (callback) {
        callback()
    }
}

Conference.prototype.naturalized = function () {
    this._colleague.naturalized()
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

Conference.prototype._join = cadence(function (async, colleague, entry) {
    this._colleague = colleague
    this.replaying = colleague.replaying
    this.id = colleague.kibitzer.legislator.id
    this.islandId = colleague.kibitzer.legislator.islandId
    this.islandName = colleague.kibitzer.legislator.islandName
    this.government = entry.body
    this._operate('internal', 'join', [ this ], async())
})

Conference.prototype._getBacklog = cadence(function (async) {
    async(function () {
        this._colleague.outOfBand(this.government.majority[0], {
            module: 'conference',
            type: 'backlog',
            from: this.government.promise
        }, async())
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
        this._colleague.publish({
            module: 'conference',
            type: 'naturalized',
            from: this.government.promise,
            body: null
        }, async())
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
                this._colleague.publish({
                    module: 'conference',
                    type: 'reduce',
                    from: this.government.immigrated.promise[this.id],
                    key: envelope.key,
                    method: envelope.method,
                    body: response
                }, async())
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

Conference.prototype.record = cadence(function (async, method, message) {
    this._colleague.record({
        module: 'conference',
        method: method,
        body: message
    }, async())
})

Conference.prototype.replay = function (name, message) {
    var operation = this._getOperation('catalog', name)
    if (operation != null) {
        operation.operation.apply([], [ message ])
    }
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
Conference.prototype.broadcast = cadence(function (async, method, message) {
    var cookie = this._nextCookie()
    var uniqueId = this.government.immigrated.promise[this.id]
    var key = method + '[' + uniqueId + '](' + cookie + ')'
    this._colleague.publish({
        module: 'conference',
        type: 'broadcast',
        key: key,
        method: method,
        body: message
    }, async())
})

module.exports = Conference
