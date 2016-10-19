var abend = require('abend')

var assert = require('assert')
var util = require('util')

var cadence = require('cadence')
var logger = require('prolific.logger').createLogger('conference')
var interrupt = require('interrupt').createInterrupter('conference')

var Operation = require('operation')

var Cliffhanger = require('cliffhanger')
var Reactor = require('reactor')

var Cache = require('magazine')

var slice = [].slice

function Conference (colleague, operations) {
    this.isLeader = false
    this.replaying = colleague.replaying
    this.colleagueId = colleague.colleagueId
    this.islandName = colleague.islandName
    this.islandId = null
    this._enqueued = new Reactor({ object: this, method: '_onEnqueued' })
    this.participantId = null
    this._cliffhanger = new Cliffhanger
    this.colleague = colleague
    this.properties = {}
    this._participantIds = null
    // Do I need two Magazines? What where reductions?
    this._broadcasts = new Cache().createMagazine()
    this._reductions = new Cache().createMagazine()
    this._operations = operations
}

Conference.prototype.naturalized = function () {
    this.colleague.naturalized()
}

// You went through a lot of iterations with event emitters and whatnot. This
// has yet to settle. Not sure where everything lives in the stack. What is the
// difference between the Kibitzer and the Colleague? Well, I have notions.

// Thus the `EventEmitter` lives on in the Kibitzer, but a colleague consumer
// has a duck typed interface. In fact, I believe Kibitzer should use Vestibule
// instead of event emitter.

// So, keep in mind that everything here is tightly bound. We're not going to be
// spreading messages. Colleague is the network interface warpper. Conference is
// the application programming interface. We have them thightly coupled, but
// still pluggable.

// Notification that a message an enqueued into the Kibiter. Reach right into
// the Kibitzer to shift it.
Conference.prototype.enqueued = function () {
    this._enqueued.check()
}

// Respond an out of band request. If you want to return a status code you can
// just throw the integer value.
Conference.prototype.oob = cadence(function (async, name, post) {
    if (!this.isLeader) throw 504
    // TODO Maybe 404 if not found.
    this._operate({ qualifier: 'request', method: '.' + name, vargs: [ post ] }, async())
})

Conference.prototype._onEnqueued = cadence(function (async) {
    var loop = async(function () {
        var entry = this.colleague.kibitzer.shift()
        if (entry == null) {
            return [ loop.break ]
        }
        this._dispatch(entry, async())
    })()
})

Conference.prototype._createOperation = function (operation) {
    if (typeof operation == 'string') {
        assert(this._self, 'self cannot be null')
        operation = { object: this._self, method: operation }
    }
    return new Operation(operation)
}

Conference.prototype._setOperation = function (qualifier, name, operation) {
    var key = qualifier + ':' + name
    this._operations[key] = this._createOperation(operation)
}

Conference.prototype._getOperation = function (qualifier, name) {
    return this._operations[qualifier + ':' + name]
}

Conference.prototype.join = function (operation) {
    this._setOperation('internal', 'join', operation)
}

Conference.prototype.immigrate = function (operation) {
    this._setOperation('internal', 'immigrate', operation)
}

Conference.prototype.exile = function (operation) {
// TODO Rename 'invoke' or 'initiate'.
    this._setOperation('internal', 'exile', operation)
}

Conference.prototype.receive = function (name, operation) {
    this._setOperation('receive', '.' + name, operation)
}

Conference.prototype.operate = function (name, operation) {
    this._setOperation('operate', '.' + name, operation)
}

Conference.prototype.reduced = function (name, operation) {
    this._setOperation('reduced', '.' + name, operation)
}

Conference.prototype.naturalize = function () {
    assert(this._naturalizing != null, 'nothing is naturalizing')
    if (this.colleagueId == this._naturalizing.colleagueId) {
        this.conduit.naturalize()
    }
    delete this._naturalizing[colleagueId]
}

Conference.prototype._apply = cadence(function (async, qualifier, name, vargs) {
    var operation = this._operations[qualifier + '.' + name]
    if (operation) {
        operation.apply([], vargs.concact(async()))
    }
})

// TODO Should this be parallel with how ever many turnstiles?
Conference.prototype._operate = cadence(function (async, message) {
//    async([function () {
        var operation = this._getOperation(message.qualifier, message.method)
        if (operation == null) {
            return null
        }
        operation.operation.apply([], message.vargs.concat(operation.atomic ? async() : abend))
//    }, function (error) {
//        console.log(error.stack)
//    }])
})

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

Conference.prototype._dispatch = cadence(function (async, message) {
    switch (message.type) {
    case 'join':
        this._join(message.entry, async())
        break
    case 'entry':
        this._entry(message.entry, async())
        break
    }
})

Conference.prototype._join = cadence(function (async, entry) {
    console.log('join >', entry)
    this.colleagueId = this.colleague.colleagueId
    this.islandId = this.colleague.kibitzer.legislator.islandId
    this.islandName = this.colleague.kibitzer.legislator.islandName
    this._generateParticipantIds(entry)
    var bootstrapped = entry.promise == '1/0'
    this._operate({
        qualifier: 'internal',
        method: 'join',
        vargs: [ true, entry.properties[this.colleagueId] ]
    }, async())
})

Conference.prototype._generateParticipantIds = function (entry) {
    this._participantIds = {}
    entry.government
        .majority.concat(entry.government.minority)
        .concat(entry.government.constituents)
        .forEach(function (id) {
            this._participantIds[id] = entry.properties[id].immigrated + ':' + id
        }, this)
    this.participantId = this._participantIds[this.colleagueId]
}

// TODO Setting a property, we only have log messages here, so that our
// indication of what the properties are may be different, out of sync with what
// the underlying paxos thinks they are, thus, properties are only for
// bootstrapping.
Conference.prototype._entry = cadence(function (async, entry) {
    console.log('entry !>', entry)
    if (entry.government != null) {
    console.log('>', entry.properties)
        this._generateParticipantIds(entry)
        this.isLeader = entry.government.majority[0] == this.colleagueId
        // TODO Set immigration on bootstrap.
        if (entry.promise == '1/0') {
            console.log(this.colleagueId)
            entry.government.immigrate = { id: this.colleagueId }
        }
        var properties = entry.properties
        if (entry.government.immigrate) {
            this._operate({
                qualifier: 'internal',
                method: 'immigrate',
                vargs: [ entry.government.immigrate.id, properties, entry.promise ]
            }, async())
        } else if (entry.government.exile) {
            this._operate({
                qualifier: 'internal',
                method: 'exile',
                vargs: [ entry.government.exile, properties, entry.promise ]
            }, async())
            delete this._participantIds[entry.government.exile]
        }
    } else {
        var value = entry.value
        // TODO In order to be truely "queued" versus atomic, each of these
        // actions is going to need to be enqueued. We're going to need to know
        // before hand if the operation is atomic or queued. If it is atomic we
        // run the action here, if not we run it in a turnstile, but we need to
        // run the whole action, we can't just give `abend` to the user
        // function.
        switch (value.type) {
        case 'broadcast':
            async(function () {
                // TODO `_operate` versus `_operateIf`.
                this._operate({
                    qualifier: 'receive',
                    method: value.method,
                    vargs: [ value.request ]
                }, async())
            }, function (response) {
                logger.info('broadcasted', {
                    $request: value.reqeust,
                    $response: response
                })
                this.colleague.publish({
                    type: 'reduce',
                    from: this.participantId,
                    reductionKey: value.reductionKey,
                    cookie: value.cookie,
                    method: value.method,
                    request: value.request,
                    response: response
                }, async())
            })
            break
        // Tally our responses and if they match the number of participants,
        // then invoke the reduction method.
        case 'reduce':
            var reduction = this._reductions.hold(value.reductionKey, {})
            var complete = true
            reduction.value[value.from] = value.response
            for (var id in this._participantIds) {
                console.log('checking!>', this._participantIds[id])
                if (!(this._participantIds[id] in reduction.value)) {
                    complete = false
                    break
                }
            }
            if (complete) {
                reduction.remove()
                var reduced = []
                for (var participantId in reduction.value) {
                    reduced.push({
                        participantId: participantId,
                        value: reduction.value[participantId]
                    })
                }
                async(function () {
                    this._operate({
                        qualifier: 'reduced',
                        method: value.method,
                        vargs: [ value.request, reduced ]
                    }, async())
                }, function () {
                    var cartridge = this._broadcasts.hold(value.reductionKey, null)
                    if (cartridge.value != null) {
                        this._cliffhanger.resolve(cartridge.value.cookie, [ null, reduced ])
                    }
                    // TODO Might leak? Use Cadence finally.
                    cartridge.release()
                })
            } else {
                reduction.release()
            }
            break
        case 'send':
            if (this.participantId == value.to) {
                async(function () {
                    this._operate({
                        qualifier: 'receive',
                        method: value.method,
                        vargs: [ value.request ]
                    }, async())
                }, function (response) {
                    this.colleague.publish(this.reinstatementId, {
                        type: 'respond',
                        to: value.from,
                        response: response,
                        cookie: value.cookie
                    }, async())
                })
            }
            break
        case 'respond':
            if (this.participantId == value.to) {
                this._cliffhanger.resolve(value.cookie, [ null, value.response ])
            }
            break
        }
    }
})

Conference.prototype.outOfBand = cadence(function (async, name, post) {
    this.colleague.outOfBand(name, post, async())
})

Conference.prototype._message = cadence(function (async, message) {
    if (message.type == 'reinstate') {
        this.islandId = message.islandId
        this.reinstatementId = message.reinstatementId,
        this.colleagueId = message.colleagueId
    } else if (message.entry.value.government) {
        var value = message.entry.value

        // We need an id that is unique across restarts. A colleague may rejoin
        // with the same id. Two colleagues with the same id at the same time is
        // a usage error.
        //
        // We're going to respond to exile immediately within the Conference, so
        // we can run broadcasts, so the application will learn about exiles
        // eventually and always after the participant is gone.
        this._participantIds = {}
        value
            .government
            .majority.concat(value.government.minority)
            .concat(value.government.constituents)
            .forEach(function (id) {
                this._participantIds[id] = value.properties[id].immigrated + ':' + id
            }, this)
        this.participantId = this._participantIds[this.colleagueId]

        // Send a collapse message either here or from conduit.
        if (value.collapsed) {
// TODO Initial, naive implementation. Cancel and reset transitions.
            this._broadcasts.expire(Infinity)
            this._cliffhanger.cancel(function (cookie) {
                var cancelable = !! this._cancelable[cookie]
                delete this._cancelable[cookie]
                return cancelable
            }.bind(this))
            this._transition = null
        }
        if (value.government.promise == '1/0') {
            var leader = value.government.majority[0]
// TODO What is all this?
// TODO Come back and think hard about about rejoining.
            this.properties = {}
            this.properties[this._participantIds[leader]] = value.properties[leader]
            this.isLeader = true
            this._operate({
                qualifier: 'internal',
                method: 'join',
                vargs: [ true, this.colleague, value.properties[leader] ]
            }, abend)
            return
        } else {
            this.isLeader = value.government.majority[0] == this.colleagueId
        }

// TODO Exile is also indempotent. It will be run whether or not immigration
// completed. Another leader may have run a naturalization almost to completion.
// The naturalization broadcast may have been where the naturalization failed.
// This would mean that participants may have prepared themselves, committed
// themselves, but have not notified the Conference that naturalization has
// completed. Thus, we do this and it is really a rollback.
//
// At the time of writing, this seems like a lot to ask of application
// developers, but it works fine with my initial application of Compassion. Is
// my initial application a special case where indempotency, where rollback is
// easy, or do the requirements laid down by the atomic log send a developer
// down a path of indempotency.
//
// With that in mind, I'm going to, at this point, exile everything, even those
// things that I know have not yet immigrated.
        var exile = value.government.exile
        if (exile) {
            this._exiles.push({
                colleagueId: exile,
                participantId: this._participantIds[exile],
                promise: message.entry.promise
            })
            delete this._participantIds[exile]
        }

        // Order matters. Citizens must be naturalized in the same order in
        // which they immigrated. If not, one citizen might be made leader and
        // not know of another citizens immigration.
        var immigration = value.government.immigrate
        if (immigration) {
// TODO Add immigrated to citizen properties.
// TODO Put id in this object.
            this._immigrants.push(this._participantIds[immigration.id])
            this.properties[this._participantIds[immigration.id]] = value.properties[immigration.id]
            if (this.participantId == this._participantIds[immigration.id]) {
// TODO PROPERTIES Get dem properties here!
                this._operate({
                    qualifier: 'internal',
                    method: 'join',
                    vargs: [
                        false,
                        this.colleague,
                        value.properties[immigration.id]
                    ]
                }, abend)
            }
        }
    } else if (message.entry.type == 'paused' && message.entry.from == this.participantId) {
        this._paused = { head: message, tail: message }
    } else if (message.entry.value.to == this.participantId) {
        var value = message.entry.value
        var participantId = this.participantId
        switch (value.type) {
        case 'pause':
            this.colleague.publish(this.reinstatementId, {
                namespace: 'bigeasy.compassion.colleague.conference',
                type: 'paused',
                from: this.participantId,
                to: value.from,
                cookie: value.cookie
            }, async())
            break
        case 'paused':
            this._cliffhanger.resolve(value.cookie, [])
            break
            break
        case 'send':
            if (participantId == value.to) {
                async(function () {
                    this._operate({
                        qualifier: 'receive',
                        method: value.method,
                        vargs: [ value.request ]
                    }, async())
                }, function (response) {
                    this.colleague.publish(this.reinstatementId, {
                        namespace: 'bigeasy.compassion.colleague.conference',
                        type: 'respond',
                        to: value.from,
                        response: response,
                        cookie: value.cookie
                    }, async())
                })
            }
            break
        case 'respond':
            this._cliffhanger.resolve(value.cookie, [ null, value.response ])
            break
        }
    } else if (this._paused != null) {
        this._paused.tail = this._paused.tail.next = message
    } else if (message.entry.value.type == 'publish') {
        var value = message.entry.value
        async(function () {
            this._operate({
                qualifier: 'receive',
                method: value.method,
                vargs: [ value.request ]
            }, async())
        }, function (response) {
            if (value.from == this.participantId) {
                this._cliffhanger.resolve(value.cookie, [ null, response ])
            }
        })
    } else if (message.entry.value.type == 'broadcast') {
        var value = message.entry.value
        async(function () {
            this._operate({
                qualifier: 'receive',
                method: value.method,
                vargs: [ value.request ]
            }, async())
        }, function (response) {
            logger.info('broadcasted', { mesage: message, response: response })
            this.colleague.publish(this.reinstatementId, {
                namespace: 'bigeasy.compassion.colleague.conference',
                type: 'reduce',
                from: this.participantId,
                reductionKey: value.reductionKey,
                cookie: value.cookie,
                method: value.method,
                request: value.request,
                response: response
            }, async())
        })
    } else if (message.entry.value.type == 'reduce') {
        var value = message.entry.value
        // TODO Use Magazine.
        var reduction = this._reductions.hold(value.reductionKey, {})
        if (value.cancelable) {
            this._cancelable[value.reductionKey] = {}
        }
        var complete = true
        reduction.value[value.from] = value.response
        for (var id in this._participantIds) {
            if (!(this._participantIds[id] in reduction.value)) {
                complete = false
                break
            }
        }
        if (complete) {
            reduction.remove()
            var reduced = []
            for (var id in reduction.value) {
                reduced.push({ participantId: id, value: reduction.value[id] })
            }
            async(function () {
                this._operate({
                    qualifier: 'reduced',
                    method: value.method,
                    vargs: [ value.request, reduced ]
                }, async())
            }, function () {
                var cartridge = this._broadcasts.hold(value.reductionKey, null)
                if (cartridge.value != null) {
                    this._cliffhanger.resolve(cartridge.value.cookie, [ null, reduced ])
                }
                // TODO Might leak? Use Cadence finally.
                cartridge.release()
            })
        } else {
            reduction.release()
        }
    }
    this._checkTransitions()
})

Conference.prototype._checkTransitions = function () {
    if (this.isLeader && this._transition == null) {
        if (this._exiles.length != 0) {
            this._transition = 'exile'
            this._operate({
                qualifier: 'internal',
                method: 'exile',
                vargs: [
                    this._exiles[0].participantId,
                    this.properties[this._exiles[0].participantId],
                    this._exiles[0].promise
                ]
            }, abend)
        } else if (this._immigrants.length != 0) {
            this._immigrate(abend)
        }
    }
}

Conference.prototype._immigrate = cadence(function (async) {
    this._transition = 'naturalize'
    async(function () {
        this._send(true, '!properties', this._immigrants[0], this.properties, async())
    }, function () {
        this._operate({
            qualifier: 'internal',
            method: 'immigrate',
            vargs: [
                this._immigrants[0],
                this.properties[this._immigrants[0]],
                this.properties[this._immigrants[0]].immigrated
            ]
        }, async())
    })
})

Conference.prototype.send = cadence(function (async, method, colleagueId, message) {
    this._send(false, '.' + method, colleagueId, message, async())
})

Conference.prototype.broadcast = function (method, message, callback) {
    this._broadcast(method, message, callback || abend)
}

Conference.prototype._broadcast = cadence(function (async, method, message) {
    var cookie = this._cliffhanger.invoke(async())
    var participantId = this._participantIds[this.colleagueId]
    var reductionKey = method + '/' + participantId + '/' + cookie
    this._broadcasts.hold(reductionKey, { cookie: cookie }).release()
    this.colleague.publish({
        namespace: 'conference',
        type: 'broadcast',
        reductionKey: reductionKey,
        cookie: cookie,
        method: '.' + method,
        request: message
    }, async())
})

Conference.prototype.reduce = cadence(function (async, method, colleagueId, message) {
    this._reduce(false, '.' + method, colleagueId, message, async())
})

Conference.prototype.message = function (message) {
    this._enqueue(message, abend)
}

Conference.prototype._enqueue = function (message, callback) {
    switch (message.type) {
    case 'reinstate':
        this._message(message, callback)
        break
    case 'entry':
        if (message.entry.value.government || message.entry.value.namespace == 'bigeasy.compassion.colleague.conference') {
            this._message(message, callback)
        }
        break
    }
}

Conference.prototype.replay = function (entry) {
    if (entry.qualifier == 'bigeasy.conference' && entry.name == 'replay') {
        var operation = this._getOperation('operate', '.' + entry.method)
        if (operation == null) {
            return null
        }
        operation.apply(entry.vargs)
    }
}

Conference.prototype.record = function () {
    var vargs = slice.call(arguments)
    var method = vargs.shift()
    logger.info('replay', { method: method, vargs: vargs })
    this._operate({ qualifier: 'operate', method: '.' + method, vargs: vargs }, abend)
}

Conference.prototype._pause = cadence(function (async, colleagueId) {
    var cookie = this._cliffhanger.invoke(async())
    var participantId = this._participantIds[colleagueId]
    this._cancelable['!pause/' + participantId + '/' + cookie] = { cookie: cookie }
    this.colleague.publish(this.reinstatementId, {
        namespace: 'bigeasy.compassion.colleague.conference',
        type: 'pause',
        from: this._participantIds[this.colleagueId],
        to: colleagueId,
        cookie: cookie
    }, async())
})

Conference.prototype._send = cadence(function (async, cancelable, method, colleagueId, message) {
    var cookie = this._cliffhanger.invoke(async())
    if (cancelable) {
        var participantId = this._participantIds[colleagueId]
        this._cancelable[method + '/' + participantId + '/' + cookie] = { cookie: cookie }
    }
    this.colleague.publish(this.reinstatementId, {
        namespace: 'bigeasy.compassion.colleague.conference',
        type: 'send',
        cancelable: cancelable,
        from: this._participantIds[this.colleagueId],
        to: colleagueId,
        method: method,
        request: message,
        cookie: cookie
    }, async())
})

Conference.prototype.publish = cadence(function (async, method, message) {
    var cookie = this._cliffhanger.invoke(async())
    this.colleague.publish(this.reinstatementId, {
        namespace: 'bigeasy.compassion.colleague.conference',
        type: 'publish',
        from: this.participantId,
        method: '.' + method,
        request: message,
        cookie: cookie
    }, async())
})

Conference.prototype._reduce = cadence(function (async, cancelable, method, converanceId, message) {
    var participantId = this._participantIds[this.colleagueId]
    var reductionKey = method + '/' + converanceId
    this.colleague.publish({
        namespace: 'bigeasy.compassion.colleague.conference',
        type: 'converge',
        reductionKey: reductionKey,
        cancelable: cancelable,
        cookie: cookie,
        method: method,
        request: null,
        response: message
    }, async())
})

// TODO Should probably invoke `join` as part of naturalization.
Conference.prototype._setProperties = cadence(function (async, properties) {
    this.properties = properties
    return {}
})

Conference.prototype._naturalized = cadence(function (async, participantId) {
    assert(this._transtion == null || this._transition == participantId)
    this._transition = null
    if (this._immigrants[0] == participantId) {
        this._immigrants.shift()
    }
    console.error('>>>', 'naturalized!', participantId)
    if (this.participantId == participantId) {
        this.colleague.naturalized()
// TODO Mark and track naturalized.
// TODO Do I need some sort of a leadership work queue? Kind of. Something that
// begins and ends, so I can naturalize a member, then as a separate, cancelable
// task, I can add it to the routing table.
        var iterator = this._paused && this._paused.head.next
        this._paused = null
        var loop = async(function () {
            if (iterator == null) {
                return [ loop.break ]
            }
            iterator = iterator.next
            this._message(iterator, async())
        })()
    } else {
        this._checkTransitions()
    }
})

Conference.prototype._exiled = cadence(function (async, participantId) {
// TODO Set `_transition` to `null` on collpase.
    console.error('!!!', 'exiled!', participantId)
    assert(this._transtion == null || this._transition == participantId)
    this._transition = null
    if (this._exiles.length != 0 && this._exiles[0].participantId == participantId) {
        this._exiles.shift()
    }
    delete this.properties[participantId]
    this._checkTransitions()
})

Conference.prototype._cancel = function () {
    for (var key in this._cancelable) {
        this._broadcasts.hold(key, null).remove()
        this._reductions.hold(key, null).remove()
        var cookie = this._cancelable[key].cookie
        if (cookie != null) {
            this._cliffhanger.resolve(cookie, [ interrupt({ name: 'cancel' }) ])
        }
    }
    this._cliffhanger.cancel(function (cookie) {
        return this._cancelable[cookie]
    })
    this._cancelable = {}
}

function Queued (operations, object) {
    this._atomic = false
    this._operations = operations
    this._object = object
}

Queued.prototype.join = function (operation) {
    operation || (operation = 'join')
    this._setOperation(this._atomic, 'internal', 'join', operation)
}

Queued.prototype.immigrate = function (operation) {
    operation || (operation = 'immigrate')
    this._setOperation(this._atomic, 'internal', 'immigrate', operation)
}

Queued.prototype.exile = function (operation) {
    operation || (operation = 'exile')
    this._setOperation(this._atomic, 'internal', 'exile', operation)
}

Queued.prototype.receive = function (name, operation) {
    operation || (operation = name)
    this._setOperation(this._atomic, 'receive', '.' + name, operation)
}

Queued.prototype.reduced = function (name, operation) {
    operation || (operation = name)
    this._setOperation(this._atomic, 'reduced', '.' + name, operation)
}

Queued.prototype._setOperation = function (atomic, qualifier, name, operation) {
    var key = qualifier + ':' + name
    this._operations[key] = {
        atomic: atomic,
        operation: this._createOperation(operation)
    }
}

Queued.prototype._createOperation = function (operation) {
    if (typeof operation == 'string') {
        assert(this._object, 'object cannot be null')
        operation = { object: this._object, method: operation }
    }
    return new Operation(operation)
}

function Atomic (operations, object) {
    Queued.call(this, operations, object)
    this._atomic = true
}
util.inherits(Atomic, Queued)

Atomic.prototype.request = function (name, operation) {
    operation || (operation = name)
    this._setOperation(this._atomic, 'request', '.' + name, operation)
}

function NewConstructor (object) {
    this._operations = {}
    this.atomic = new Atomic(this._operations, object)
    this.queued = new Queued(this._operations, object)
}

NewConstructor.prototype.newConference = function (colleague) {
    return new Conference(colleague, this._operations)
}

exports.newConference = function (object) {
    return new NewConstructor(object)
}
