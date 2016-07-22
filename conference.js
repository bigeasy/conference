var abend = require('abend')

var assert = require('assert')

var cadence = require('cadence')
var logger = require('prolific.logger').createLogger('bigeasy.conference')
var interrupt = require('interrupt').createInterrupter('bigeasy.conference')

var Operation = require('operation')

var Cliffhanger = require('cliffhanger')
var Cancelable = require('./cancelable')

var Cache = require('magazine')

function Conference (colleague, self) {
    this.isLeader = false
    this.islandId = null
    this.reinstatementId = null
    this.colleagueId = null
    this.participantId = null
    this._cliffhanger = new Cliffhanger
    this._colleague = colleague
    this._self = self || null
    this.properties = {}
    this._participantIds = null
    this._broadcasts = new Cache().createMagazine()
    this._reductions = new Cache().createMagazine()
    this._immigrants = []
    this.cancelable = new Cancelable(this)
    this._cancelable = {}
    this._exiles = []
    this._operations = {}
    this._setOperation('reduced', '!naturalize', { object: this, method: '_naturalized' })
    this._setOperation('reduced', '!exile', { object: this, method: '_exiled' })
    this._colleague.messages.on('message', this.message.bind(this))
}

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
    async([function () {
        var operation = this._getOperation(message.qualifier, message.method)
        if (operation == null) {
            return null
        }
        operation.apply([], message.vargs.concat(async()))
    }, /^compassion.colleage.confer#cancelled$/m, function () {
        return [ async.break ]
    }])
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
            this._cliffhanger.cancel(function () { return true })
            this._transition = null
        }
        if (value.government.promise == '1/0') {
            var leader = value.government.majority[0]
// TODO What is all this?
// TODO Come back and think hard about about rejoining.
            this.properties[this._participantIds[leader]] = value.properties[leader]
            this.isLeader = true
            this._operate({
                qualifier: 'internal',
                method: 'join',
                vargs: [ true, this._colleague, value.properties[leader] ]
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
// TODO Add immigrated to citizen properties
// TODO Put id in this object.
            this._immigrants.push(this._participantIds[immigration.id])
            this.properties[this._participantIds[immigration.id]] = value.properties[immigration.id]
            if (this.participantId == this._participantIds[immigration.id]) {
                this._operate({
                    qualifier: 'internal',
                    method: 'join',
                    vargs: [
                        false,
                        this._colleague,
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
            this._colleague.publish(this.reinstatementId, {
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
                    this._colleague.publish(this.reinstatementId, {
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
            this._colleague.publish(this.reinstatementId, {
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
            async(function () {
                this._operate({
                    qualifier: 'reduced',
                    method: value.method,
                    vargs: [ reduction.value, value.request ]
                }, async())
            }, function () {
                var cartridge = this._broadcasts.hold(value.reductionKey, null)
                if (cartridge.value != null) {
                    this._cliffhanger.resolve(cartridge.value.cookie, [ null, reduction.value ])
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
            this._transition = 'naturalize'
            this._operate({
                qualifier: 'internal',
                method: 'immigrate',
                vargs: [
                    this._immigrants[0],
                    this.properties[this._immigrants[0]],
                    this.properties[this._immigrants[0]].immigrated
                ]
            }, abend)
        }
    }
}

Conference.prototype.send = cadence(function (async, method, colleagueId, message) {
    this._send(false, '.' + method, colleagueId, message, async())
})

Conference.prototype.broadcast = cadence(function (async, method, message) {
    this._broadcast(false, '.' + method, message, async())
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

Conference.prototype._pause = cadence(function (async, colleagueId) {
    var cookie = this._cliffhanger.invoke(async())
    var participantId = this._participantIds[colleagueId]
    this._cancelable['!pause/' + participantId + '/' + cookie] = { cookie: cookie }
    this._colleague.publish(this.reinstatementId, {
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
    this._colleague.publish(this.reinstatementId, {
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
    this._colleague.publish(this.reinstatementId, {
        namespace: 'bigeasy.compassion.colleague.conference',
        type: 'publish',
        from: this.participantId,
        method: '.' + method,
        request: message,
        cookie: cookie
    }, async())
})

Conference.prototype._broadcast = cadence(function (async, cancelable, method, message) {
    var cookie = this._cliffhanger.invoke(async())
    var participantId = this._participantIds[this.colleagueId]
    var reductionKey = method + '/' + participantId + '/' + cookie
    if (cancelable) {
        this._cancelable[reductionKey] = { cookie: cookie }
    }
    this._broadcasts.hold(reductionKey, { cookie: cookie }).release()
    this._colleague.publish(this.reinstatementId, {
        namespace: 'bigeasy.compassion.colleague.conference',
        type: 'broadcast',
        reductionKey: reductionKey,
        cancelable: cancelable,
        cookie: cookie,
        method: method,
        request: message
    }, async())
})

Conference.prototype._reduce = cadence(function (async, cancelable, method, converanceId, message) {
    var participantId = this._participantIds[this.colleagueId]
    var reductionKey = method + '/' + converanceId
    this._colleague.publish({
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

Conference.prototype._naturalized = cadence(function (async, responses, participantId) {
    assert(this._transtion == null || this._transition == participantId)
    this._transition = null
    if (this._immigrants[0] == participantId) {
        this._immigrants.shift()
    }
    console.error('>>>', 'naturalized!', participantId)
    if (this.participantId == participantId) {
        this._colleague.naturalized()
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

Conference.prototype._exiled = cadence(function (async, responses, participantId) {
// TODO Set `_transition` to `null` on collpase.
    assert(this._transtion == null || this._transition == participantId)
    this._transition = null
    this._immigrants = this._immigrants.filter(function (immigrantId) {
        return immigrantId == participantId
    })
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

module.exports = Conference
