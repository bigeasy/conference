var cadence = require('cadence')
var rescue = require('rescue')

function Cancelable (conference) {
    this._conference = conference
}

Cancelable.prototype.send = cadence(function (async, method, colleagueId, value) {
    this._conference._send(true, '.' + method, colleagueId, value, async())
})

Cancelable.prototype.broadcast = cadence(function (async, method, value) {
    async([function () {
        this._conference._broadcast(true, '.' + method, value, async())
    }, rescue(/^bigeasy.cliffhanger#canceled$/m)])
})

Cancelable.prototype.reduce = cadence(function (async, method, value) {
    async([function () {
        this._conference._reduce(true, '.' + method, value, async())
    }, rescue(/^bigeasy.cliffhanger#canceled$/m)])
})

Cancelable.prototype.pause = cadence(function (async, participantId) {
    async([function () {
        this._conference._pause(participantId, async())
    }, rescue(/^bigeasy.cliffhanger#canceled$/m)])
})

Cancelable.prototype.naturalize = cadence(function (async, participantId) {
    async([function () {
        async(function () {
            this._conference._broadcast(true, '!naturalize', participantId, async())
        }, function () {
            return []
        })
    }, rescue(/^bigeasy.cliffhanger#canceled$/m)])
})

Cancelable.prototype.exile = cadence(function (async, participantId) {
    async([function () {
        async(function () {
            this._conference._broadcast(true, '!exile', participantId, async())
        }, function () {
            return []
        })
    }, rescue(/^bigeasy.cliffhanger#canceled$/m)])
})

module.exports = Cancelable
