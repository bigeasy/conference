require('proof')(5, require('cadence')(prove))

function prove (async, assert) {
    var abend = require('abend')
    var Counterfeiter = require('compassion.counterfeiter')
    var counterfeiter = new Counterfeiter

    var reduced = null

    var cadence = require('cadence')
    var reactor = {
        bootstrap: cadence(function (async) {
            return null
        }),
        join: cadence(function (async, conference) {
            conference.ifNotReplaying(async)(function () {
            })
        }),
        naturalized: cadence(function (async) {
            return null
        }),
        catalog: cadence(function (async, value) {
            assert(value, 1, 'cataloged')
        }),
        request: cadence(function (async, value) {
            return value + 1
        }),
        message: cadence(function (async, conference, value) {
            return value - 1
        }),
        government: cadence(function (async, conference) {
            if (conference.government.promise == '1/0') {
                assert(true, 'got a government')
            }
        }),
        messages: cadence(function (async, conference, reduction) {
            if (conference.id == 'third') {
                assert(reduction.request, 1, 'reduced request')
                assert(reduction.arrayed, [{
                    id: 'second', value: 0
                }, {
                    id: 'first', value: 0
                }, {
                    id: 'third', value: 0
                }], 'reduced responses')
                reduced()
            }
        }),
        exile: cadence(function (async, conference) {
            if (conference.id == 'third') {
                assert(conference.government.exile, {
                    id: 'fourth',
                    promise: '5/0',
                    properties: { key: 'value', url: 'fourth' }
                }, 'exile')
            }
        })
    }
    var Conference = require('..')
    assert(Conference, 'require')
    function createConference () {
        return new Conference(reactor, function (constructor) {
            constructor.setProperties({ key: 'value' })
            constructor.bootstrap()
            constructor.join()
            constructor.immigrate(cadence(function (async, id) {
                return
                if (conference.government.promise == '1/0') {
                    async(function () {
                        conference.outOfBand('1', 'request', 1, async())
                    }, function (response) {
                        assert(response, 2, 'out of band')
                        conference.naturalized()
                        var properties = conference.getProperties(id)
                        assert(id, '1', 'immigrate id')
                        assert(conference.getProperties(id), {}, 'immigrate properties')
                        assert(conference.getProperties('1/0'), {}, 'immigrate promise properties')
                        assert(conference.getProperties('2'), null, 'properites not found')
                    })
                }
            }))
            constructor.naturalized()
            constructor.exile()
            constructor.government()
            constructor.socket()
            constructor.receive('message')
            constructor.reduced('message', 'messages')
            constructor.request('request')
            constructor.method('catalog')
        })
    }
    counterfeiter.done.wait(abend)
    var conference = createConference()
    async(function () {
        counterfeiter.bootstrap({ conference: conference, id: 'first' }, async())
    }, function () {
        counterfeiter.events['first'].dequeue(async())
    }, function () {
        counterfeiter.join({
            conference: createConference(),
            id: 'second',
            leader: 'first',
            republic: counterfeiter.kibitzers['first'].paxos.republic
        }, async())
        counterfeiter.join({
            conference: createConference(),
            id: 'third',
            leader: 'first',
            republic: counterfeiter.kibitzers['first'].paxos.republic
        }, async())
    }, function () {
        counterfeiter.events['first'].join(function (envelope) {
            return envelope.promise == '4/1'
        }, async())
    }, function () {
        counterfeiter.join({
            conference: createConference(),
            id: 'fourth',
            leader: 'first',
            republic: counterfeiter.kibitzers['first'].paxos.republic
        }, async())
    }, function () {
        counterfeiter.events['fourth'].join(function (envelope) {
            return envelope.promise == '5/9'
        }, async())
        counterfeiter.events['third'].join(function (envelope) {
            return envelope.promise == '5/9'
        }, async())
    }, function (entry) {
        reduced = async()
        conference.broadcast('message', 1)
        counterfeiter.leave('fourth')
    }, function () {
        counterfeiter.destroy()
    })
}
