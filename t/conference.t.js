require('proof')(2, require('cadence')(prove))

function prove (async, assert) {
    var abend = require('abend')
    var Counterfeiter = require('compassion.counterfeiter')
    var counterfeiter = new Counterfeiter

    var cadence = require('cadence')
    var reactor = {
        bootstrap: cadence(function (async) {
        }),
        join: cadence(function (async, conference) {
            conference.ifNotReplaying(async)(function () {
            })
        }),
        naturalized: cadence(function (async) {
        }),
        catalog: cadence(function (async, value) {
            assert(value, 1, 'cataloged')
        }),
        request: cadence(function (async, value) {
            return value + 1
        }),
        message: cadence(function (async, value) {
            return value - 1
        }),
        government: cadence(function (async, conference) {
            if (conference.government.promise == '1/0') {
                assert(true, 'got a government')
            }
        }),
        messages: cadence(function (async, responses, request) {
            assert(request, 1, 'reduced request')
            assert(responses, [{
                id: '1', value: 7
            }, {
                id: '2', value: 5
            }], 'reduced responses')
        }),
        exile: cadence(function (async, id, promise, properties) {
            assert({
                id: id,
                promise: promise,
                properties: properties
            }, {
                id: '2',
                promise: '2/0',
                properties: {}
            }, 'exile')
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
    async(function () {
        var conference = createConference()
        counterfeiter.bootstrap({ conference: conference, id: 'first' }, async())
    }, function () {
        counterfeiter.events['first'].dequeue(async())
    }, function () {
        var conference = createConference()
        counterfeiter.join({
            conference: conference,
            id: 'second',
            leader: 'first',
            republic: counterfeiter.kibitzers['first'].paxos.republic
        }, async())
    }, function () {
        counterfeiter.events['second'].dequeue(async())
    }, function (entry) {
        console.log('ENTRY', entry)
        counterfeiter.events['second'].dequeue(async())
    }, function (entry) {
        console.log('ENTRY', entry)
        counterfeiter.destroy()
    })
}
