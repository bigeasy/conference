require('proof/redux')(9, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')
    var Conference = require('..')
    var events = require('events')
    assert(Conference, 'require')

    var wait
    var object = {
        test: function () {
            assert(this === object, 'object this')
        },
        join: cadence(function (async) {
            wait()
            return {}
        })
    }
    var conference = new Conference({ messages: new events.EventEmitter }, object)

    var specific = {
        test: function () {
            assert(this === specific, 'specific object')
        }
    }

    conference._createOperation({ object: specific, method: 'test' }).apply([], [])
    conference._createOperation('test').apply([], [])

    conference.join('join')

    conference.message({})

    async(function () {
        conference._enqueue({
            type: 'reinstate',
            islandId: '0',
            reinstatementId: 0,
            colleagueId: '0'
        }, async())
    }, function () {
        wait = async()
        assert(conference.islandId, '0', 'set island id')
        assert(conference.reinstatementId, 0, 'set reinstatement id')
        assert(conference.colleagueId, '0', 'set colleague id')
        assert(conference.participantId, null, 'participant id unset')
        conference._enqueue({
            type: 'entry',
// TODO Add government flag to message.
            isGovernment: true,
            entry: {
                value: {
                    government: {
                        promise: '1/0',
                        majority: [ '0' ],
                        minority: [],
                        constituents: []
                    },
                    properties: {
                        0: { immigrated: '1/0' }
                    }
                }
            }
        }, async())
    }, function () {
        assert(conference.properties, { '1/0:0': { immigrated: '1/0' } }, 'participants')
        conference._enqueue({
            type: 'entry',
// TODO Add government flag to message.
            isGovernment: true,
            entry: {
                value: {
                    government: {
                        promise: '2/0',
                        majority: [ '0' ],
                        minority: [],
                        constituents: [ '1' ],
                        immigrate: { id: '1' }
                    },
                    properties: {
                        0: { immigrated: '1/0' },
                        1: { immigrated: '2/0' }
                    }
                }
            }
        }, async())
    }, function () {
        assert(conference._immigrants, [ '2/0:1' ], 'immigrants')
    })
}
