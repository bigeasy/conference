require('proof/redux')(24, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')
    var reactor = {
        join: cadence(function (async) {
            conference.ifNotReplaying(function () {
                assert(true, 'if not replaying sync')
            })
            conference.ifNotReplaying(cadence(function (async) {
                assert(true, 'if not replaying async')
                conference.record('record', { body: 1 }, async())
            }), async())
        }),
        request: cadence(function (async, value) {
            return value + 1
        }),
        message: cadence(function (async, value) {
            return value - 1
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
    var conference = new Conference(reactor, function (constructor) {
        constructor.join()
        constructor.immigrate(cadence(function (async, id) {
            if (conference.government.promise == '1/0') {
                conference.naturalized()
                var properties = conference.getProperties(id)
                assert(id, '1', 'immigrate id')
                assert(conference.getProperties(id), {}, 'immigrate properties')
            }
        }))
        constructor.exile()
        constructor.receive('message')
        constructor.reduced('message', 'messages')
        constructor.request('request')
        constructor.catalog('catalog')
    })
    var immigrate = {
        module: 'paxos',
        promise: '1/0',
        government: true,
        body: {
            module: 'paxos',
            promise: '1/0',
            majority: [ '1' ],
            minority: [],
            constituents: [],
            immigrant: { id: '1' },
            properties: { '1': {} },
            immigrated: {
                id: { '1/0': '1' },
                promise: { '1': '1/0' }
            }
        }
    }
    var outOfBands = [{
        expected: {
            to: '1',
            body: {
                module: 'conference',
                type: 'request',
                body: {
                    method: 'request',
                    body: 1
                }
            }
        },
        message: 'out of band'
    }, {
        expected: {
            to: '1',
            body: {
                module: 'conference',
                type: 'request',
                body: {
                    method: '_request',
                    body: 1
                }
            }
        },
        message: 'out of band not found request'
    }, {
        expected: {
            to: '1',
            body: {
                module: 'conference',
                type: 'backlog',
                body: {
                    promise: '2/0'
                }
            }
        },
        message: 'out of band not found request'
    }]
    var published = []
    var colleague = {
        published: 0,
        naturalized: function () {
            assert(true, 'naturalized')
        },
        publish: function (message, callback) {
            published.push(message)
            callback()
        },
        outOfBand: cadence(function (async, to, body) {
            var test = outOfBands.shift()
            assert({
                to: to,
                body: body
            }, test.expected, test.message)
            conference.responder.outOfBand(body, async())
        }),
        record: cadence(function (async, envelope) {
            assert(envelope, {
                module: 'conference',
                method: 'record',
                body: { body: 1 }
            }, 'record')
        }),
        kibitzer: {
            legislator: { id: '1' }
        }
    }
    async(function () {
        conference.responder.join(colleague, immigrate, async())
    }, function () {
        conference.responder.entry(immigrate, async())
    }, function () {
        conference.responder.entry({
            module: 'paxos',
            promise: '2/0',
            government: true,
            body: {
                module: 'paxos',
                promise: '2/0',
                majority: [ '1' ],
                minority: [],
                constituents: [ '2' ],
                immigrant: { id: '2' },
                properties: { '1': {}, '2': {} },
                immigrated: {
                    id: { '1/0': '1', '2/0': '2' },
                    promise: { '1': '1/0', '2': '2/0' }
                }
            }
        }, async())
    }, function () {
        assert(conference.government, {
            module: 'paxos',
            promise: '2/0',
            majority: [ '1' ],
            minority: [],
            constituents: [ '2' ],
            immigrant: { id: '2' },
            properties: { '1': {}, '2': {} },
            immigrated: {
                id: { '1/0': '1', '2/0': '2' },
                promise: { '1': '1/0', '2': '2/0' }
            }
        }, 'constituent added')
        conference.responder.outOfBand({
            module: 'conference',
            type: 'backlog',
            from: '2/0'
        }, async())
    }, function (broadcasts) {
        assert(broadcasts, {}, 'created backlog')
        conference.responder.entry({
            module: 'paxos',
            promise: '2/1',
            government: false,
            body: {
                module: 'conference',
                type: 'naturalized',
                from: '2/0'
            }
        }, async())
    }, function () {
        conference.responder.outOfBand({
            module: 'conference',
            type: 'backlog',
            from: '2/0'
        }, async())
    }, function (broadcasts) {
        assert(broadcasts, null, 'backlog not found')
        conference.outOfBand(conference.government.majority[0], 'request', 1, async())
    }, function (response) {
        assert(response, 2, 'out of band response')
        conference.outOfBand(conference.government.majority[0], '_request', 1, async())
    }, function (response) {
        assert(response, null, 'out of band not found')
        conference.broadcast('message', 1, async())
    }, function () {
        assert(published.shift(), {
            module: 'conference',
            type: 'broadcast',
            key: 'message[1/0](1)',
            method: 'message',
            body: 1
        }, 'published')
        conference.responder.entry({
            module: 'paxos',
            promise: '2/2',
            government: false,
            body: {
                module: 'conference',
                type: 'broadcast',
                key: 'message[1/0](1)',
                method: 'message',
                body: 1
            }
        }, async())
    }, function () {
        assert(published.shift(), {
            module: 'conference',
            type: 'reduce',
            from: '1/0',
            key: 'message[1/0](1)',
            method: 'message',
            body: 0
        }, 'first reduction published')
        conference.responder.entry({
            module: 'paxos',
            promise: '2/3',
            government: false,
            body: {
                module: 'conference',
                type: 'reduce',
                from: '1/0',
                key: 'message[1/0](1)',
                method: 'message',
                body: 7
            }
        }, async())
        conference.responder.entry({
            module: 'paxos',
            promise: '2/4',
            government: false,
            body: {
                module: 'conference',
                type: 'reduce',
                from: '2/0',
                key: 'message[1/0](1)',
                method: 'message',
                body: 5
            }
        }, async())
    }, function () {
        reactor.messages = cadence(function (async, responses, request) {
            assert({
                responses: responses,
                request: request
            }, {
                responses: [{ id: '1', value: 0 }],
                request: 1
            }, 'exile reduced')
        })
        // This time we're going to broadcast and then exile.
        conference.responder.entry({
            module: 'paxos',
            promise: '3/3',
            government: false,
            body: {
                module: 'conference',
                type: 'broadcast',
                key: 'message[1/0](2)',
                method: 'message',
                body: 1
            }
        }, async())
    }, function () {
        assert(published.shift(), {
            module: 'conference',
            type: 'reduce',
            from: '1/0',
            key: 'message[1/0](2)',
            method: 'message',
            body: 0
        }, 'broadcast again')
        conference.responder.entry({
            module: 'paxos',
            promise: '3/4',
            government: false,
            body: {
                module: 'conference',
                type: 'reduce',
                from: '1/0',
                key: 'message[1/0](2)',
                method: 'message',
                body: 0
            }
        }, async())
    }, function () {
        conference.responder.entry({
            module: 'paxos',
            promise: '3/0',
            government: true,
            body: {
                module: 'paxos',
                promise: '3/0',
                majority: [ '1' ],
                minority: [],
                constituents: [ ],
                exile: { id: '2', promise: '2/0', properties: {} },
                properties: { '1': {}, '2': {} },
                immigrated: {
                    id: { '1/0': '1' },
                    promise: { '1': '1/0' }
                }
            }
        }, async())
    }, function () {
        var reactor = {
            immigrate: cadence(function (async) {}),
            double: cadence(function (async, request) {
                return request * 2
            })
        }
        var colleague = {
            published: 0,
            naturalized: function () {
                assert(true, 'naturalized')
            },
            publish: function (message, callback) {
                published.push(message)
                callback()
            },
            outOfBand: cadence(function (async, to, body) {
                assert({
                    to: to,
                    body: body
                }, {
                    to: '1',
                    body: {
                        module: 'conference',
                        type: 'backlog',
                        from: '3/0'
                    }
                }, 'out of band backlog request')
                return {
                    'double[1/0](1)': {
                        key: 'double[1/0](1)',
                        request: 1,
                        method: 'double',
                        responses: { '1/0': 4 }
                    }
                }
            }),
            record: cadence(function (async, envelope) {
                assert(envelope, {
                    module: 'conference',
                    method: 'record',
                    body: { body: 1 }
                }, 'record')
            }),
            kibitzer: {
                legislator: { id: '3' }
            }
        }
        immigrate = {
            module: 'paxos',
            promise: '3/0',
            government: true,
            body: {
                module: 'paxos',
                promise: '3/0',
                majority: [ '1' ],
                minority: [],
                constituents: [ '2', '3' ],
                immigrant: { id: '3' },
                properties: { '1': {}, '2': {}, '3': {} },
                immigrated: {
                    id: { '1/0': '1', '2/0': '2', '3/0': '3' },
                    promise: { '1': '1/0', '2': '2/0', '3': '3/0' }
                }
            }
        }
        conference = new Conference(reactor, function (constructor) {
            constructor.receive('double')
            constructor.reduced('double', 'doubled')
        })
        conference.responder.join(colleague, immigrate, async())
    }, function () {
        conference.responder.entry(immigrate, async())
    }, function () {
        assert(published.shift(), {
            module: 'conference',
            type: 'reduce',
            from: '3/0',
            key: 'double[1/0](1)',
            method: 'double',
            body: 2
        }, 'broadcast backlog')
        assert(published.shift(), {
            module: 'conference',
            type: 'naturalized',
            from: '3/0',
            body: null
        }, 'naturalized')
    }, function () {
        conference._entry({
            module: 'paxos',
            promise: '4/0',
            government: true,
            body: {
                module: 'paxos',
                promise: '4/0',
                majority: [ '1', '2' ],
                minority: [ '3' ],
                constituents: [],
                properties: { '1': {}, '2': {}, '3': {} },
                immigrated: {
                    id: { '1/0': '1', '2/0': '2', '3/0': '3' },
                    promise: { '1': '1/0', '2': '2/0', '3': '3/0' }
                }
            }
        }, async())
    }, function () {
        assert(conference.government.promise, '4/0', 'government without immigration or exile')
    })
}
