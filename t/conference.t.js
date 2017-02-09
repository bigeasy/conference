require('proof/redux')(27, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')
    var reactor = {
        join: cadence(function (async) {
            conference.ifNotReplaying(cadence(function (async) {
                assert(true, 'if not replaying async')
                async(function () {
                    conference.record('catalog', 1, async())
                })
            }), async())
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
    var conference = new Conference(reactor, function (constructor) {
        constructor.join()
        constructor.immigrate(cadence(function (async, id) {
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
        constructor.exile()
        constructor.government()
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
    var requests = conference.spigot.requests.shifter()
    var dispatcher = conference._dispatcher
    async(function () {
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'join',
            body: {
                id: '1',
                replaying: false
            }
        }, async())
    }, function () {
        assert(requests.shift(), {
            module: 'conference',
            method: 'record',
            body: { method: 'catalog', body: 1 }
        }, 'recorded')
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: immigrate
        }, async())
        assert(requests.shift(), {
            module: 'conduit',
            to: 'conference',
            from: 'conference',
            cookie: '1',
            body: {
                module: 'conference',
                method: 'outOfBand',
                body: {
                    module: 'conference',
                    method: 'request',
                    to: '1',
                    from: '1/0',
                    body: 1
                }
            }
        }, 'out of band request')
        conference.spigot.responses.enqueue({
            module: 'conduit',
            method: 'request',
            to: 'conference',
            cookie: '1',
            body: 2
        }, async())
    }, function () {
        assert(requests.shift(), {
            module: 'conference',
            method: 'naturalized',
            body: null
        }, 'naturalized')
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: {
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
        dispatcher.request({
            module: 'conference',
            method: 'backlog',
            from: '2/0'
        }, async())
    }, function (broadcasts) {
        assert(broadcasts, {}, 'created backlog')
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: {
                module: 'paxos',
                promise: '2/1',
                government: false,
                body: {
                    module: 'conference',
                    method: 'naturalized',
                    from: '2/0'
                }
            }
        }, async())
    }, function () {
        dispatcher.request({
            module: 'conference',
            method: 'backlog',
            from: '2/0',
            body: null
        }, async())
    }, function (broadcasts) {
        assert(broadcasts, null, 'backlog not found')
        dispatcher.request({
            module: 'conference',
            method: 'outOfBand',
            from: '2/0',
            body: {
                method: 'request',
                body: 1
            }
        }, async())
    }, function (response) {
        assert(response, 2, 'out of band response')
        dispatcher.request({
            module: 'conference',
            method: 'outOfBand',
            from: '2/0',
            body: {
                method: '_request',
                body: 1
            }
        }, async())
    }, function (response) {
        assert(response, null, 'out of band not found')
        conference.broadcast('message', 1)
    }, function () {
        assert(requests.shift(), {
            module: 'conference',
            method: 'broadcast',
            key: 'message[1/0](1)',
            body: {
                module: 'conference',
                method: 'message',
                body: 1
            }
        }, 'published')
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: {
                module: 'paxos',
                promise: '2/2',
                government: false,
                body: {
                    module: 'conference',
                    method: 'broadcast',
                    key: 'message[1/0](1)',
                    body: {
                        method: 'message',
                        body: 1
                    }
                }
            }
        }, async())
    }, function () {
        assert(requests.shift(), {
            module: 'conference',
            method: 'reduce',
            from: '1/0',
            key: 'message[1/0](1)',
            body: 0
        }, 'first reduction published')
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: {
                module: 'paxos',
                promise: '2/3',
                government: false,
                body: {
                    module: 'conference',
                    method: 'reduce',
                    from: '1/0',
                    key: 'message[1/0](1)',
                    body: 7
                }
            }
        }, async())
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: {
                module: 'paxos',
                promise: '2/4',
                government: false,
                body: {
                    module: 'conference',
                    method: 'reduce',
                    from: '2/0',
                    key: 'message[1/0](1)',
                    body: 5
                }
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
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: {
                module: 'paxos',
                promise: '3/3',
                government: false,
                body: {
                    module: 'conference',
                    method: 'broadcast',
                    key: 'message[1/0](2)',
                    body: {
                        method: 'message',
                        body: 1
                    }
                }
            }
        }, async())
    }, function () {
        assert(requests.shift(), {
            module: 'conference',
            method: 'reduce',
            from: '1/0',
            key: 'message[1/0](2)',
            body: 0
        }, 'broadcast again')
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: {
                module: 'paxos',
                promise: '3/4',
                government: false,
                body: {
                    module: 'conference',
                    method: 'reduce',
                    from: '1/0',
                    key: 'message[1/0](2)',
                    body: 0
                }
            }
        }, async())
    }, function () {
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: {
                module: 'paxos',
                promise: '3/0',
                government: true,
                body: {
                    module: 'paxos',
                    promise: '3/0',
                    majority: [ '1' ],
                    minority: [],
                    constituents: [],
                    exile: { id: '2', promise: '2/0', properties: {} },
                    properties: { '1': {}, '2': {} },
                    immigrated: {
                        id: { '1/0': '1' },
                        promise: { '1': '1/0' }
                    }
                }
            }
        }, async())
    }, function () {
        var reactor = {
            join: cadence(function (async) {
                conference.ifNotReplaying(function (callback) {
                    throw new Error('should not be replaying')
                }, async())
            }),
            double: cadence(function (async, request) {
                return request * 2
            })
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
            constructor.join()
            constructor.receive('double')
            constructor.reduced('double', 'doubled')
        })
        requests = conference.spigot.requests.shifter()
        dispatcher = conference._dispatcher
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'join',
            body: {
                id: '3',
                replaying: true
            }
        }, async())
    }, function () {
        dispatcher.fromBasin({
            module: 'conference',
            method: 'entry',
            body: immigrate
        }, async())
        assert(requests.shift(), {
            module: 'conduit',
            to: 'conference',
            from: 'conference',
            cookie: '1',
            body: {
                module: 'conference',
                method: 'backlog',
                to: '1',
                from: '3/0',
                body: null
            }
        }, 'out of band backlog request')
        conference.spigot.responses.push({
            module: 'conduit',
            to: 'conference',
            from: 'conference',
            cookie: '1',
            body: {
                'double[1/0](1)': {
                    key: 'double[1/0](1)',
                    request: 1,
                    method: 'double',
                    responses: { '1/0': 4 }
                }
            }
        })
    }, function () {
        assert(requests.shift(), {
            module: 'conference',
            method: 'reduce',
            from: '3/0',
            key: 'double[1/0](1)',
            body: 2
        }, 'broadcast backlog')
        assert(requests.shift(), {
            module: 'conference',
            method: 'naturalized',
            body: null
        }, 'naturalized')
    }, function () {
        dispatcher.fromBasin({
            module: 'colleague',
            method: 'entry',
            body: {
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
            }
        }, async())
    }, function () {
        assert(conference.government.promise, '4/0', 'government without immigration or exile')
    })
}
