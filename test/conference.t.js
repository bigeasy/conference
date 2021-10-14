require('proof')(7, okay => {
    const Conference = require('..')

    const conference = new Conference

    conference.arrive('1/0')

    conference.map('a', { value: 1 })

    {
        const colleague = new Conference
        const snapshot = conference.snapshot()
        okay(snapshot, {
            instances: [ '1/0' ],
            broadcasts: {
                a: {
                    key: 'a',
                    missing: [],
                    map: { value: 1 },
                    reduce: {}
                }
            }
        }, 'snapshot')
        colleague.join(snapshot)
        colleague.arrive('2/0')
        okay(colleague.reduce('a', '1/0', { value: 2 }), [{
            key: 'a',
            map: { value: 1 },
            reduce: { '1/0': { value: 2 } }
        }], 'reduced snapshot')
    }

    conference.arrive('2/0')

    okay(conference.reduce('a', '1/0', { value: 2 }), [{
        key: 'a',
        map: { value: 1 },
        reduce: { '1/0': { value: 2 } }
    }], 'reduced')

    conference.map('b', { value: 1 })

    okay(conference.reduce('b', '1/0', { value: 2 }), [], 'reduced none')
    const reduction = conference.reduce('b', '2/0', { value: 3 })
    okay(Conference.toArray(reduction[0]), [{
        promise: '1/0',
        value: { value: 2 }
    }, {
        promise: '2/0',
        value: { value: 3 }
    }], 'to array')
    okay(reduction, [{
        key: 'b',
        map: { value: 1 },
        reduce: { '1/0': { value: 2 }, '2/0': { value: 3 } }
    }], 'reduce many')

    conference.map('d', { value: 1 })

    conference.map('c', { value: 1 })
    conference.reduce('c', '1/0', { value: 2 })
    okay(conference.depart('2/0'), [{
        key: 'c',
        map: { value: 1 },
        reduce: { '1/0': { value: 2 } }
    }], 'reduce depart')

    conference.reduce('d', '1/0', { value: 2 })
})
