const assert = require('assert')

class Conference {
    constructor () {
        this.instances = []
        this.broadcasts = {}
    }

    static toArray (reduction) {
        const array = []
        for (const promise in reduction.reduce) {
            array.push({ promise: promise, value: reduction.reduce[promise] })
        }
        return array
    }

    snapshot () {
        return JSON.parse(JSON.stringify({
            instances: this.instances,
            broadcasts: this.broadcasts
        }))
    }

    join ({ instances, broadcasts }) {
        this.instances = instances
        this.broadcasts = broadcasts
    }

    arrive (promise) {
        this.instances.push(promise)
        for (const key in this.broadcasts) {
            const broadcast = this.broadcasts[key]
            broadcast.missing.push(promise)
        }
    }

    map (key, message) {
        this.broadcasts[key] = {
            key: key,
            missing: [],
            map: message,
            reduce: {}
        }
    }

    reduce (key, promise, message) {
        this.broadcasts[key].reduce[promise] = message
        return this._check(key)
    }

    _check (key) {
        const { missing, map, reduce } = this.broadcasts[key]
        const reductions = Object.keys(reduce).length
        if (reductions == this.instances.length - missing.length) {
            assert(reductions, this.instances.map(promise => !~missing.indexOf(promise)), 'bad join state')
            delete this.broadcasts[key]
            return [{ key, map, reduce }]
        }
        return []
    }

    depart (promise) {
        this.instances.splice(this.instances.indexOf(promise), 1)
        const reductions = []
        for (const key in this.broadcasts) {
            delete this.broadcasts[key].reduce[promise]
            reductions.push.apply(reductions, this._check(key))
        }
        return reductions
    }
}

module.exports = Conference
