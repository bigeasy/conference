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

    reduce (promise, key, message) {
        this.broadcasts[key].reduce[promise] = message
        return this._check(key)
    }

    _check (key) {
        const broadcast = this.broadcasts[key]
        const reductions = Object.keys(broadcast.reduce).length
        if (reductions == this.instances.length - broadcast.missing.length) {
            assert(reductions, this.instances.map(promise => !~broadcast.missing.indexOf(promise)), 'bad join state')
            delete this.broadcasts[key]
            return broadcast
        }
        return null
    }

    depart (promise) {
        this.instances.splice(this.instances.indexOf(promise), 1)
        const reductions = []
        for (const key in this.broadcasts) {
            delete this.broadcasts[key].reduce[promise]
            const reduction = this._check(key)
            if (reduction != null) {
                reductions.push(reduction)
            }
        }
        return reductions
    }
}

module.exports = Conference
