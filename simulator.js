var cadence = require('cadence')

function Colleague (conference) {
    this._kibitzer = null

    this.read = new Procession
    this.write = new Procession
//    this.read.pump(function (envelope) { console.log('READ', envelope) })
//    this.write.pump(function (envelope) { console.log('WRITE', envelope) })

    this._requester = new Requester('colleague', this.read, this.write)
//    this._requester.read.pump(function (envelope) { console.log('REQUESTER READ', envelope) })
    var responder = new Responder(this, 'colleague', this._requester.read, this._requester.write)
    var server = new Server({ object: this, method: '_connect' }, 'outgoing', responder.read, responder.write)
    this._client = new Client('incoming', server.read, server.write)

    this._write = this._client.write

    server.read.pump(this, '_read')
}

function Simulator () {
}

Simulator.prototype.bootstrap = cadence(function (async, identifier, conference) {
    var colleague = this._colleagues[identifier] = new Colleague(conference)
    async(function () {
        colleague.getProperties(async())
    }, function (properties) {
        console.log(properties)
        throw new Error
    })
})

module.exports = Simulator
