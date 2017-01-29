Conference

```javascript
var Conference = require('conference')
var Colleague = require('colleague')

var reactor = {
    join: function (conference, colleague, callback) {
    },
    immigrate: function (conference, id, callback) {
    },
    exile: function (conference, id, callback) {
    }
}

var conference = new Conference(reactor, function (define) {
    define.join()
    define.immigrate()
    define.exile()
}))

var Colleague = new Colleague(process)

colleague.spigot.pump(new Spigot.Responder(conference.responder))

colleague.listen(functionn (error) {
    if (error) throw error
})
```

Some thoughts on construction above. Do not have a good idea of how to create an
example of usage.
