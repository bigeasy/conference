[![Actions Status](https://github.com/bigeasy/conference/workflows/Node%20CI/badge.svg)](https://github.com/bigeasy/conference/actions)
[![codecov](https://codecov.io/gh/bigeasy/conference/branch/master/graph/badge.svg)](https://codecov.io/gh/bigeasy/conference)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Reduce messages from an atomic log.

| What          | Where                                             |
| --- | --- |
| Discussion    | https://github.com/bigeasy/conference/issues/1    |
| Documentation | https://bigeasy.github.io/conference              |
| Source        | https://github.com/bigeasy/conference             |
| Issues        | https://github.com/bigeasy/conference/issues      |
| CI            | https://travis-ci.org/bigeasy/conference          |
| Coverage:     | https://codecov.io/gh/bigeasy/conference          |
| License:      | MIT                                               |

Ascension installs from NPM.

```
//{ "mode": "text" }
npm install conference
```

## Living `README.md`

This `README.md` is also a unit test using the
[Proof](https://github.com/bigeasy/proof) unit test framework. We'll use the
Proof `okay` function to assert out statements in the readme. A Proof unit test
generally looks like this.

```javascript
//{ "code": { "tests": 7 }, "text": { "tests": 4  } }
require('proof')(%(tests)d, okay => {
    //{ "include": "test", "mode": "code" }
    //{ "include": "proof" }
})
```

```javascript
//{ "name": "proof", "mode": "text" }
okay('always okay')
okay(true, 'okay if true')
okay(1, 1, 'okay if equal')
okay({ value: 1 }, { value: 1 }, 'okay if deep strict equal')
```

You can run this unit test yourself to see the output from the various
code sections of the readme.

```text
//{ "mode": "text" }
git clone git@github.com:bigeasy/conference.git
cd conference
npm install --no-package-lock --no-save
node test/readme.t.js
```

## Overview

Require.

```javascript
//{ "name": "test", "code": { "path": "'..'" }, "text": { "path": "'conference'" } }
const Conference = require(%(path)s)
```

Construct.

```javascript
//{ "name": "test" }
const conference = new Conference
```
Arrive.

```javascript
//{ "name": "test" }
conference.arrive('1/0')
```

Map.

```javascript
//{ "name": "test" }
conference.map('x', { call: 1 })
```
Reduce.

```javascript
//{ "name": "test", "unblock": true }
{
    const reduction = conference.reduce('x', '1/0', { response: 1 })
    okay(reduction, [{
        key: 'x',
        map: { call: 1 },
        reduce: { '1/0': { response: 1 } }
    }], 'reduced')
}
```

More meaningful if you can imagine multiple participants. You track each
arrival.

```javascript
//{ "name": "test" }
conference.arrive('2/0')
conference.arrive('3/0')
```

Map is called once.

```javascript
//{ "name": "test" }
conference.map('x', { call: 1 })
```

Reduce is called once for each member.

```javascript
//{ "name": "test" }
okay(conference.reduce('x', '1/0', { reduce: 1 }), [], 'only one of three responses')
okay(conference.reduce('x', '3/0', { reduce: 1 }), [], 'two of three responses')
const reduction = conference.reduce('x', '2/0', { reduce: 1 })
okay(reduction, [{
    key: 'x',
    map: { call: 1 },
    reduce: {
        '1/0': { reduce: 1 },
        '2/0': { reduce: 1 },
        '3/0': { reduce: 1 }
    }
}], 'all responses')
```

Convert ot array.

```javascript
//{ "name": "test" }
okay(Conference.toArray(reduction[0]), [{
    promise: '1/0', value: { reduce: 1 },
}, {
    promise: '3/0', value: { reduce: 1 },
}, {
    promise: '2/0', value: { reduce: 1 }
}], 'to array')
```

Makes it easier to perform reduce-like actions with JavaScript array functions.

```javascript
//{ "name": "test" }
const sum = Conference.toArray(reduction[0])
    .map(reduced => reduced.value.reduce)
    .reduce((sum, value) => sum + value, 0)
okay(sum, 3, 'summed')
```

```javascript
//{ "name": "test" }
conference.map('x', { call: 1 })
conference.map('y', { call: 2 })
```

```javascript
//{ "name": "test" }
conference.reduce('x', '1/0', { response: 1 })
conference.reduce('x', '3/0', { response: 1 })
conference.reduce('y', '1/0', { response: 2 })
conference.reduce('y', '3/0', { response: 2 })
```

```javascript
//{ "name": "test", "unblock": true }
{
    const reduction = conference.depart('2/0')
    okay(reduction, [{
        key: 'x',
        map: { call: 1 },
        reduce: { '1/0': { response: 1 }, '3/0': { response: 1 } }
    }, {
        key: 'y',
        map: { call: 2 },
        reduce: { '1/0': { response: 2 }, '3/0': { response: 2 } }
    }], 'reductions upon depart')
}
```
