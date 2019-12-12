const fs = require('fs')
const path = require('path')

function getFiles(dir) {
    const dirents = fs.readdirSync(dir, { withFileTypes: true })
    const files = dirents.map(dirent => {
        const res = path.resolve(dir, dirent.name)
        return dirent.isDirectory() ? getFiles(res) : res
    })
    return files.flat()
}

function file2Route(dirname, file, folder, func) {
    let ending = ''
    if (func !== 'default' && func) {
        ending = `.${func}`
    }
    return (
        file
            .replace(`${dirname}/${folder}/`, '')
            .replace(/\//g, '.')
            .replace(/\..{1,3}$/, '') + ending
    )
}

function* getPathFunctions(path, dirname) {
    const files = getFiles(path)
    const functionsMap = {}
    for (let file of files) {
        if (file.match(/\.js$/)) {
            const required = require(file)
            const functions = Object.keys(required)
            if (typeof required === 'function') {
                const route = file2Route(dirname, file, path)
                yield { route, func: required, file }
            }
            for (let func of functions) {
                if (typeof required[func] === 'function') {
                    const route = file2Route(dirname, file, path, func)
                    yield { route, func: required[func], file }
                }
            }
        }
    }
    return functionsMap
}

const EventEmitter = require('events')
const uuid = require('uuid/v1')
const rpcEmitter = new EventEmitter()
function wrapFunction(func, ch, q, exchange, rpcExchange, resultExchange) {
    ch.assertQueue('', { exclusive: true }).then((replyQ) => {
        ch.consume(replyQ.queue, msg => {
            const correlationId = msg.properties.correlationId
            rpcEmitter.emit(correlationId, msg)
        })
    }).catch(console.error)

    const self = {
        ch,
        async fire(topic, payload) {
            return ch.publish(
                'events',
                topic,
                Buffer.from(JSON.stringify(payload)),
                {
                    contentType: 'application/json'
                }
            )
        },
        rpcJob(topic, payload, ex = null, timeout = 10000) {
            return new Promise(function(resolve, reject) {
                const timeoutId = setTimeout(() => {
                    reject(`Timed out after ${timeout} ms.`)
                }, timeout)
                const correlationId = uuid()

                ch.publish(ex || rpcExchange, topic, Buffer.from(JSON.stringify(payload)), {
                    contentType: 'application/json',
                    correlationId,
                    replyTo: q.queue
                })
                rpcEmitter.once(correlationId, msg => {
                    clearTimeout(timeoutId)
                    resolve(msg)
                })
            })
        }
    }
    return async function(msg) {
        try {
            msg.content = JSON.parse(msg.content.toString())
            let result
            try {
                result = await func.call(self, msg)
            } catch (error) {
                console.error(error)
                ch.publish(
                    resultExchange,
                    `${msg.fields.routingKey}.error`,
                    Buffer.from(
                        JSON.stringify(error, Object.getOwnPropertyNames(error))
                    ),
                    { contentType: 'application/json' }
                )
            }
            if (msg.properties.replyTo) {
                ch.sendToQueue(
                    msg.properties.replyTo,
                    Buffer.from(JSON.stringify(result)),
                    { correlationId: msg.properties.correlationId }
                )
            } else {
                if (result) {
                    ch.publish(
                        resultExchange,
                        `${msg.fields.routingKey}.result`,
                        Buffer.from(JSON.stringify(result)),
                        { contentType: 'application/json' }
                    )
                }
            }
            ch.ack(msg)
        } catch (error) {
            console.error(error)
            ch.nack(msg)
        }
    }
}
exports.wrapFunction = wrapFunction
exports.getPathFunctions = getPathFunctions
