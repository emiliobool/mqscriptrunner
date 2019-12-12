const amqp = require('amqplib')
const { getPathFunctions, wrapFunction } = require('./utils')

module.exports = async function init(options = {}) {
    options = Object.assign(
        {
            exchange: 'events',
            rpcExchange: 'jobs',
            rabbitmqURL: '',
            scriptsPath: 'events',
            prefetch: 0,
            dirname: __dirname
        },
        options
    )
    const conn = await amqp.connect(options.rabbitmqURL)
    conn.on('error', function(err) {
        if (err.message !== 'Connection closing') {
            console.error('[AMQP] conn error', err.message)
        }
    })
    conn.on('close', function() {
        console.error('[AMQP] Disconnected')
        setTimeout(() => {
            process.exit(2)
        }, 1000)
    })
    console.log('Connected to RabbitMQ Server')

    const mainChannel = await conn.createChannel()
    mainChannel.assertExchange(options.exchange, 'topic', {
        durable: true
    })

    const ch = await conn.createChannel()
    ch.prefetch(options.prefetch)

    for (let { route, func, file } of getPathFunctions(options.scriptsPath, options.dirname)) {
        let funcOptions = { noAck: false }
        if (funcOptions.options) {
            funcOptions = Object.assign(funcOptions, func.options)
        }
        console.log(`Setting up '${route}' from '${file}'`, funcOptions)
        const q = await ch.assertQueue('', { exclusive: true })
        await ch.bindQueue(q.queue, options.exchange, route)
        ch.consume(
            q.queue,
            wrapFunction(func, ch, q, options.exchange, options.rpcExchange),
            funcOptions
        )
    }
}
