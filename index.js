const amqp = require('amqplib')
const { getPathFunctions, wrapFunction } = require('./utils')

module.exports = async function init(options = {}) {
    this.options = Object.assign(
        {
            exchange: 'events',
            rpcExchange: 'jobs',
            rabbitmqURL: '',
            scriptsPath: 'events',
            prefetch: 0
        },
        options
    )
    const conn = await amqp.connect(this.rabbitmqURL)
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
    mainChannel.assertExchange(this.exchange, 'topic', {
        durable: true
    })

    const ch = await conn.createChannel()
    ch.prefetch(this.prefetch)

    for (let { route, func, file } of getPathFunctions(scriptsPath)) {
        let options = { noAck: false }
        if (func.options) {
            options = Object.assign(options, func.options)
        }
        console.log(`Setting up '${route}' from '${file}'`, options)
        const q = await ch.assertQueue('', { exclusive: true })
        await ch.bindQueue(q.queue, this.exchange, route)
        ch.consume(
            q.queue,
            wrapFunction(func, ch, q, this.exchange, this.rpcExchange),
            options
        )
    }
}
