const amqp = require('amqplib')
const { getPathFunctions, wrapFunction } = require('./utils')

module.exports = async function init(options = {}) {
    options = Object.assign(
        {
            exchange: 'events',
            resultExchange: 'events',
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
        restart()
    })
    console.log('Connected to RabbitMQ Server')

    const mainChannel = await conn.createChannel()
    mainChannel.assertExchange(options.exchange, 'topic', {
        durable: true
    })

    const ch = await conn.createChannel()
    ch.prefetch(options.prefetch)

    for (let { route, func, file } of getPathFunctions(
        options.scriptsPath,
        options.dirname
    )) {
        let funcOptions = { noAck: false }
        if (funcOptions.options) {
            funcOptions = Object.assign(funcOptions, func.options)
        }
        console.log(`Setting up '${route}' from '${file}'`, funcOptions)
        const q = await ch.assertQueue('', { exclusive: true })
        const replyQ = await ch.assertQueue('', { exclusive: true })

        await ch.bindQueue(q.queue, options.exchange, route)
        ch.consume(
            q.queue,
            wrapFunction(
                func,
                ch,
                q,
                replyQ,
                options.exchange,
                options.rpcExchange,
                options.resultExchange
            ),
            funcOptions
        )
    }
}

function restart() {
    setTimeout(function() {
        process.on('exit', function() {
            require('child_process').spawn(process.argv.shift(), process.argv, {
                cwd: process.cwd(),
                detached: true,
                stdio: 'inherit'
            })
        })
        process.exit()
    }, 3000)
}
