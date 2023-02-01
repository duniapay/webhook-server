const { Kafka } = require('kafkajs')
const axios = require('axios')
const { createHmac } = import('crypto');
const path = require('path')
const PORT = process.env.PORT || 5000
const FROM_BEGINNING = process.env.NODE_ENV !== 'production' ? true : false



  async function main() {
    let kafka;

    if(process.env.NODE_ENV !== 'production') {
      kafka = new Kafka({
        clientId: 'fiatconnect',
        brokers: ['localhost:9092'],
      })
    
      const producer = kafka.producer()
      await producer
        .connect()
        .then(() => {
          logger.info('Broker has been initialized')
        })
        .catch((e) => {
          logger.error(e)
        })
    } else {
      const broker = process.env.KAFKA_URL
      // convert this string to array
      const brokersList = broker.split(',')

      kafka = new Kafka({
        clientId: 'fiatconnect',
        brokers: brokersList,
        ssl: {
          rejectUnauthorized: false,
          ca: process.env.KAFKA_TRUSTED_CERT,
          key: process.env.KAFKA_CLIENT_CERT_KEY,
          cert: process.env.KAFKA_CLIENT_CERT,
        },
      })
    
      const producer = kafka.producer()
      await producer
        .connect()
        .then(() => {
          logger.info('Broker has been initialized')
        })
        .catch((e) => {
          logger.error(e)
        })
      return producer
    }
    

    const consumer = kafka.consumer({ groupId: 'webhook-server' })
    await consumer.connect()
    await consumer.subscribe({ topics: ['kyc' , 'transfer'], fromBeginning: FROM_BEGINNING })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          value: message.value.toString(),
          topic: topic,
          
        })
        let response = await sendEvent(message.value.toString())

        // Retry logic for failed requests
        let retry_count = 0
        const retry_intervals = [3, 66, 731, 4098]
        // while response.status_code != 200 and retry_count < 4
       while (response.status !==200 && retry_count<4) {
          retry_count += 1
          await new Promise((resolve) => setTimeout(resolve, retry_intervals[retry_count-1]))
          response = await sendEvent(message.value.toString())
          return response;
       }
      },
    })
    return consumer;
  }


  async function sendEvent(e) {
        const providerId = 'dunia-payment'
    const baseUrl =
      'https://liquidity-dot-celo-mobile-alfajores.appspot.com/fiatconnect/webhook/' +
      providerId

    /**
     * Your API call to webhookUrl with
     * your defined body about status of event
     */
    const hmac = createHmac('sha1', secret)

    const webhookDigest = hmac.update(JSON.stringify(e)).digest('hex')
    const t = `t=` + Date.now()
    const s = `v1=` + webhookDigest

    await axios.post(
      baseUrl,
      { body: JSON.stringify(e) },
      {
        headers: {
          'Content-Type': 'application/json',
          'fiatconnect-signature': t + ',' + s,
        },
      },
    )
  }


  main().catch((err) => {
    // eslint-disable-next-line no-console
    console.error(err)
    process.exit(1)
  })