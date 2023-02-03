const axios = require('axios')
const { createHmac } = import('crypto');
const RedisSMQ = require("rsmq");

const path = require('path')
const PORT = process.env.PORT || 5000
const FROM_BEGINNING = process.env.NODE_ENV !== 'production' ? true : false
/// Load private keys from environment variable
const REDIS_HOST =
  process.env.NODE_ENV === 'localhost'
    ? 'redis://localhost:6379'
    : process.env.REDISCLOUD_URL
    
    const raw_url = new URL(REDIS_HOST)
    const REDIS_HOSTNAME = raw_url.hostname
    const REDIS_PORT = raw_url.port
    const REDIS_PASSWORD = raw_url.password
    const NAMESPACE = 'rsmq'
    
    const rsmq = new RedisSMQ({
      host: REDIS_HOSTNAME,
      port: REDIS_PORT,
      ns: NAMESPACE,
      password: REDIS_PASSWORD,
    })

  async function main() {
    await listenFromQueue('kyc');
    await listenFromQueue('transfer');
}
    

  
  
async function listenFromQueue(queueName) {
  rsmq.receiveMessage({ qname: queueName }, async function (err, resp) {
    if (err) {
      console.error(err)
      return
    }
    if (resp.id) {
      console.log("Message received.", resp)
      await sendEvent(resp.message)
    } else {
      console.log("No messages for me...")
    }
  });
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