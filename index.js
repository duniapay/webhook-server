const axios = require("axios");
const RedisSMQ = require("rsmq");

const path = require("path");
const PORT = process.env.PORT || 5000;
const FROM_BEGINNING = process.env.NODE_ENV !== "production" ? true : false;
/// Load private keys from environment variable
const REDIS_HOST =
  process.env.NODE_ENV === "localhost"
    ? "redis://localhost:6379"
    : process.env.REDISCLOUD_URL;

const raw_url = new URL(REDIS_HOST);
const REDIS_HOSTNAME = raw_url.hostname;
const REDIS_PORT = raw_url.port;
const REDIS_PASSWORD = raw_url.password;
const NAMESPACE = "rsmq";

const rsmq = new RedisSMQ({
  host: REDIS_HOSTNAME,
  port: REDIS_PORT,
  ns: NAMESPACE,
  password: REDIS_PASSWORD,
});

async function main() {
  // check for new messages on a delay
  console.log("worker started");
  setInterval(async () => {
    console.log("Checking for jobs");
    await listenFromQueue("kyc");
    await listenFromQueue("transfer");
  }, 1000);
}

async function listenFromQueue(queueName) {
  rsmq.receiveMessage({ qname: queueName }, async (err, resp) => {
    if (err) {
      return;
    }
    if (resp.id) {
      console.log(`Event received ${resp.message}`);
      // do lots of processing here
      const results = await sendEvent(resp.message);
      console.log(results);
      // when we are done we can delete the message from the queue
      if (results === true) {
        rsmq.deleteMessage({ qname: queueName, id: resp.id }, (err) => {
          if (err) {
            return;
          }
          console.log("deleted message with id", resp.id);
        });
      }
    } else {
      console.log("no message in queue");
    }
  });
}

async function sendEvent(e) {
  const providerId = "dunia-payment";
  const secret = "heu";
  const baseUrl =
    "https://liquidity-dot-celo-mobile-alfajores.appspot.com/fiatconnect/webhook/" +
    providerId;

  /**
   * Your API call to webhookUrl with
   * your defined body about status of event
   */
  const hmac = "";

  const webhookDigest = JSON.stringify(e);
  const t = `t=` + Date.now();
  const s = `v1=` + webhookDigest;

  try {
    const resp = await axios.post(
      baseUrl,
      { body: JSON.stringify(e) },
      {
        headers: {
          "Content-Type": "application/json",
          "fiatconnect-signature": t + "," + s,
        },
      }
    );
    return true;
  } catch (error) {
    // console.error(error)
    return false;
  }
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
