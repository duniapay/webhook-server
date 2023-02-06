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

const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;
const WEBHOOK_URL =
  "https://liquidity-dot-celo-mobile-alfajores.appspot.com/fiatconnect/webhook/";

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
      const results = await sendWebhookEvent(
        resp.message.eventType,
        resp.message.provider,
        resp.message.eventId,
        resp.message.address,
        resp.message.payload
      );
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

const sendWebhookEvent = async (
  eventType,
  provider,
  eventId,
  address,
  payload
) => {
  const timestamp = Math.floor(Date.now() / 1000);
  const message = `${timestamp}.${JSON.stringify(payload)}`;
  const signature = crypto
    .createHmac("sha256", WEBHOOK_SECRET)
    .update(message)
    .digest("hex");
  const headers = {
    "FiatConnect-Signature": `t=${timestamp},v1=${signature}`,
  };
  const body = {
    eventType,
    provider,
    eventId,
    timestamp,
    address,
    payload,
  };
  try {
    const response = await axios.post(WEBHOOK_URL, body, { headers });
    if (response.status === 200) {
      console.log("Webhook event sent successfully");
      return;
    }
  } catch (err) {
    console.error("Failed to send webhook event:", err);
  }
};
main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
