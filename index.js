const Queue = require('bull');
const axios = require('axios');
require('dotenv').config()

const webhookQueue = new Queue('webhook', process.env.REDIS_URL);

webhookQueue.process(async (job) => {
  const event = job.data;

  try {
    const response = await axios.post(process.env.WEBHOOK_SERVER_URL, event);

    if (response.status !== 200) {
      throw new Error(`Received status code ${response.status}`);
    }
  } catch (error) {
    throw new Error('Error sending webhook event');
  }
});

async function sendWebhookEvent(event) {
  await webhookQueue.add(event, {
    attempts: 4,
    backoff: {
      type: 'exponential',
      delay: 1000
    },
    removeOnComplete: true
  });
}
