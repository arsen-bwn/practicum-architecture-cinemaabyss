import express from 'express';
import helmet from 'helmet';
import morgan from 'morgan';
import { Kafka, Consumer, Producer } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

type EventPayload = Record<string, unknown>;

const port = parseInt(process.env.PORT ?? '8082', 10);
const kafkaBrokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);

const topics = {
  movie: process.env.KAFKA_TOPIC_MOVIE ?? 'movie-events',
  user: process.env.KAFKA_TOPIC_USER ?? 'user-events',
  payment: process.env.KAFKA_TOPIC_PAYMENT ?? 'payment-events',
};

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID ?? 'events-service',
  brokers: kafkaBrokers,
});

const producer: Producer = kafka.producer();
const consumer: Consumer = kafka.consumer({
  groupId: process.env.KAFKA_GROUP_ID ?? 'events-service-consumer',
});

let producerReady = false;
let consumerReady = false;

const app = express();
app.disable('x-powered-by');

app.use(express.json());
app.use(helmet());
app.use(
  morgan(':method :url :status :res[content-length] - :response-time ms', {
    skip: () => process.env.NODE_ENV === 'test',
  }),
);

app.get('/api/events/health', (_req, res) => {
  const ready = isReady();
  res.status(ready ? 200 : 503).json({ status: ready });
});

app.post('/api/events/movie', async (req, res) => {
  await handleEvent(req, res, topics.movie, 'movie');
});

app.post('/api/events/user', async (req, res) => {
  await handleEvent(req, res, topics.user, 'user');
});

app.post('/api/events/payment', async (req, res) => {
  await handleEvent(req, res, topics.payment, 'payment');
});

app.use((_req, res) => {
  res.status(404).json({ error: 'Not Found' });
});

function isReady(): boolean {
  return producerReady && consumerReady;
}

async function handleEvent(
  req: express.Request,
  res: express.Response,
  topic: string,
  type: string,
): Promise<void> {
  const payload = req.body as EventPayload;
  const eventId = uuidv4();

  try {
    await producer.send({
      topic,
      messages: [
        {
          key: eventId,
          value: JSON.stringify({
            id: eventId,
            type,
            timestamp: new Date().toISOString(),
            payload,
          }),
        },
      ],
    });

    res.status(201).json({ status: 'success', topic, id: eventId });
  } catch (error) {
    console.error(`[events-service] Failed to publish message`, error);
    res.status(500).json({ error: 'Failed to publish event' });
  }
}

async function start(): Promise<void> {
  console.log(`[events-service] Starting on port ${port}`);
  console.log(`[events-service] Kafka brokers: ${kafkaBrokers.join(', ')}`);

  await producer.connect();
  producerReady = true;
  console.log('[events-service] Producer connected');

  await consumer.connect();
  consumerReady = true;
  console.log('[events-service] Consumer connected');

  for (const topic of Object.values(topics)) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString();
      const value = message.value?.toString();

      console.log(
        `[events-service] Consumed topic=${topic} partition=${partition} offset=${message.offset} key=${key}`,
      );

      if (value) {
        try {
          const event = JSON.parse(value);
          console.log('[events-service] Event payload:', event);
        } catch (error) {
          console.warn('[events-service] Failed to parse message payload', error);
        }
      }
    },
  });

  app.listen(port, () => {
    console.log(`[events-service] HTTP API listening on port ${port}`);
  });
}

start().catch((error) => {
  console.error('[events-service] Failed to start service', error);
  process.exit(1);
});

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

async function shutdown(): Promise<void> {
  console.log('[events-service] Shutting down...');
  await consumer.disconnect();
  consumerReady = false;
  await producer.disconnect();
  producerReady = false;
  process.exit(0);
}