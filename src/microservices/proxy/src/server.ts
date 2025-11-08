import express from 'express';
import helmet from 'helmet';
import morgan from 'morgan';
import httpProxy from 'http-proxy';
import { IncomingMessage, ServerResponse } from 'http';

type ServiceKey = 'monolith' | 'movies' | 'events';

const port = parseInt(process.env.PORT ?? '8000', 10);
const upstreamTimeout = parseInt(process.env.UPSTREAM_TIMEOUT_MS ?? '10000', 10);

const serviceMap: Record<ServiceKey, string> = {
  monolith: normalizeUrl(process.env.MONOLITH_URL ?? 'http://localhost:8080'),
  movies: normalizeUrl(process.env.MOVIES_SERVICE_URL ?? 'http://localhost:8081'),
  events: normalizeUrl(process.env.EVENTS_SERVICE_URL ?? 'http://localhost:8082'),
};

const proxy = httpProxy.createProxyServer({
  changeOrigin: true,
  ignorePath: false,
  xfwd: true,
});

proxy.on('error', (error, _req, res) => {
  const serverRes = res as ServerResponse;

  if (serverRes.headersSent) {
    serverRes.end();
    return;
  }

  const statusCode = (error as NodeJS.ErrnoException).code === 'ECONNREFUSED' ? 503 : 502;

  serverRes.writeHead(statusCode, { 'Content-Type': 'application/json' });
  serverRes.end(
    JSON.stringify({
      error: statusCode === 503 ? 'Upstream service unavailable' : 'Bad Gateway',
      details: error.message,
    }),
  );
});

const app = express();
app.disable('x-powered-by');

app.use(helmet());
app.use(
  morgan(':method :url :status :res[content-length] - :response-time ms', {
    skip: () => process.env.NODE_ENV === 'test',
  }),
);

app.get('/health', (_req, res) => {
  res.status(200).send('Strangler Fig Proxy is healthy');
});

app.use('/api/movies/health', (req, res) => {
  forwardRequest(req, res, 'movies');
});

app.use('/api/movies', (req, res) => {
  forwardRequest(req, res, pickMoviesTarget());
});

app.use('/api/events', (req, res) => {
  forwardRequest(req, res, 'events');
});

app.use('/api', (req, res) => {
  forwardRequest(req, res, 'monolith');
});

app.use((_req, res) => {
  res.status(404).json({ error: 'Not Found' });
});

const server = app.listen(port, () => {
  console.log(`Strangler Fig proxy listening on port ${port}`);
  console.log(`  Monolith upstream: ${serviceMap.monolith}`);
  console.log(`  Movies upstream:   ${serviceMap.movies}`);
  console.log(`  Events upstream:   ${serviceMap.events}`);
  console.log(
    `  Gradual migration: ${isGradualMigrationEnabled()} (${getMoviesMigrationPercent()}% → movies service)`,
  );
});

process.on('SIGTERM', shutdownGracefully);
process.on('SIGINT', shutdownGracefully);

function forwardRequest(
  req: express.Request,
  res: express.Response,
  service: ServiceKey,
): void {
  const target = serviceMap[service];
  const originalUrl = req.originalUrl;

  if ((process.env.PROXY_DEBUG ?? '').toLowerCase() === 'true') {
    console.log(`[proxy] ${req.method} ${originalUrl} -> ${service} (${target})`);
  }

  (req as express.Request & { url: string }).url = originalUrl;
  res.setHeader('x-strangler-target', service);

  proxy.web(req as unknown as IncomingMessage, res as unknown as ServerResponse, {
    target,
    proxyTimeout: upstreamTimeout,
    timeout: upstreamTimeout,
  });
}

function pickMoviesTarget(): ServiceKey {
  const percent = getMoviesMigrationPercent();

  if (!isGradualMigrationEnabled()) {
    return percent >= 100 ? 'movies' : 'monolith';
  }

  if (percent <= 0) {
    return 'monolith';
  }

  if (percent >= 100) {
    return 'movies';
  }

  return Math.random() * 100 < percent ? 'movies' : 'monolith';
}

function getMoviesMigrationPercent(): number {
  const raw = process.env.MOVIES_MIGRATION_PERCENT ?? '0';
  const parsed = Number(raw);

  if (Number.isNaN(parsed)) {
    console.warn(`[proxy] Invalid MOVIES_MIGRATION_PERCENT "${raw}", falling back to 0`);
    return 0;
  }

  return Math.min(100, Math.max(0, parsed));
}

function isGradualMigrationEnabled(): boolean {
  return (process.env.GRADUAL_MIGRATION ?? 'true').toLowerCase() === 'true';
}

function normalizeUrl(value: string): string {
  return value.replace(/\/+$/, '');
}

function shutdownGracefully(): void {
  console.log('Shutting down proxy...');
  server.close(() => {
    proxy.close();
    process.exit(0);
  });
}