// =============================================================================
// PYMES Data Strategy - API Gateway Entry Point
// obsidian://open?vault=proyecto&file=Backend-MOC
// =============================================================================

import 'dotenv/config';
import { getContainer } from './infrastructure/config/container.js';
import { createServer, startServer } from './infrastructure/http/server.js';
import { DatasetStatusSyncListener } from './infrastructure/messaging/bullmq/DatasetStatusSyncListener.js';

async function main(): Promise<void> {
  console.log('PYMES API Gateway starting...');

  // Initialize container (validates environment)
  const container = getContainer();
  const config = container.config;

  console.log(`Environment: ${config.NODE_ENV}`);
  console.log(`Log level: ${config.LOG_LEVEL}`);

  // Start BullMQ global listener for job completion
  const jobListener = new DatasetStatusSyncListener(container);
  jobListener.start();

  // Create and start server
  const app = createServer(container);
  await startServer(app, config.PORT);

  // Graceful shutdown
  const shutdown = async (signal: string): Promise<void> => {
    console.log(`\n${signal} received, shutting down gracefully...`);
    await container.shutdown();
    console.log('Shutdown complete');
    process.exit(0);
  };

  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err: unknown) => {
  console.error('Failed to start API Gateway:', err);
  process.exit(1);
});
