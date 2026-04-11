import type { Container } from '../../config/container.js';
import { DatasetId } from '../../../domain/value-objects/DatasetId.js';
import { EventsController } from '../../http/controllers/EventsController.js';

export class DatasetStatusSyncListener {
  constructor(private readonly container: Container) {}

  start(): void {
    const qe = this.container.queueEvents;

    // When the worker picks up a job, notify clients so the table shows
    // "Procesando" immediately instead of waiting for the next poll cycle.
    qe.onJobActive(async (jobId: string) => {
      try {
        const bullmqQueue = (this.container.jobQueue as any).queue;
        const jobData = await bullmqQueue.getJob(jobId);
        const datasetIdStr = jobData?.data?.datasetId;
        if (!datasetIdStr) return;

        const dataset = await this.container.datasetRepository.findById(
          (await import('../../../domain/value-objects/DatasetId.js')).DatasetId.fromString(datasetIdStr)
        );
        if (!dataset) return;

        EventsController.emit(dataset.userId, 'dataset:status_changed', {
          datasetId: datasetIdStr,
          status: 'PROCESSING',
        });
      } catch {
        // Non-critical — polling will catch it
      }
    });

    qe.onJobCompleted(async (jobId: string, returnvalue: any) => {
      try {
        let result: any = null;
        try {
          result = typeof returnvalue === 'string' ? JSON.parse(returnvalue) : returnvalue;
        } catch (e) {
          console.error(`Failed to parse job return value for jobId ${jobId}: ${returnvalue}`);
          return;
        }

        const datasetIdStr = result?.datasetId;
        if (!datasetIdStr) {
          console.warn(`Job ${jobId} completed but returned no datasetId`);
          return;
        }

        const datasetId = DatasetId.fromString(datasetIdStr);
        const dataset = await this.container.datasetRepository.findById(datasetId);

        if (!dataset) {
          console.warn(`Dataset ${datasetIdStr} not found for completed job ${jobId}`);
          return;
        }

        const statistics = {
          rowsProcessed: result.rowsProcessed,
          columnsCount: result.columnsCount,
          preview: result.preview,
          transformationResults: result.transformationResults,
        };

        // Update metadata with schema if provided
        if (result.schema) {
          dataset.updateMetadata({ ...dataset.metadata, inferredSchema: result.schema });
        }

        // Update storageKey to point to the processed output file in the results bucket
        if (result.outputKey) {
          dataset.updateStorageKey(result.outputKey);
        }

        dataset.markReady(statistics);
        await this.container.datasetRepository.save(dataset);

        // Notify connected SSE clients for this user
        EventsController.emit(dataset.userId, 'dataset:status_changed', {
          datasetId: datasetIdStr,
          status: 'READY',
        });

        console.log(`Dataset ${datasetIdStr} successfully marked as READY`);
      } catch (error) {
        console.error(`Error processing job completion for jobId ${jobId}:`, error);
      }
    });

    qe.onJobFailed(async (jobId: string, failedReason: string) => {
      try {
        // We can't easily get the datasetId from the job without fetching the job itself.
        // Let's fetch the job payload from BullMQ to find the datasetId.
        const job = await this.container.jobQueue.getStatus(jobId);
        // Wait, getStatus only returns status!
        const bullmqQueue = (this.container.jobQueue as any).queue;
        const jobData = await bullmqQueue.getJob(jobId);

        if (!jobData || !jobData.data || !jobData.data.datasetId) {
          console.warn(`Job ${jobId} failed but could not fetch datasetId`);
          return;
        }

        const datasetIdStr = jobData.data.datasetId;
        const datasetId = DatasetId.fromString(datasetIdStr);
        const dataset = await this.container.datasetRepository.findById(datasetId);

        if (!dataset) return;

        dataset.markError();
        await this.container.datasetRepository.save(dataset);

        // Notify connected SSE clients for this user
        EventsController.emit(dataset.userId, 'dataset:status_changed', {
          datasetId: datasetIdStr,
          status: 'ERROR',
        });

        console.log(`Dataset ${datasetIdStr} marked as ERROR due to job failure`);
      } catch (error) {
        console.error(`Error processing job failure for jobId ${jobId}:`, error);
      }
    });

    console.log('DatasetStatusSyncListener started - Listening to BullMQ events');
  }

  private async notifyN8nForAiSuggestions(datasetId: string): Promise<void> {
    const webhookUrl = process.env['N8N_SUGGESTIONS_WEBHOOK_URL'];
    if (!webhookUrl) {
      console.warn('N8N_SUGGESTIONS_WEBHOOK_URL not set — skipping AI suggestion trigger');
      return;
    }

    const anomalies = await this.container.anomalyRepository.findByDatasetId(datasetId);
    const limited = anomalies.slice(0, 20);

    if (limited.length === 0) {
      console.log(`Dataset ${datasetId} has no anomalies — skipping n8n notification`);
      return;
    }

    const payload = {
      datasetId,
      anomalies: limited.map((a) => ({
        id: a.id,
        type: a.type,
        column: a.column,
        row: a.row,
        description: a.description,
        originalValue: a.originalValue,
        suggestedValue: a.suggestedValue,
      })),
    };

    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      throw new Error(`n8n webhook responded with status ${String(response.status)}`);
    }

    console.log(`n8n notified for AI suggestions on dataset ${datasetId} (${String(limited.length)} anomalies)`);
  }
}
