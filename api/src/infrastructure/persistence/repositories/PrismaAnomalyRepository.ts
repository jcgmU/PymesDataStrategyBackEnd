import { PrismaClient, Prisma, type Anomaly as PrismaAnomaly, type Decision as PrismaDecision } from '@prisma/client';
import type { AnomalyRepository } from '../../../domain/ports/repositories/AnomalyRepository.js';
import {
  Anomaly,
  type AnomalyType,
  type AnomalyStatus,
  type AnomalyDecision,
  type DecisionAction,
} from '../../../domain/entities/Anomaly.js';

type PrismaAnomalyWithDecision = PrismaAnomaly & {
  decision: PrismaDecision | null;
};

/**
 * Prisma implementation of AnomalyRepository.
 */
export class PrismaAnomalyRepository implements AnomalyRepository {
  constructor(private readonly prisma: PrismaClient) {}

  async findByDatasetId(datasetId: string): Promise<Anomaly[]> {
    const records = await this.prisma.anomaly.findMany({
      where: { datasetId },
      include: { decision: true },
      orderBy: { createdAt: 'asc' },
    });

    return records.map((record) => this.toDomain(record));
  }

  async findById(id: string): Promise<Anomaly | null> {
    const record = await this.prisma.anomaly.findUnique({
      where: { id },
      include: { decision: true },
    });

    if (!record) return null;

    return this.toDomain(record);
  }

  async save(anomaly: Anomaly): Promise<void> {
    await this.prisma.$transaction(async (tx) => {
      // Upsert the anomaly
      await tx.anomaly.upsert({
        where: { id: anomaly.id },
        create: {
          id: anomaly.id,
          datasetId: anomaly.datasetId,
          column: anomaly.column,
          row: anomaly.row,
          type: anomaly.type,
          description: anomaly.description,
          originalValue: anomaly.originalValue,
          suggestedValue: anomaly.suggestedValue,
          aiSuggestion: anomaly.aiSuggestion,
          status: anomaly.status,
          createdAt: anomaly.createdAt,
          updatedAt: anomaly.updatedAt,
        },
        update: {
          status: anomaly.status,
          aiSuggestion: anomaly.aiSuggestion,
          updatedAt: anomaly.updatedAt,
        },
      });

      // Upsert decision if present
      if (anomaly.decision !== null) {
        const d = anomaly.decision;
        await tx.decision.upsert({
          where: { anomalyId: anomaly.id },
          create: {
            id: d.id,
            anomalyId: d.anomalyId,
            action: d.action,
            correction: d.correction,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            correctionIr: (d.correctionIr !== null ? d.correctionIr : Prisma.DbNull) as any,
            irSource: d.irSource ?? null,
            irRawText: d.irRawText ?? null,
            userId: d.userId,
            createdAt: d.createdAt,
          },
          update: {
            action: d.action,
            correction: d.correction,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            correctionIr: (d.correctionIr !== null ? d.correctionIr : Prisma.DbNull) as any,
            irSource: d.irSource ?? null,
            irRawText: d.irRawText ?? null,
          },
        });
      }
    });
  }

  async saveMany(anomalies: Anomaly[]): Promise<void> {
    for (const anomaly of anomalies) {
      await this.save(anomaly);
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Mapping helpers
  // ─────────────────────────────────────────────────────────────

  private toDomain(record: PrismaAnomalyWithDecision): Anomaly {
    let decision: AnomalyDecision | null = null;

    if (record.decision !== null) {
      decision = {
        id: record.decision.id,
        anomalyId: record.decision.anomalyId,
        action: record.decision.action as DecisionAction,
        correction: record.decision.correction,
        correctionIr: (record.decision.correctionIr as Record<string, unknown> | null) ?? null,
        irSource: record.decision.irSource ?? null,
        irRawText: record.decision.irRawText ?? null,
        userId: record.decision.userId,
        createdAt: record.decision.createdAt,
      };
    }

    return Anomaly.reconstitute({
      id: record.id,
      datasetId: record.datasetId,
      column: record.column,
      row: record.row,
      type: record.type as AnomalyType,
      description: record.description,
      originalValue: record.originalValue,
      suggestedValue: record.suggestedValue,
      aiSuggestion: (record as any).aiSuggestion ?? null,
      status: record.status as AnomalyStatus,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
      decision,
    });
  }
}
