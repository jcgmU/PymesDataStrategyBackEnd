import type { AnomalyRepository } from '../../domain/ports/repositories/AnomalyRepository.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import type { AnomalyType, DecisionAction } from '../../domain/entities/Anomaly.js';
import { DatasetId } from '../../domain/value-objects/DatasetId.js';
import { NotFoundError } from '../../domain/errors/NotFoundError.js';

// ─── DTOs ────────────────────────────────────────────────────────────────────

export interface GenerateReportInput {
  datasetId: string;
  userId: string;
}

export interface ReportStats {
  totalAnomalies: number;
  approved: number;
  corrected: number;
  discarded: number;
  pending: number;
  byType: Record<string, { count: number; action: string }>;
  datasetName: string;
}

export interface GenerateReportOutput {
  stats: ReportStats;
  narrative: string;
  datasetName: string;
}

// ─── Use Case ────────────────────────────────────────────────────────────────

/**
 * Use case for generating a clean-data report with AI narrative for a dataset.
 *
 * Flow:
 * 1. Verify dataset exists and belongs to the requesting user
 * 2. Fetch all anomalies (including their decisions)
 * 3. Calculate stats (totals + by anomaly type)
 * 4. Call Gemini to generate a Spanish executive narrative
 * 5. Return { stats, narrative, datasetName }
 */
export class GenerateReportUseCase {
  private readonly geminiApiKey: string | undefined;
  private readonly geminiModel: string;

  constructor(
    private readonly anomalyRepository: AnomalyRepository,
    private readonly datasetRepository: DatasetRepository
  ) {
    this.geminiApiKey = process.env['GEMINI_API_KEY'];
    this.geminiModel = process.env['GEMINI_MODEL'] ?? 'gemini-2.0-flash-exp';
  }

  async execute(input: GenerateReportInput): Promise<GenerateReportOutput> {
    // 1. Verify dataset exists and belongs to the user
    const datasetId = DatasetId.fromString(input.datasetId);
    const dataset = await this.datasetRepository.findById(datasetId);

    if (!dataset) {
      throw new NotFoundError('Dataset', input.datasetId);
    }

    if (dataset.userId !== input.userId) {
      // Treat as not found to avoid leaking existence to other users
      throw new NotFoundError('Dataset', input.datasetId);
    }

    // 2. Fetch anomalies with their decisions
    const anomalies = await this.anomalyRepository.findByDatasetId(input.datasetId);

    // 3. Calculate stats
    let approved = 0;
    let corrected = 0;
    let discarded = 0;
    let pending = 0;

    const byType: Record<AnomalyType, { count: number; actions: DecisionAction[] }> = {} as Record<
      AnomalyType,
      { count: number; actions: DecisionAction[] }
    >;

    for (const anomaly of anomalies) {
      const type = anomaly.type;

      if (!byType[type]) {
        byType[type] = { count: 0, actions: [] };
      }
      byType[type].count++;

      const action = anomaly.decision?.action ?? null;
      if (action === 'APPROVED') {
        approved++;
        byType[type].actions.push('APPROVED');
      } else if (action === 'CORRECTED') {
        corrected++;
        byType[type].actions.push('CORRECTED');
      } else if (action === 'DISCARDED') {
        discarded++;
        byType[type].actions.push('DISCARDED');
      } else {
        pending++;
      }
    }

    // Summarize byType — pick the most common action per type (or 'PENDING')
    const byTypeSummary: Record<string, { count: number; action: string }> = {};
    for (const [type, data] of Object.entries(byType)) {
      const actionCounts: Record<string, number> = {};
      for (const a of data.actions) {
        actionCounts[a] = (actionCounts[a] ?? 0) + 1;
      }
      let dominantAction = 'PENDING';
      let maxCount = 0;
      for (const [a, cnt] of Object.entries(actionCounts)) {
        if (cnt > maxCount) {
          maxCount = cnt;
          dominantAction = a;
        }
      }
      byTypeSummary[type] = { count: data.count, action: dominantAction };
    }

    const stats: ReportStats = {
      totalAnomalies: anomalies.length,
      approved,
      corrected,
      discarded,
      pending,
      byType: byTypeSummary,
      datasetName: dataset.name,
    };

    // 4. Generate narrative
    const narrative = await this.generateNarrative(stats);

    return { stats, narrative, datasetName: dataset.name };
  }

  // ─── Private helpers ─────────────────────────────────────────────────────

  private async generateNarrative(stats: ReportStats): Promise<string> {
    if (!this.geminiApiKey) {
      return 'Informe generado sin IA — configura GEMINI_API_KEY para habilitar la narrativa automática.';
    }

    const TYPE_LABELS: Record<string, string> = {
      MISSING_VALUE: 'Valores faltantes', DUPLICATE: 'Duplicados',
      FORMAT_INVALID: 'Formato inválido', FORMAT_ERROR: 'Error de formato',
      OUTLIER: 'Valores atípicos', INCONSISTENT: 'Inconsistentes',
      WHITESPACE_ONLY: 'Celdas vacías (espacios)', SUSPICIOUS_PLACEHOLDER: 'Marcadores de posición',
      LEADING_TRAILING_WHITESPACE: 'Espacios al inicio/fin', DATE_LOGICAL: 'Fechas incoherentes',
      NUMERIC_ROUND_NUMBER: 'Números redondos sospechosos', LOW_VARIANCE: 'Baja variación',
      OUTLIER_IQR: 'Outliers IQR', SEQUENCE_GAP: 'Huecos en secuencia',
      CROSS_FIELD_SWAP: 'Dato en campo incorrecto',
    };
    const ACTION_LABELS: Record<string, string> = {
      APPROVED: 'aprobadas sin cambio', CORRECTED: 'corregidas',
      DISCARDED: 'eliminadas', PENDING: 'pendientes',
    };
    const typeLines = Object.entries(stats.byType)
      .map(([type, data]) =>
        `  - ${TYPE_LABELS[type] ?? type}: ${data.count} columna(s) → ${ACTION_LABELS[data.action] ?? data.action}`
      )
      .join('\n');

    const prompt = `Eres un analista de datos senior. Redacta un informe ejecutivo en español sobre el proceso de limpieza del dataset "${stats.datasetName}".

Resultados del proceso:
- Total de anomalías detectadas: ${stats.totalAnomalies}
- Aprobadas sin cambio: ${stats.approved}
- Corregidas manualmente: ${stats.corrected}
- Filas eliminadas: ${stats.discarded}
- Pendientes sin resolver: ${stats.pending}

Detalle por tipo de anomalía:
${typeLines}

INSTRUCCIONES IMPORTANTES:
- Escribe exactamente 3 párrafos separados por una línea en blanco.
- Usa SOLO texto plano. Sin asteriscos, sin markdown, sin negritas, sin guiones como viñetas.
- Primer párrafo: resumen general del volumen y tipos de anomalías encontradas.
- Segundo párrafo: describe las correcciones más relevantes aplicadas y por qué mejoran la calidad.
- Tercer párrafo: impacto esperado en la calidad del dataset tras la limpieza.`;

    try {
      return await this.callGemini(prompt);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new GeminiUnavailableError(`Gemini narrative generation failed: ${message}`);
    }
  }

  private async callGemini(prompt: string): Promise<string> {
    // Dynamic import — avoids hard failure if package is somehow not present
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let genaiModule: any;
    try {
      genaiModule = await import('@google/generative-ai');
    } catch {
      throw new Error('Package @google/generative-ai not installed');
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    const genAI = new genaiModule.GoogleGenerativeAI(this.geminiApiKey);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    const model = genAI.getGenerativeModel({
      model: this.geminiModel,
      generationConfig: {
        temperature: 0.4,
        maxOutputTokens: 1024,
      },
    });

    const timeoutMs = 20_000;
    const timeoutPromise = new Promise<never>((_, reject) => {
      const id = setTimeout(() => {
        clearTimeout(id);
        reject(new Error('timeout: Gemini did not respond within 20 seconds'));
      }, timeoutMs);
    });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    const resultPromise = model.generateContent(prompt);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const result = await Promise.race([resultPromise, timeoutPromise]);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    return (result.response.text() as string).trim();
  }
}

// ─── Custom error ─────────────────────────────────────────────────────────────

export class GeminiUnavailableError extends Error {
  readonly code = 'GEMINI_UNAVAILABLE';

  constructor(message: string) {
    super(message);
    this.name = 'GeminiUnavailableError';
  }
}
