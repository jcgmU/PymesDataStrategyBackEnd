import type { AnomalyRepository } from '../../domain/ports/repositories/AnomalyRepository.js';
import type { DatasetRepository } from '../../domain/ports/repositories/DatasetRepository.js';
import type { IRNode } from '../../domain/ir/IR.js';
import { IRValidator } from '../../domain/ir/IRValidator.js';
import type { ColumnInfo } from '../../domain/ir/IRValidator.js';
import { InstructionParser, isParseError } from '../services/InstructionParser.js';
import type { ParseError } from '../services/InstructionParser.js';
import { NotFoundError } from '../../domain/errors/NotFoundError.js';
import { ValidationError } from '../../domain/errors/ValidationError.js';
import { DatasetId } from '../../domain/value-objects/DatasetId.js';

// ─── Input / Output DTOs ───────────────────────────────────────────────────

export interface ParseInstructionInput {
  datasetId: string;
  anomalyId: string;
  instruction: string;
  userId: string;
}

export interface ParseInstructionPreview {
  description: string;
  affectedRows: number;
  sampleResult: string | number | null;
  requiresConfirmation: boolean;
}

export interface ParseInstructionSuccess {
  ir: IRNode;
  source: 'rule' | 'gemini';
  preview: ParseInstructionPreview;
}

export type ParseInstructionOutput = ParseInstructionSuccess | ParseError;

// ─── Use Case ─────────────────────────────────────────────────────────────

/**
 * ParseInstructionUseCase
 *
 * Orchestrates:
 *  1. Load anomaly → validate it belongs to the dataset
 *  2. Extract dataset columns from dataset.schema
 *  3. Call InstructionParser (rules → Gemini)
 *  4. Validate result with IRValidator
 *  5. Build preview response
 */
export class ParseInstructionUseCase {
  private readonly parser: InstructionParser;

  constructor(
    private readonly anomalyRepository: AnomalyRepository,
    private readonly datasetRepository: DatasetRepository,
    parser?: InstructionParser
  ) {
    this.parser = parser ?? new InstructionParser();
  }

  async execute(input: ParseInstructionInput): Promise<ParseInstructionOutput> {
    if (!input.instruction || input.instruction.trim() === '') {
      throw new ValidationError('instruction is required', 'instruction');
    }

    // 1. Load dataset
    const dataset = await this.datasetRepository.findById(
      DatasetId.fromString(input.datasetId)
    );
    if (!dataset) {
      throw new NotFoundError('Dataset', input.datasetId);
    }

    // 2. Load anomaly
    const anomaly = await this.anomalyRepository.findById(input.anomalyId);
    if (!anomaly) {
      throw new NotFoundError('Anomaly', input.anomalyId);
    }
    if (anomaly.datasetId !== input.datasetId) {
      throw new ValidationError(
        `Anomaly ${input.anomalyId} does not belong to dataset ${input.datasetId}`,
        'anomalyId'
      );
    }

    // 3. Extract columns from dataset schema
    const columns: ColumnInfo[] = this.extractColumns(dataset.schema);

    // 4. Parse instruction
    const parseResult = await this.parser.parse(
      input.instruction,
      {
        anomalyId: anomaly.id,
        column: anomaly.column,
        dtype: this.getDtypeForColumn(columns, anomaly.column),
        anomalyType: anomaly.type,
        description: anomaly.description,
        samples: this.getSamples(anomaly),
        affectedRows: this.getAffectedRows(anomaly),
      },
      columns
    );

    if (isParseError(parseResult)) {
      return parseResult;
    }

    // 5. Post-parse validation with IRValidator
    const validation = IRValidator.validate(parseResult.ir, columns);
    if (!validation.valid) {
      const availableColumns = columns.map((c) => c.name).join(', ');
      return {
        error: 'invalid_instruction',
        message: `${validation.error}${availableColumns ? `. Columnas disponibles: ${availableColumns}` : ''}.`,
        canRetry: true,
      };
    }

    // 6. Build preview
    const preview = this.buildPreview(validation.node, parseResult.source, anomaly);

    return {
      ir: validation.node,
      source: parseResult.source,
      preview,
    };
  }

  // ─── Private helpers ─────────────────────────────────────────────────────

  /**
   * Extract column info from dataset.schema.
   * The worker stores schema as { columns: [{name, dtype}] } or similar.
   * We support a few common shapes gracefully.
   */
  private extractColumns(schema: Record<string, unknown>): ColumnInfo[] {
    // Shape 1: { columns: [{name: string, dtype: string}] }
    if (Array.isArray(schema['columns'])) {
      return (schema['columns'] as unknown[])
        .filter(
          (c): c is { name: string; dtype: string } =>
            typeof c === 'object' &&
            c !== null &&
            typeof (c as Record<string, unknown>)['name'] === 'string' &&
            typeof (c as Record<string, unknown>)['dtype'] === 'string'
        )
        .map((c) => ({ name: c.name, dtype: c.dtype }));
    }

    // Shape 2: { col1: 'int64', col2: 'object', ... }
    const result: ColumnInfo[] = [];
    for (const [key, value] of Object.entries(schema)) {
      if (typeof value === 'string') {
        result.push({ name: key, dtype: value });
      }
    }
    return result;
  }

  private getDtypeForColumn(columns: ColumnInfo[], column: string): string {
    return columns.find((c) => c.name === column)?.dtype ?? 'unknown';
  }

  private getSamples(anomaly: { originalValue: string | null }): (string | number | null)[] {
    if (anomaly.originalValue !== null) {
      return [anomaly.originalValue];
    }
    return [];
  }

  private getAffectedRows(anomaly: { row: number | null }): number {
    // row may be a specific row index or null (meaning "all rows with this anomaly pattern")
    return anomaly.row !== null ? 1 : 0;
  }

  /**
   * Build a human-readable Spanish description of the IR node.
   */
  private buildPreview(
    node: IRNode,
    source: 'rule' | 'gemini',
    anomaly: { column: string; row: number | null }
  ): ParseInstructionPreview {
    const affectedRows = anomaly.row !== null ? 1 : 0;
    const requiresConfirmation = source === 'gemini';

    const description = this.describeNode(node, anomaly.column);
    const sampleResult = this.sampleResultFor(node);

    return {
      description,
      affectedRows,
      sampleResult,
      requiresConfirmation,
    };
  }

  /**
   * Generate a Spanish description of what the IR node will do.
   */
  private describeNode(node: IRNode, column: string): string {
    switch (node.op) {
      case 'FILL_LITERAL':
        if (node.value === null) {
          return `Rellenará las celdas afectadas con null (valor vacío)`;
        }
        return `Rellenará las celdas afectadas con el valor "${String(node.value)}"`;

      case 'FILL_AGGREGATE': {
        const aggNames: Record<string, string> = {
          mean: 'la media',
          median: 'la mediana',
          mode: 'la moda',
          min: 'el mínimo',
          max: 'el máximo',
          sum: 'la suma',
        };
        const aggLabel = aggNames[node.agg] ?? node.agg;
        return `Rellenará las celdas afectadas con ${aggLabel} de la columna '${column}'`;
      }

      case 'FILL_FROM_COLUMN':
        return `Rellenará las celdas afectadas con el valor de la columna '${node.sourceColumn}'`;

      case 'DELETE_ROWS':
        return `Eliminará las filas afectadas del dataset`;

      case 'KEEP':
        return `Conservará el valor original sin modificarlo`;

      case 'TRANSFORM': {
        const fnNames: Record<string, string> = {
          round: 'redondeará',
          floor: 'aplicará piso (floor) a',
          ceil: 'aplicará techo (ceil) a',
          abs: 'aplicará valor absoluto a',
          upper: 'convertirá a mayúsculas',
          lower: 'convertirá a minúsculas',
          title: 'aplicará formato título (Title Case) a',
          trim: 'eliminará espacios al inicio y fin de',
          multiply: 'multiplicará',
          add: 'sumará',
          subtract: 'restará',
          divide: 'dividirá',
        };
        const fnLabel = fnNames[node.fn] ?? node.fn;
        const inputDesc = this.describeNode(node.input, column);
        return `${fnLabel} el resultado de: ${inputDesc}`;
      }

      case 'CONDITIONAL': {
        const thenDesc = this.describeNode(node.then, column);
        const elseDesc = this.describeNode(node.else, column);
        return `Si se cumple la condición: ${thenDesc}; de lo contrario: ${elseDesc}`;
      }
    }
  }

  /**
   * Compute sampleResult only for FILL_LITERAL (backend doesn't have the DataFrame).
   */
  private sampleResultFor(node: IRNode): string | number | null {
    if (node.op === 'FILL_LITERAL') {
      return node.value;
    }
    return null;
  }
}
