export type AnomalyType = 'MISSING_VALUE' | 'OUTLIER' | 'FORMAT_ERROR' | 'DUPLICATE';
export type AnomalyStatus = 'PENDING' | 'RESOLVED';
export type DecisionAction = 'APPROVED' | 'CORRECTED' | 'DISCARDED';

export interface AnomalyDecision {
  id: string;
  anomalyId: string;
  action: DecisionAction;
  correction: string | null;
  correctionIr: Record<string, unknown> | null;
  irSource: string | null;
  irRawText: string | null;
  userId: string;
  createdAt: Date;
}

export interface AnomalyProps {
  id: string;
  datasetId: string;
  column: string;
  row: number | null;
  type: AnomalyType;
  description: string;
  originalValue: string | null;
  suggestedValue: string | null;
  aiSuggestion: string | null;
  status: AnomalyStatus;
  createdAt: Date;
  updatedAt: Date;
  decision: AnomalyDecision | null;
}

/**
 * Anomaly domain entity.
 * Represents a data quality issue detected in a dataset that requires
 * a human decision (Human-in-the-Loop).
 */
export class Anomaly {
  private constructor(private readonly props: AnomalyProps) {}

  static create(
    props: Omit<AnomalyProps, 'status' | 'createdAt' | 'updatedAt' | 'decision' | 'aiSuggestion'>
  ): Anomaly {
    return new Anomaly({
      ...props,
      aiSuggestion: null,
      status: 'PENDING',
      createdAt: new Date(),
      updatedAt: new Date(),
      decision: null,
    });
  }

  static reconstitute(props: AnomalyProps): Anomaly {
    return new Anomaly(props);
  }

  get id(): string {
    return this.props.id;
  }

  get datasetId(): string {
    return this.props.datasetId;
  }

  get column(): string {
    return this.props.column;
  }

  get row(): number | null {
    return this.props.row;
  }

  get type(): AnomalyType {
    return this.props.type;
  }

  get description(): string {
    return this.props.description;
  }

  get originalValue(): string | null {
    return this.props.originalValue;
  }

  get suggestedValue(): string | null {
    return this.props.suggestedValue;
  }

  get aiSuggestion(): string | null {
    return this.props.aiSuggestion;
  }

  setAiSuggestion(suggestion: string): void {
    this.props.aiSuggestion = suggestion;
    this.props.updatedAt = new Date();
  }

  get status(): AnomalyStatus {
    return this.props.status;
  }

  get createdAt(): Date {
    return this.props.createdAt;
  }

  get updatedAt(): Date {
    return this.props.updatedAt;
  }

  get decision(): AnomalyDecision | null {
    return this.props.decision;
  }

  resolve(decision: AnomalyDecision): void {
    this.props.status = 'RESOLVED';
    this.props.decision = decision;
    this.props.updatedAt = new Date();
  }
}
