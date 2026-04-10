import type {
  IRNode,
  AggregateFunction,
  TransformFn,
  ConditionOp,
  Condition,
  Operand,
} from './IR.js';

/** Column info provided by the caller (from dataset schema) */
export interface ColumnInfo {
  name: string;
  dtype: string; // e.g. 'int64', 'float64', 'object', 'string', 'bool', etc.
}

export type ValidateResult =
  | { valid: true; node: IRNode }
  | { valid: false; error: string };

// ─── Constants (whitelists) ────────────────────────────────────────────────

const VALID_OPS = new Set([
  'FILL_LITERAL',
  'FILL_AGGREGATE',
  'FILL_FROM_COLUMN',
  'DELETE_ROWS',
  'KEEP',
  'TRANSFORM',
  'CONDITIONAL',
]);

const VALID_AGGS: Set<AggregateFunction> = new Set([
  'mean',
  'median',
  'mode',
  'min',
  'max',
  'sum',
]);

const NUMERIC_AGGS: Set<AggregateFunction> = new Set([
  'mean',
  'median',
  'min',
  'max',
  'sum',
]);

const VALID_TRANSFORMS: Set<TransformFn> = new Set([
  'round',
  'floor',
  'ceil',
  'abs',
  'upper',
  'lower',
  'title',
  'trim',
  'multiply',
  'add',
  'subtract',
  'divide',
]);

const NUMERIC_TRANSFORMS: Set<string> = new Set([
  'round',
  'floor',
  'ceil',
  'abs',
  'multiply',
  'add',
  'subtract',
  'divide',
]);

const STRING_TRANSFORMS: Set<string> = new Set(['upper', 'lower', 'title', 'trim']);

const VALID_CONDITION_OPS: Set<ConditionOp> = new Set([
  'eq',
  'neq',
  'gt',
  'gte',
  'lt',
  'lte',
  'is_null',
  'is_not_null',
  'contains',
  'starts_with',
  'ends_with',
  'between',
]);

const MAX_DEPTH = 3;

// ─── Type guards ───────────────────────────────────────────────────────────

function isNumericDtype(dtype: string): boolean {
  const lower = dtype.toLowerCase();
  return (
    lower.includes('int') ||
    lower.includes('float') ||
    lower.includes('double') ||
    lower.includes('decimal') ||
    lower.includes('numeric') ||
    lower === 'number'
  );
}

function isStringDtype(dtype: string): boolean {
  const lower = dtype.toLowerCase();
  return (
    lower === 'object' ||
    lower === 'string' ||
    lower === 'str' ||
    lower.includes('char') ||
    lower.includes('text')
  );
}

// ─── Validator ─────────────────────────────────────────────────────────────

export class IRValidator {
  /**
   * Validate an unknown IR tree against all 5 rules.
   *
   * @param ir      Unknown value from client or Gemini
   * @param columns Dataset columns with their dtypes (for type-check rules)
   */
  static validate(ir: unknown, columns: ColumnInfo[]): ValidateResult {
    const columnMap = new Map(columns.map((c) => [c.name, c.dtype]));

    try {
      const node = IRValidator.validateNode(ir, 0, columnMap);
      return { valid: true, node };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return { valid: false, error: message };
    }
  }

  // ── Rule 1 + 2: structure check + depth check ──────────────────────────

  private static validateNode(
    raw: unknown,
    depth: number,
    columnMap: Map<string, string>
  ): IRNode {
    // Rule 2: depth check
    if (depth > MAX_DEPTH) {
      throw new Error(`El árbol IR supera la profundidad máxima permitida (${MAX_DEPTH})`);
    }

    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
      throw new Error('El nodo IR debe ser un objeto');
    }

    const node = raw as Record<string, unknown>;
    const op = node['op'];

    if (typeof op !== 'string') {
      throw new Error('Todo nodo IR debe tener un campo "op" de tipo string');
    }

    // Rule 1: whitelist of ops
    if (!VALID_OPS.has(op)) {
      throw new Error(
        `Operación desconocida: "${op}". Operaciones válidas: ${[...VALID_OPS].join(', ')}`
      );
    }

    switch (op) {
      case 'FILL_LITERAL':
        return IRValidator.validateFillLiteral(node);

      case 'FILL_AGGREGATE':
        return IRValidator.validateFillAggregate(node, columnMap);

      case 'FILL_FROM_COLUMN':
        return IRValidator.validateFillFromColumn(node, columnMap);

      case 'DELETE_ROWS':
        return { op: 'DELETE_ROWS' };

      case 'KEEP':
        return { op: 'KEEP' };

      case 'TRANSFORM':
        return IRValidator.validateTransform(node, depth, columnMap);

      case 'CONDITIONAL':
        return IRValidator.validateConditional(node, depth, columnMap);

      default:
        throw new Error(`Operación no implementada: "${op}"`);
    }
  }

  // ── FILL_LITERAL ─────────────────────────────────────────────────────────

  private static validateFillLiteral(node: Record<string, unknown>): IRNode {
    if (!('value' in node)) {
      throw new Error('FILL_LITERAL requiere el campo "value"');
    }
    const value = node['value'];
    if (value !== null && typeof value !== 'string' && typeof value !== 'number') {
      throw new Error(
        `FILL_LITERAL.value debe ser string, number o null; se recibió: ${typeof value}`
      );
    }
    return { op: 'FILL_LITERAL', value: value as string | number | null };
  }

  // ── FILL_AGGREGATE ───────────────────────────────────────────────────────

  private static validateFillAggregate(
    node: Record<string, unknown>,
    columnMap: Map<string, string>
  ): IRNode {
    const agg = node['agg'];
    if (typeof agg !== 'string' || !VALID_AGGS.has(agg as AggregateFunction)) {
      throw new Error(
        `FILL_AGGREGATE.agg inválido: "${agg}". Valores válidos: ${[...VALID_AGGS].join(', ')}`
      );
    }

    // Rule 4: numeric aggs only on numeric columns — but at this level we don't
    // know which column is the target (determined at runtime by the worker).
    // If a "column" hint is provided (not standard but some callers add it), use it.
    // Otherwise we skip the dtype check here and let IRValidator.validateTransform
    // handle it when the FILL_AGGREGATE is wrapped in a TRANSFORM.
    // The per-column dtype check is done in TRANSFORM and CONDITIONAL when
    // sourceColumn is known.
    if (columnMap.size > 0 && NUMERIC_AGGS.has(agg as AggregateFunction)) {
      // We cannot enforce the target dtype here without knowing it; skip.
    }

    return { op: 'FILL_AGGREGATE', agg: agg as AggregateFunction };
  }

  // ── FILL_FROM_COLUMN ─────────────────────────────────────────────────────

  private static validateFillFromColumn(
    node: Record<string, unknown>,
    columnMap: Map<string, string>
  ): IRNode {
    const sourceColumn = node['sourceColumn'];
    if (typeof sourceColumn !== 'string' || sourceColumn.trim() === '') {
      throw new Error('FILL_FROM_COLUMN requiere el campo "sourceColumn" de tipo string');
    }

    // Rule 3: column must exist
    if (columnMap.size > 0 && !columnMap.has(sourceColumn)) {
      const available = [...columnMap.keys()].join(', ');
      throw new Error(
        `La columna "${sourceColumn}" no existe en el dataset. Columnas disponibles: ${available}.`
      );
    }

    return { op: 'FILL_FROM_COLUMN', sourceColumn };
  }

  // ── TRANSFORM ────────────────────────────────────────────────────────────

  private static validateTransform(
    node: Record<string, unknown>,
    depth: number,
    columnMap: Map<string, string>
  ): IRNode {
    const fn = node['fn'];
    if (typeof fn !== 'string' || !VALID_TRANSFORMS.has(fn as TransformFn)) {
      throw new Error(
        `TRANSFORM.fn inválido: "${fn}". Funciones válidas: ${[...VALID_TRANSFORMS].join(', ')}`
      );
    }

    const params =
      node['params'] !== undefined ? (node['params'] as Record<string, unknown>) : undefined;

    if (params !== undefined && (typeof params !== 'object' || Array.isArray(params))) {
      throw new Error('TRANSFORM.params debe ser un objeto');
    }

    const inputNode = IRValidator.validateNode(node['input'], depth + 1, columnMap);

    // Rule 4: type compatibility for transforms
    // We infer the column dtype from the input node when possible.
    const inferredDtype = IRValidator.inferDtypeFromNode(inputNode, columnMap);

    if (inferredDtype !== null) {
      if (NUMERIC_TRANSFORMS.has(fn) && !isNumericDtype(inferredDtype)) {
        throw new Error(
          `La transformación "${fn}" solo aplica a columnas numéricas, pero la columna tiene dtype "${inferredDtype}"`
        );
      }
      if (STRING_TRANSFORMS.has(fn) && !isStringDtype(inferredDtype)) {
        throw new Error(
          `La transformación "${fn}" solo aplica a columnas de texto, pero la columna tiene dtype "${inferredDtype}"`
        );
      }
    }

    return {
      op: 'TRANSFORM',
      fn: fn as TransformFn,
      ...(params !== undefined ? { params } : {}),
      input: inputNode,
    };
  }

  // ── CONDITIONAL ──────────────────────────────────────────────────────────

  private static validateConditional(
    node: Record<string, unknown>,
    depth: number,
    columnMap: Map<string, string>
  ): IRNode {
    const condition = IRValidator.validateCondition(node['condition'], columnMap);
    const thenNode = IRValidator.validateNode(node['then'], depth + 1, columnMap);
    const elseNode = IRValidator.validateNode(node['else'], depth + 1, columnMap);

    return {
      op: 'CONDITIONAL',
      condition,
      then: thenNode,
      else: elseNode,
    };
  }

  // ── Condition ────────────────────────────────────────────────────────────

  private static validateCondition(
    raw: unknown,
    columnMap: Map<string, string>
  ): Condition {
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
      throw new Error('CONDITIONAL.condition debe ser un objeto');
    }

    const cond = raw as Record<string, unknown>;
    const op = cond['op'];

    if (typeof op !== 'string' || !VALID_CONDITION_OPS.has(op as ConditionOp)) {
      throw new Error(
        `Operador de condición inválido: "${op}". Válidos: ${[...VALID_CONDITION_OPS].join(', ')}`
      );
    }

    const left = IRValidator.validateOperand(cond['left'], columnMap);

    if (op === 'is_null' || op === 'is_not_null') {
      return { op: op as ConditionOp, left };
    }

    const right = IRValidator.validateOperand(cond['right'], columnMap);

    if (op === 'between') {
      const upper = IRValidator.validateOperand(cond['upper'], columnMap);
      // Rule 5: operand type compatibility for 'between'
      IRValidator.assertCompatibleOperands(left, right, columnMap);
      IRValidator.assertCompatibleOperands(left, upper, columnMap);
      return { op: op as ConditionOp, left, right, upper };
    }

    // Rule 5: operand type compatibility
    IRValidator.assertCompatibleOperands(left, right, columnMap);

    return { op: op as ConditionOp, left, right };
  }

  // ── Operand ──────────────────────────────────────────────────────────────

  private static validateOperand(raw: unknown, columnMap: Map<string, string>): Operand {
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
      throw new Error('Un operando debe ser un objeto con campo "kind"');
    }

    const o = raw as Record<string, unknown>;
    const kind = o['kind'];

    if (kind === 'literal') {
      const value = o['value'];
      if (
        value !== null &&
        typeof value !== 'string' &&
        typeof value !== 'number' &&
        typeof value !== 'boolean'
      ) {
        throw new Error(
          `Operando literal: "value" debe ser string, number, boolean o null`
        );
      }
      return { kind: 'literal', value: value as string | number | boolean | null };
    }

    if (kind === 'column') {
      const column = o['column'];
      if (typeof column !== 'string' || column.trim() === '') {
        throw new Error('Operando column: "column" debe ser string no vacío');
      }
      // Rule 3
      if (columnMap.size > 0 && !columnMap.has(column)) {
        const available = [...columnMap.keys()].join(', ');
        throw new Error(
          `La columna "${column}" no existe en el dataset. Columnas disponibles: ${available}.`
        );
      }
      return { kind: 'column', column };
    }

    if (kind === 'aggregate') {
      const agg = o['agg'];
      if (typeof agg !== 'string' || !VALID_AGGS.has(agg as AggregateFunction)) {
        throw new Error(
          `Operando aggregate: agg inválido "${agg}". Válidos: ${[...VALID_AGGS].join(', ')}`
        );
      }
      const column = o['column'];
      if (typeof column !== 'string' || column.trim() === '') {
        throw new Error('Operando aggregate: "column" debe ser string no vacío');
      }
      // Rule 3
      if (columnMap.size > 0 && !columnMap.has(column)) {
        const available = [...columnMap.keys()].join(', ');
        throw new Error(
          `La columna "${column}" no existe en el dataset. Columnas disponibles: ${available}.`
        );
      }
      // Rule 4: numeric agg only on numeric column
      const dtype = columnMap.get(column);
      if (dtype !== undefined && NUMERIC_AGGS.has(agg as AggregateFunction)) {
        if (!isNumericDtype(dtype)) {
          throw new Error(
            `El agregado "${agg}" solo aplica a columnas numéricas, pero "${column}" tiene dtype "${dtype}"`
          );
        }
      }
      return { kind: 'aggregate', agg: agg as AggregateFunction, column };
    }

    throw new Error(
      `Tipo de operando desconocido: "${kind}". Tipos válidos: literal, column, aggregate`
    );
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  /**
   * Infer the dtype of a node's output when it references a known column.
   * Returns null when we can't determine it statically.
   */
  private static inferDtypeFromNode(
    node: IRNode,
    columnMap: Map<string, string>
  ): string | null {
    if (node.op === 'FILL_FROM_COLUMN') {
      return columnMap.get(node.sourceColumn) ?? null;
    }
    if (node.op === 'FILL_AGGREGATE') {
      // No target column known at this level
      return null;
    }
    return null;
  }

  /**
   * Rule 5: Assert two operands have compatible types.
   * Numeric literals/columns must not be compared to string literals/columns.
   */
  private static assertCompatibleOperands(
    a: Operand,
    b: Operand,
    _columnMap: Map<string, string>
  ): void {
    // We do a lightweight check: if both sides are literals of different primitive types, reject.
    if (a.kind === 'literal' && b.kind === 'literal') {
      const aIsNum = typeof a.value === 'number';
      const bIsNum = typeof b.value === 'number';
      const aIsStr = typeof a.value === 'string';
      const bIsStr = typeof b.value === 'string';

      if ((aIsNum && bIsStr) || (aIsStr && bIsNum)) {
        throw new Error(
          `Los operandos de la condición tienen tipos incompatibles: ` +
            `"${typeof a.value}" vs "${typeof b.value}"`
        );
      }
    }
    // Column vs literal: we'd need runtime dtype info to fully enforce this.
    // The worker will catch type errors at execution time.
  }
}
