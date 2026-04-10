import { describe, it, expect } from 'vitest';
import { IRValidator } from '../IRValidator.js';
import type { ColumnInfo } from '../IRValidator.js';

// ─── Helpers ──────────────────────────────────────────────────────────────

const numericCols: ColumnInfo[] = [
  { name: 'precio', dtype: 'float64' },
  { name: 'edad', dtype: 'int64' },
];

const stringCols: ColumnInfo[] = [
  { name: 'nombre', dtype: 'object' },
  { name: 'email', dtype: 'string' },
];

const mixedCols: ColumnInfo[] = [...numericCols, ...stringCols];

const noCols: ColumnInfo[] = [];

function valid(ir: unknown, cols: ColumnInfo[] = noCols) {
  const result = IRValidator.validate(ir, cols);
  expect(result.valid).toBe(true);
  return result;
}

function invalid(ir: unknown, cols: ColumnInfo[] = noCols) {
  const result = IRValidator.validate(ir, cols);
  expect(result.valid).toBe(false);
  return result;
}

// ─── Tests ────────────────────────────────────────────────────────────────

describe('IRValidator', () => {
  // ── Valid IR for all 7 ops ──────────────────────────────────────────────

  describe('FILL_LITERAL', () => {
    it('accepts string value', () => {
      const r = valid({ op: 'FILL_LITERAL', value: 'Sin dato' });
      if (r.valid) expect(r.node.op).toBe('FILL_LITERAL');
    });

    it('accepts numeric value', () => {
      const r = valid({ op: 'FILL_LITERAL', value: 42.5 });
      if (r.valid && r.node.op === 'FILL_LITERAL') expect(r.node.value).toBe(42.5);
    });

    it('accepts null value', () => {
      const r = valid({ op: 'FILL_LITERAL', value: null });
      if (r.valid && r.node.op === 'FILL_LITERAL') expect(r.node.value).toBeNull();
    });

    it('rejects boolean value', () => {
      invalid({ op: 'FILL_LITERAL', value: true });
    });

    it('rejects missing value field', () => {
      invalid({ op: 'FILL_LITERAL' });
    });
  });

  describe('FILL_AGGREGATE', () => {
    it('accepts mean', () => {
      valid({ op: 'FILL_AGGREGATE', agg: 'mean' });
    });
    it('accepts median', () => {
      valid({ op: 'FILL_AGGREGATE', agg: 'median' });
    });
    it('accepts mode', () => {
      valid({ op: 'FILL_AGGREGATE', agg: 'mode' });
    });
    it('accepts min', () => {
      valid({ op: 'FILL_AGGREGATE', agg: 'min' });
    });
    it('accepts max', () => {
      valid({ op: 'FILL_AGGREGATE', agg: 'max' });
    });
    it('accepts sum', () => {
      valid({ op: 'FILL_AGGREGATE', agg: 'sum' });
    });
    it('rejects unknown agg', () => {
      invalid({ op: 'FILL_AGGREGATE', agg: 'variance' });
    });
  });

  describe('FILL_FROM_COLUMN', () => {
    it('accepts valid column (no column list)', () => {
      valid({ op: 'FILL_FROM_COLUMN', sourceColumn: 'precio' });
    });

    it('accepts valid column when column exists in list', () => {
      valid({ op: 'FILL_FROM_COLUMN', sourceColumn: 'precio' }, numericCols);
    });

    it('rejects non-existent column when column list is provided', () => {
      const r = invalid({ op: 'FILL_FROM_COLUMN', sourceColumn: 'nonexistent' }, numericCols);
      if (!r.valid) expect(r.error).toContain('nonexistent');
    });

    it('rejects missing sourceColumn', () => {
      invalid({ op: 'FILL_FROM_COLUMN' });
    });
  });

  describe('DELETE_ROWS', () => {
    it('accepts DELETE_ROWS', () => {
      valid({ op: 'DELETE_ROWS' });
    });
  });

  describe('KEEP', () => {
    it('accepts KEEP', () => {
      valid({ op: 'KEEP' });
    });
  });

  describe('TRANSFORM', () => {
    it('accepts round with numeric input', () => {
      valid({
        op: 'TRANSFORM',
        fn: 'round',
        params: { decimals: 2 },
        input: { op: 'FILL_AGGREGATE', agg: 'mean' },
      });
    });

    it('accepts upper on FILL_FROM_COLUMN string column', () => {
      valid(
        {
          op: 'TRANSFORM',
          fn: 'upper',
          input: { op: 'FILL_FROM_COLUMN', sourceColumn: 'nombre' },
        },
        stringCols
      );
    });

    it('accepts floor with no column context (can\'t check dtype)', () => {
      valid({
        op: 'TRANSFORM',
        fn: 'floor',
        input: { op: 'FILL_AGGREGATE', agg: 'mean' },
      });
    });

    it('accepts multiply', () => {
      valid({
        op: 'TRANSFORM',
        fn: 'multiply',
        params: { by: 2 },
        input: { op: 'FILL_AGGREGATE', agg: 'mean' },
      });
    });

    it('rejects unknown fn', () => {
      invalid({
        op: 'TRANSFORM',
        fn: 'sqrt',
        input: { op: 'FILL_AGGREGATE', agg: 'mean' },
      });
    });

    it('rejects numeric transform on string column', () => {
      invalid(
        {
          op: 'TRANSFORM',
          fn: 'round',
          input: { op: 'FILL_FROM_COLUMN', sourceColumn: 'nombre' },
        },
        stringCols
      );
    });

    it('rejects string transform on numeric column', () => {
      invalid(
        {
          op: 'TRANSFORM',
          fn: 'upper',
          input: { op: 'FILL_FROM_COLUMN', sourceColumn: 'precio' },
        },
        numericCols
      );
    });

    it('rejects missing input field', () => {
      invalid({ op: 'TRANSFORM', fn: 'round' });
    });
  });

  describe('CONDITIONAL', () => {
    const validConditional = {
      op: 'CONDITIONAL',
      condition: {
        op: 'gt',
        left: { kind: 'column', column: 'precio' },
        right: { kind: 'literal', value: 100 },
      },
      then: { op: 'FILL_LITERAL', value: '100' },
      else: { op: 'KEEP' },
    };

    it('accepts valid conditional with column list', () => {
      valid(validConditional, numericCols);
    });

    it('accepts valid conditional without column list', () => {
      valid(validConditional);
    });

    it('accepts is_null condition (no right operand)', () => {
      valid({
        op: 'CONDITIONAL',
        condition: {
          op: 'is_null',
          left: { kind: 'column', column: 'precio' },
        },
        then: { op: 'FILL_LITERAL', value: 0 },
        else: { op: 'KEEP' },
      });
    });

    it('accepts between condition', () => {
      valid({
        op: 'CONDITIONAL',
        condition: {
          op: 'between',
          left: { kind: 'column', column: 'precio' },
          right: { kind: 'literal', value: 10 },
          upper: { kind: 'literal', value: 100 },
        },
        then: { op: 'FILL_LITERAL', value: 'ok' },
        else: { op: 'DELETE_ROWS' },
      });
    });

    it('rejects unknown condition op', () => {
      const r = invalid({
        op: 'CONDITIONAL',
        condition: {
          op: 'not_exists',
          left: { kind: 'column', column: 'precio' },
          right: { kind: 'literal', value: 100 },
        },
        then: { op: 'KEEP' },
        else: { op: 'KEEP' },
      });
      if (!r.valid) expect(r.error).toContain('not_exists');
    });

    it('rejects non-existent column in condition', () => {
      const r = invalid(
        {
          op: 'CONDITIONAL',
          condition: {
            op: 'gt',
            left: { kind: 'column', column: 'ghost_column' },
            right: { kind: 'literal', value: 50 },
          },
          then: { op: 'KEEP' },
          else: { op: 'KEEP' },
        },
        numericCols
      );
      if (!r.valid) expect(r.error).toContain('ghost_column');
    });
  });

  // ── Rule 1: unknown op ──────────────────────────────────────────────────

  describe('unknown op', () => {
    it('rejects unknown op string', () => {
      const r = invalid({ op: 'EXECUTE_CODE' });
      if (!r.valid) expect(r.error).toContain('EXECUTE_CODE');
    });

    it('rejects missing op', () => {
      invalid({ value: 42 });
    });

    it('rejects non-object node', () => {
      invalid('FILL_LITERAL');
    });

    it('rejects null node', () => {
      invalid(null);
    });
  });

  // ── Rule 2: max depth ──────────────────────────────────────────────────

  describe('max depth', () => {
    it('accepts depth 2 (CONDITIONAL → TRANSFORM → FILL_AGGREGATE)', () => {
      valid({
        op: 'CONDITIONAL',
        condition: {
          op: 'gt',
          left: { kind: 'column', column: 'precio' },
          right: { kind: 'literal', value: 0 },
        },
        then: {
          op: 'TRANSFORM',
          fn: 'round',
          input: { op: 'FILL_AGGREGATE', agg: 'mean' },
        },
        else: { op: 'KEEP' },
      });
    });

    it('rejects depth 4', () => {
      // CONDITIONAL(0) → TRANSFORM(1) → TRANSFORM(2) → TRANSFORM(3) → FILL_LITERAL(4) — exceeds MAX_DEPTH=3
      const r = invalid({
        op: 'CONDITIONAL',
        condition: {
          op: 'gt',
          left: { kind: 'literal', value: 1 },
          right: { kind: 'literal', value: 0 },
        },
        then: {
          op: 'TRANSFORM',
          fn: 'abs',
          input: {
            op: 'TRANSFORM',
            fn: 'floor',
            input: {
              op: 'TRANSFORM',
              fn: 'ceil',
              input: {
                op: 'FILL_LITERAL',
                value: 42,
              },
            },
          },
        },
        else: { op: 'KEEP' },
      });
      if (!r.valid) expect(r.error).toContain('profundidad');
    });
  });

  // ── Rule 3: column existence ────────────────────────────────────────────

  describe('column existence', () => {
    it('rejects FILL_FROM_COLUMN with nonexistent column', () => {
      const r = invalid(
        { op: 'FILL_FROM_COLUMN', sourceColumn: 'ghost' },
        mixedCols
      );
      if (!r.valid) expect(r.error).toContain('ghost');
    });

    it('allows any column when no column list provided', () => {
      valid({ op: 'FILL_FROM_COLUMN', sourceColumn: 'any_column' });
    });
  });

  // ── Rule 4: type compatibility ──────────────────────────────────────────

  describe('type compatibility', () => {
    it('rejects numeric transform (round) on string column', () => {
      invalid(
        {
          op: 'TRANSFORM',
          fn: 'round',
          input: { op: 'FILL_FROM_COLUMN', sourceColumn: 'nombre' },
        },
        stringCols
      );
    });

    it('rejects string transform (trim) on numeric column', () => {
      invalid(
        {
          op: 'TRANSFORM',
          fn: 'trim',
          input: { op: 'FILL_FROM_COLUMN', sourceColumn: 'precio' },
        },
        numericCols
      );
    });

    it('rejects agg (mean) on aggregate operand targeting string column', () => {
      const r = invalid(
        {
          op: 'CONDITIONAL',
          condition: {
            op: 'gt',
            left: { kind: 'aggregate', agg: 'mean', column: 'nombre' },
            right: { kind: 'literal', value: 0 },
          },
          then: { op: 'KEEP' },
          else: { op: 'KEEP' },
        },
        stringCols
      );
      if (!r.valid) expect(r.error).toContain('mean');
    });

    it('accepts mode on any column type (string col)', () => {
      valid(
        {
          op: 'CONDITIONAL',
          condition: {
            op: 'is_not_null',
            left: { kind: 'column', column: 'nombre' },
          },
          then: { op: 'FILL_AGGREGATE', agg: 'mode' },
          else: { op: 'KEEP' },
        },
        stringCols
      );
    });
  });

  // ── Rule 5: operand type compatibility ──────────────────────────────────

  describe('operand type compatibility', () => {
    it('rejects comparing string literal to number literal', () => {
      invalid({
        op: 'CONDITIONAL',
        condition: {
          op: 'eq',
          left: { kind: 'literal', value: 'hello' },
          right: { kind: 'literal', value: 42 },
        },
        then: { op: 'KEEP' },
        else: { op: 'KEEP' },
      });
    });

    it('accepts comparing number to number literal', () => {
      valid({
        op: 'CONDITIONAL',
        condition: {
          op: 'gt',
          left: { kind: 'literal', value: 100 },
          right: { kind: 'literal', value: 50 },
        },
        then: { op: 'KEEP' },
        else: { op: 'KEEP' },
      });
    });
  });
});
