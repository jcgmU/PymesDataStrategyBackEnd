/**
 * IR (Intermediate Representation) type definitions.
 * These types form the closed vocabulary of operations that the
 * NL parser can emit and the worker can execute.
 *
 * The IR is a tree where each node is one of 7 operations.
 * Max depth = 3 (root = depth 0).
 */

export type AggregateFunction = 'mean' | 'median' | 'mode' | 'min' | 'max' | 'sum';

export type TransformFn =
  | 'round'
  | 'floor'
  | 'ceil'
  | 'abs'
  | 'upper'
  | 'lower'
  | 'title'
  | 'trim'
  | 'multiply'
  | 'add'
  | 'subtract'
  | 'divide';

export type ConditionOp =
  | 'eq'
  | 'neq'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'is_null'
  | 'is_not_null'
  | 'contains'
  | 'starts_with'
  | 'ends_with'
  | 'between';

export type Operand =
  | { kind: 'column'; column: string }
  | { kind: 'literal'; value: string | number | boolean | null }
  | { kind: 'aggregate'; agg: AggregateFunction; column: string };

export interface Condition {
  op: ConditionOp;
  left: Operand;
  right?: Operand;
  upper?: Operand; // for 'between'
}

/**
 * A node in the IR tree. Each node represents one operation.
 */
export type IRNode =
  | { op: 'FILL_LITERAL'; value: string | number | null }
  | { op: 'FILL_AGGREGATE'; agg: AggregateFunction }
  | { op: 'FILL_FROM_COLUMN'; sourceColumn: string }
  | { op: 'DELETE_ROWS' }
  | { op: 'KEEP' }
  | { op: 'TRANSFORM'; fn: TransformFn; params?: Record<string, unknown>; input: IRNode }
  | { op: 'CONDITIONAL'; condition: Condition; then: IRNode; else: IRNode };
