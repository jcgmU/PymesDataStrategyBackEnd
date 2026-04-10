import { describe, it, expect, vi, beforeEach } from 'vitest';
import { InstructionParser } from '../InstructionParser.js';
import type { AnomalyContext } from '../InstructionParser.js';
import type { ColumnInfo } from '../../../domain/ir/IRValidator.js';

// ─── Helpers ──────────────────────────────────────────────────────────────

const ctx: AnomalyContext = {
  anomalyId: 'anomaly-1',
  column: 'precio',
  dtype: 'float64',
  anomalyType: 'OUTLIER',
  description: 'Outlier detected',
  samples: [150, 200, null],
  affectedRows: 3,
};

const numericCols: ColumnInfo[] = [
  { name: 'precio', dtype: 'float64' },
  { name: 'edad', dtype: 'int64' },
];

const allCols: ColumnInfo[] = [
  ...numericCols,
  { name: 'nombre', dtype: 'object' },
];

// Parser with no Gemini key — forces Gemini to return error
const parserNoGemini = new InstructionParser('', 'gemini-2.0-flash-exp');

// ─── Rule-based parsing tests ─────────────────────────────────────────────

describe('InstructionParser — rule-based', () => {
  describe('null literal rule', () => {
    it('parses "null" (lowercase)', async () => {
      const r = await parserNoGemini.parse('null', ctx, []);
      expect('ir' in r).toBe(true);
      if ('ir' in r) {
        expect(r.ir).toEqual({ op: 'FILL_LITERAL', value: null });
        expect(r.source).toBe('rule');
      }
    });

    it('parses "NULL" (uppercase)', async () => {
      const r = await parserNoGemini.parse('NULL', ctx, []);
      expect('ir' in r).toBe(true);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_LITERAL', value: null });
    });

    it('parses " null " (with whitespace)', async () => {
      const r = await parserNoGemini.parse('  null  ', ctx, []);
      expect('ir' in r).toBe(true);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_LITERAL', value: null });
    });
  });

  describe('numeric literal rule', () => {
    it('parses integer "42"', async () => {
      const r = await parserNoGemini.parse('42', ctx, []);
      expect('ir' in r).toBe(true);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_LITERAL', value: 42 });
    });

    it('parses float "3.14"', async () => {
      const r = await parserNoGemini.parse('3.14', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_LITERAL', value: 3.14 });
    });

    it('parses negative "-5"', async () => {
      const r = await parserNoGemini.parse('-5', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_LITERAL', value: -5 });
    });

    it('parses "0"', async () => {
      const r = await parserNoGemini.parse('0', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_LITERAL', value: 0 });
    });
  });

  describe('quoted string literal rule', () => {
    it('parses double-quoted "Sin dato"', async () => {
      const r = await parserNoGemini.parse('"Sin dato"', ctx, []);
      expect('ir' in r).toBe(true);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_LITERAL', value: 'Sin dato' });
    });

    it('parses single-quoted \'N/A\'', async () => {
      const r = await parserNoGemini.parse("'N/A'", ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_LITERAL', value: 'N/A' });
    });
  });

  describe('FILL_AGGREGATE rules', () => {
    it('parses "usa la media"', async () => {
      const r = await parserNoGemini.parse('usa la media', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'mean' });
    });

    it('parses "usa el promedio"', async () => {
      const r = await parserNoGemini.parse('usa el promedio', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'mean' });
    });

    it('parses "pon la media"', async () => {
      const r = await parserNoGemini.parse('pon la media', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'mean' });
    });

    it('parses "usa la mediana"', async () => {
      const r = await parserNoGemini.parse('usa la mediana', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'median' });
    });

    it('parses "con la mediana"', async () => {
      const r = await parserNoGemini.parse('con la mediana', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'median' });
    });

    it('parses "usa la moda"', async () => {
      const r = await parserNoGemini.parse('usa la moda', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'mode' });
    });

    it('parses "pon la moda"', async () => {
      const r = await parserNoGemini.parse('pon la moda', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'mode' });
    });

    it('parses "usa el mínimo"', async () => {
      const r = await parserNoGemini.parse('usa el mínimo', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'min' });
    });

    it('parses "usa el minimo" (without accent)', async () => {
      const r = await parserNoGemini.parse('usa el minimo', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'min' });
    });

    it('parses "usa el máximo"', async () => {
      const r = await parserNoGemini.parse('usa el máximo', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'max' });
    });

    it('parses "con el maximo" (without accent)', async () => {
      const r = await parserNoGemini.parse('con el maximo', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'max' });
    });
  });

  describe('DELETE_ROWS rule', () => {
    it('parses "elimina las filas"', async () => {
      const r = await parserNoGemini.parse('elimina las filas', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'DELETE_ROWS' });
    });

    it('parses "borra las filas afectadas"', async () => {
      const r = await parserNoGemini.parse('borra las filas afectadas', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'DELETE_ROWS' });
    });

    it('parses "quita el registro"', async () => {
      const r = await parserNoGemini.parse('quita el registro', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'DELETE_ROWS' });
    });
  });

  describe('KEEP rule', () => {
    it('parses "mantener"', async () => {
      const r = await parserNoGemini.parse('mantener', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'KEEP' });
    });

    it('parses "dejar el valor"', async () => {
      const r = await parserNoGemini.parse('dejar el valor', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'KEEP' });
    });

    it('parses "conserva"', async () => {
      const r = await parserNoGemini.parse('conserva', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'KEEP' });
    });

    it('parses "deja el valor original"', async () => {
      const r = await parserNoGemini.parse('deja el valor original', ctx, []);
      if ('ir' in r) expect(r.ir).toEqual({ op: 'KEEP' });
    });
  });

  describe('FILL_FROM_COLUMN rule', () => {
    it('parses "copia de precio" when column exists', async () => {
      const r = await parserNoGemini.parse('copia de precio', ctx, numericCols);
      if ('ir' in r) {
        expect(r.ir.op).toBe('FILL_FROM_COLUMN');
        if (r.ir.op === 'FILL_FROM_COLUMN') expect(r.ir.sourceColumn).toBe('precio');
      }
    });

    it('parses "toma del precio" when column exists', async () => {
      const r = await parserNoGemini.parse('toma del precio', ctx, numericCols);
      if ('ir' in r) expect(r.ir.op).toBe('FILL_FROM_COLUMN');
    });

    it('falls through to Gemini when column does not exist', async () => {
      // Column 'ghost' doesn't exist → rule match is null → Gemini called → error (no key)
      const r = await parserNoGemini.parse('copia de ghost', ctx, numericCols);
      // With no Gemini key, expect gemini_unavailable error
      expect('error' in r).toBe(true);
    });
  });
});

// ─── Conditional escalation ──────────────────────────────────────────────

describe('InstructionParser — conditional escalation', () => {
  it('escalates to Gemini when "si" keyword is present with aggregate', async () => {
    // "usa la mediana si es nulo" — has "si" keyword → escalates to Gemini
    // With no API key, Gemini returns error
    const r = await parserNoGemini.parse('usa la mediana si es nulo', ctx, []);
    expect('error' in r).toBe(true);
    if ('error' in r) expect(r.error).toBe('gemini_unavailable');
  });

  it('escalates when "mayor" keyword is present', async () => {
    const r = await parserNoGemini.parse('si es mayor a 100 usa 100', ctx, []);
    expect('error' in r).toBe(true);
  });

  it('escalates when "menor" keyword is present', async () => {
    const r = await parserNoGemini.parse('si es menor que 0 elimina', ctx, []);
    expect('error' in r).toBe(true);
  });
});

// ─── Gemini fallback tests (mocked) ─────────────────────────────────────

describe('InstructionParser — Gemini fallback (mocked)', () => {
  let parser: InstructionParser;

  beforeEach(() => {
    parser = new InstructionParser('fake-key', 'gemini-2.0-flash-exp');
  });

  it('returns IR from Gemini when valid JSON is returned', async () => {
    const validIR = JSON.stringify({ op: 'FILL_AGGREGATE', agg: 'median' });

    // Mock the private callGeminiAPI method via the dynamic import path
    // We spy on the module's dynamic import by mocking at the class level
    vi.spyOn(parser as unknown as { callGeminiAPI: (...args: unknown[]) => Promise<string> }, 'callGeminiAPI')
      .mockResolvedValueOnce(validIR);

    const r = await parser.parse('si es nulo usa la mediana', ctx, []);
    expect('ir' in r).toBe(true);
    if ('ir' in r) {
      expect(r.ir).toEqual({ op: 'FILL_AGGREGATE', agg: 'median' });
      expect(r.source).toBe('gemini');
    }
  });

  it('retries once when Gemini returns malformed JSON, then fails', async () => {
    vi.spyOn(parser as unknown as { callGeminiAPI: (...args: unknown[]) => Promise<string> }, 'callGeminiAPI')
      .mockResolvedValue('not valid json at all {{{');

    const r = await parser.parse('si es nulo usa la mediana', ctx, []);
    expect('error' in r).toBe(true);
    if ('error' in r) expect(r.error).toBe('gemini_unavailable');
  });

  it('retries once when Gemini returns invalid IR, then fails', async () => {
    const invalidIR = JSON.stringify({ op: 'UNKNOWN_OP' });

    vi.spyOn(parser as unknown as { callGeminiAPI: (...args: unknown[]) => Promise<string> }, 'callGeminiAPI')
      .mockResolvedValue(invalidIR);

    const r = await parser.parse('si es nulo usa la mediana', ctx, []);
    expect('error' in r).toBe(true);
    if ('error' in r) expect(r.error).toBe('gemini_unavailable');
  });

  it('returns gemini_unavailable on 503-like error', async () => {
    vi.spyOn(parser as unknown as { callGeminiAPI: (...args: unknown[]) => Promise<string> }, 'callGeminiAPI')
      .mockRejectedValueOnce(new Error('503 Service Unavailable'));

    const r = await parser.parse('si es nulo usa la mediana', ctx, []);
    expect('error' in r).toBe(true);
    if ('error' in r) {
      expect(r.error).toBe('gemini_unavailable');
      expect(r.canRetry).toBe(true);
    }
  });

  it('returns gemini_unavailable on timeout', async () => {
    vi.spyOn(parser as unknown as { callGeminiAPI: (...args: unknown[]) => Promise<string> }, 'callGeminiAPI')
      .mockRejectedValueOnce(new Error('timeout: Gemini did not respond within 10 seconds'));

    const r = await parser.parse('si es nulo usa la mediana', ctx, []);
    expect('error' in r).toBe(true);
    if ('error' in r) expect(r.canRetry).toBe(true);
  });

  it('returns gemini_unavailable when package not installed', async () => {
    vi.spyOn(parser as unknown as { callGeminiAPI: (...args: unknown[]) => Promise<string> }, 'callGeminiAPI')
      .mockRejectedValueOnce(new Error('Package @google/generative-ai not installed'));

    const r = await parser.parse('si es nulo usa la mediana', ctx, []);
    expect('error' in r).toBe(true);
  });

  it('returns valid IR on second attempt after validation error', async () => {
    const invalidIR = JSON.stringify({ op: 'UNKNOWN_OP' });
    const validIR = JSON.stringify({ op: 'DELETE_ROWS' });

    vi.spyOn(parser as unknown as { callGeminiAPI: (...args: unknown[]) => Promise<string> }, 'callGeminiAPI')
      .mockResolvedValueOnce(invalidIR)
      .mockResolvedValueOnce(validIR);

    const r = await parser.parse('si es nulo elimina la fila', ctx, []);
    expect('ir' in r).toBe(true);
    if ('ir' in r) expect(r.ir).toEqual({ op: 'DELETE_ROWS' });
  });
});
