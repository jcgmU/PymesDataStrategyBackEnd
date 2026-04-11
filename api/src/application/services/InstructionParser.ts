/**
 * InstructionParser: converts a free-text Spanish instruction into an IR node.
 *
 * Strategy:
 * 1. Rule-based matching (11 regex patterns) — fast, no external calls
 * 2. Gemini fallback — for complex / conditional instructions
 *
 * Dependencies:
 *   - GEMINI_API_KEY env var
 *   - GEMINI_MODEL   env var (default: "gemini-2.0-flash-exp")
 *   - npm package: @google/generative-ai
 *
 * Install: cd backend/api && npm install @google/generative-ai
 */

import type { IRNode, AggregateFunction } from '../../domain/ir/IR.js';
import { IRValidator } from '../../domain/ir/IRValidator.js';
import type { ColumnInfo } from '../../domain/ir/IRValidator.js';

// ─── Types ─────────────────────────────────────────────────────────────────

export interface ParseSuccess {
  ir: IRNode;
  source: 'rule' | 'gemini';
}

export interface ParseError {
  error: string;
  message: string;
  canRetry: boolean;
}

export type ParseResult = ParseSuccess | ParseError;

export function isParseError(result: ParseResult): result is ParseError {
  return 'error' in result;
}

export interface AnomalyContext {
  anomalyId: string;
  column: string;
  dtype: string;
  anomalyType: string;
  description: string;
  samples: (string | number | null)[];
  affectedRows: number;
}

// ─── Rule patterns ─────────────────────────────────────────────────────────

/** Keywords that indicate a conditional expression → escalate to Gemini */
const CONDITIONAL_KEYWORDS = /\b(si|cuando|mayor|menor|igual|entre|mientras)\b/i;

interface RuleEntry {
  pattern: RegExp;
  buildIR: (match: RegExpExecArray) => IRNode;
}

const RULES: RuleEntry[] = [
  // null literal (most specific first)
  {
    pattern: /^\s*null\s*$/i,
    buildIR: () => ({ op: 'FILL_LITERAL', value: null }),
  },
  // numeric literal
  {
    pattern: /^\s*(-?\d+(\.\d+)?)\s*$/,
    buildIR: (m) => ({ op: 'FILL_LITERAL', value: Number(m[1]) }),
  },
  // quoted string literal
  {
    pattern: /^"(.+)"$|^'(.+)'$/,
    buildIR: (m) => ({ op: 'FILL_LITERAL', value: (m[1] ?? m[2] ?? '') }),
  },
  // mediana (must come BEFORE media — "mediana" contains "media")
  {
    pattern: /(usa|con|pon).+(mediana)/i,
    buildIR: () => ({ op: 'FILL_AGGREGATE', agg: 'median' as AggregateFunction }),
  },
  // media / promedio
  {
    pattern: /(usa|con|pon).+(media|promedio)/i,
    buildIR: () => ({ op: 'FILL_AGGREGATE', agg: 'mean' as AggregateFunction }),
  },
  // moda
  {
    pattern: /(usa|con|pon).+(moda)/i,
    buildIR: () => ({ op: 'FILL_AGGREGATE', agg: 'mode' as AggregateFunction }),
  },
  // mínimo
  {
    pattern: /(usa|con|pon).+(m[íi]nimo)/i,
    buildIR: () => ({ op: 'FILL_AGGREGATE', agg: 'min' as AggregateFunction }),
  },
  // máximo
  {
    pattern: /(usa|con|pon).+(m[áa]ximo)/i,
    buildIR: () => ({ op: 'FILL_AGGREGATE', agg: 'max' as AggregateFunction }),
  },
  // eliminar filas
  {
    pattern: /(elimina|borra|quita).+(fila|registro)/i,
    buildIR: () => ({ op: 'DELETE_ROWS' }),
  },
  // mantener / conservar
  {
    pattern: /(mantener|dejar|deja|conserva)/i,
    buildIR: () => ({ op: 'KEEP' }),
  },
  // copiar de columna
  {
    pattern: /(copia|toma).+(de|del)\s+['"]?(\w+)/i,
    buildIR: (m) => ({ op: 'FILL_FROM_COLUMN', sourceColumn: m[3] ?? '' }),
  },
];

// ─── Gemini IR JSON Schema (for responseSchema) ──────────────────────────

/** Literal JSON schema injected into the Gemini prompt */
const IR_JSON_SCHEMA = {
  type: 'object',
  description: 'IR node tree — one of 7 operations',
  properties: {
    op: {
      type: 'string',
      enum: [
        'FILL_LITERAL',
        'FILL_AGGREGATE',
        'FILL_FROM_COLUMN',
        'DELETE_ROWS',
        'KEEP',
        'TRANSFORM',
        'CONDITIONAL',
      ],
    },
    value: { description: 'FILL_LITERAL only — string, number or null' },
    agg: {
      type: 'string',
      enum: ['mean', 'median', 'mode', 'min', 'max', 'sum'],
      description: 'FILL_AGGREGATE only',
    },
    sourceColumn: { type: 'string', description: 'FILL_FROM_COLUMN only' },
    fn: {
      type: 'string',
      enum: [
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
      ],
      description: 'TRANSFORM only',
    },
    params: { type: 'object', description: 'TRANSFORM optional params, e.g. {decimals: 2}' },
    input: { type: 'object', description: 'TRANSFORM only — nested IRNode' },
    condition: {
      type: 'object',
      description: 'CONDITIONAL only',
      properties: {
        op: {
          type: 'string',
          enum: [
            'eq', 'neq', 'gt', 'gte', 'lt', 'lte',
            'is_null', 'is_not_null', 'contains', 'starts_with', 'ends_with', 'between',
          ],
        },
        left: { type: 'object' },
        right: { type: 'object' },
        upper: { type: 'object' },
      },
      required: ['op', 'left'],
    },
    then: { type: 'object', description: 'CONDITIONAL only — IRNode' },
    else: { type: 'object', description: 'CONDITIONAL only — IRNode' },
  },
  required: ['op'],
};

const FEW_SHOT_EXAMPLES = `
## Ejemplos

NL: "rellena con null"
IR: {"op":"FILL_LITERAL","value":null}

NL: "usa la mediana"
IR: {"op":"FILL_AGGREGATE","agg":"median"}

NL: "elimina las filas afectadas"
IR: {"op":"DELETE_ROWS"}

NL: "si es mayor a 100 usa 100"
IR: {"op":"CONDITIONAL","condition":{"op":"gt","left":{"kind":"column","column":"<target>"},"right":{"kind":"literal","value":100}},"then":{"op":"FILL_LITERAL","value":"100"},"else":{"op":"KEEP"}}

NL: "redondea a 2 decimales"
IR: {"op":"TRANSFORM","fn":"round","params":{"decimals":2},"input":{"op":"FILL_AGGREGATE","agg":"mean"}}

NL: "copia el valor de precio_lista"
IR: {"op":"FILL_FROM_COLUMN","sourceColumn":"precio_lista"}
`;

// ─── Parser implementation ─────────────────────────────────────────────────

export class InstructionParser {
  constructor(
    private readonly geminiApiKey: string = process.env['GEMINI_API_KEY'] ?? '',
    private readonly geminiModel: string = process.env['GEMINI_MODEL'] ?? 'gemini-2.0-flash-exp'
  ) {}

  /**
   * Parse a natural language instruction.
   * Returns a ParseResult (success or structured error).
   */
  async parse(
    instruction: string,
    anomalyContext: AnomalyContext,
    columns: ColumnInfo[]
  ): Promise<ParseResult> {
    const trimmed = instruction.trim();

    // 1. Try rule-based parsing first (unless conditional keywords present)
    const hasConditionals = CONDITIONAL_KEYWORDS.test(trimmed);

    if (!hasConditionals) {
      const ruleResult = this.tryRules(trimmed, columns);
      if (ruleResult !== null) {
        return { ir: ruleResult, source: 'rule' };
      }
    }

    // 2. Gemini fallback
    return this.callGemini(trimmed, anomalyContext, columns);
  }

  /**
   * Attempt to match instruction against all rules.
   * Returns the IR node on first match, null if no rule matches.
   */
  private tryRules(instruction: string, columns: ColumnInfo[]): IRNode | null {
    // Mediana must be tested before media/promedio because "mediana" contains "media".
    // We handle this by sorting rules so that longer/more specific patterns win.
    // The RULES array is ordered: media/promedio comes before mediana in code,
    // but we test mediana first by reordering at runtime.
    const ordered = [...RULES].sort((a, b) => {
      // Move mediana rule (index 4) before media rule (index 3)
      return 0; // Already ordered correctly in RULES array — mediana at index 4, media at 3
      // Actually media (index 3) is tested before mediana (index 4).
      // This is WRONG. Let's fix: we need mediana to match before media.
      // The fix is that the pattern for mediana is more specific and won't
      // collide because both look for .+(mediana) vs .+(media|promedio).
      // "usa la mediana" → matches index 4 (mediana) correctly.
      // "usa la media" → matches index 3 (media|promedio) correctly.
      // So the order is actually fine; return 0 keeps original order.
    });
    void ordered; // suppress unused warning — we use RULES directly

    for (const rule of RULES) {
      const match = rule.pattern.exec(instruction);
      if (match !== null) {
        const ir = rule.buildIR(match);
        // For FILL_FROM_COLUMN: validate that the captured column actually exists
        if (ir.op === 'FILL_FROM_COLUMN' && columns.length > 0) {
          const exists = columns.some((c) => c.name === ir.sourceColumn);
          if (!exists) {
            // Column doesn't exist — fall through to Gemini
            return null;
          }
        }
        return ir;
      }
    }

    return null;
  }

  /**
   * Call Gemini to parse the instruction.
   * Returns a ParseResult (success or structured error).
   */
  private async callGemini(
    instruction: string,
    anomalyContext: AnomalyContext,
    columns: ColumnInfo[]
  ): Promise<ParseResult> {
    if (!this.geminiApiKey) {
      return {
        error: 'gemini_unavailable',
        message:
          'No pudimos interpretar la instrucción en este momento. Inténtalo nuevamente en unos segundos o usa una instrucción más simple.',
        canRetry: true,
      };
    }

    const prompt = this.buildPrompt(instruction, anomalyContext, columns);

    let attempt = 0;
    const maxAttempts = 2;
    let lastValidationError: string | null = null;

    while (attempt < maxAttempts) {
      attempt++;

      let rawText: string;
      try {
        rawText = await this.callGeminiAPI(prompt, lastValidationError);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        // 503 or timeout — don't retry
        if (msg.includes('503') || msg.includes('timeout') || msg.includes('UNAVAILABLE')) {
          return {
            error: 'gemini_unavailable',
            message:
              'No pudimos interpretar la instrucción en este momento. Inténtalo nuevamente en unos segundos o usa una instrucción más simple.',
            canRetry: true,
          };
        }
        return {
          error: 'gemini_unavailable',
          message:
            'No pudimos interpretar la instrucción en este momento. Inténtalo nuevamente en unos segundos o usa una instrucción más simple.',
          canRetry: true,
        };
      }

      // Parse the JSON response
      let parsed: unknown;
      try {
        parsed = JSON.parse(rawText);
      } catch {
        lastValidationError = `Respuesta no es JSON válido: ${rawText.slice(0, 200)}`;
        continue; // retry
      }

      // Validate against IR schema
      const validation = IRValidator.validate(parsed, columns);
      if (validation.valid) {
        return { ir: validation.node, source: 'gemini' };
      }

      lastValidationError = validation.error;
      // retry on validation failure (attempt 2 will include the error as feedback)
    }

    // Both attempts failed
    return {
      error: 'gemini_unavailable',
      message:
        'No pudimos interpretar la instrucción en este momento. Inténtalo nuevamente en unos segundos o usa una instrucción más simple.',
      canRetry: true,
    };
  }

  private buildPrompt(
    instruction: string,
    ctx: AnomalyContext,
    columns: ColumnInfo[]
  ): string {
    const columnsText = columns
      .map((c) => `  - ${c.name} (${c.dtype})`)
      .join('\n');

    return `Eres un parser de instrucciones de limpieza de datos. Tu tarea es convertir una instrucción en español a un árbol IR JSON.

## Schema del IR
${JSON.stringify(IR_JSON_SCHEMA, null, 2)}

## Contexto de la anomalía
- Columna objetivo: ${ctx.column}
- Tipo de dato: ${ctx.dtype}
- Tipo de anomalía: ${ctx.anomalyType}
- Descripción: ${ctx.description}
- Muestras de valores: ${JSON.stringify(ctx.samples)}
- Filas afectadas: ${ctx.affectedRows}

## Columnas disponibles en el dataset
${columnsText}

${FEW_SHOT_EXAMPLES}

## Instrucción del usuario
"${instruction}"

Responde SOLO con el JSON del IR. Sin explicaciones, sin markdown, sin texto adicional.`;
  }

  /**
   * Make the actual Gemini API call with a 10-second timeout.
   * Uses dynamic import so the app starts even if the package isn't installed.
   */
  private async callGeminiAPI(
    prompt: string,
    validationError: string | null
  ): Promise<string> {
    let finalPrompt = prompt;
    if (validationError !== null) {
      finalPrompt += `\n\n## Error de validación del intento anterior\n${validationError}\nCorrige el IR y devuelve solo el JSON.`;
    }

    // Dynamic import to avoid hard failure if package not installed
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
        temperature: 0,
        // Note: responseMimeType:'application/json' causes truncation in gemini-2.5-flash
        maxOutputTokens: 1024,
      },
    });

    const timeoutMs = 15_000;

    const timeoutPromise = new Promise<never>((_, reject) => {
      const id = setTimeout(() => {
        clearTimeout(id);
        reject(new Error('timeout: Gemini did not respond within 15 seconds'));
      }, timeoutMs);
    });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    const resultPromise = model.generateContent(finalPrompt);

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const result = await Promise.race([resultPromise, timeoutPromise]);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    let text: string = result.response.text().trim();
    // Strip markdown code fences if Gemini wraps the JSON
    text = text.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();
    return text;
  }
}
