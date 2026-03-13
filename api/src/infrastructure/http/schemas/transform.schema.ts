import { z } from 'zod';

/**
 * Valid transformation types — must match the domain TransformationType.
 */
export const TRANSFORMATION_TYPES = [
  'CLEAN_NULLS',
  'NORMALIZE',
  'AGGREGATE',
  'FILTER',
  'MERGE',
  'CUSTOM',
] as const;

/**
 * Schema for triggering a dataset transformation.
 */
export const transformDatasetSchema = z.object({
  transformationType: z.enum(TRANSFORMATION_TYPES, {
    errorMap: () => ({
      message: `transformationType must be one of: ${TRANSFORMATION_TYPES.join(', ')}`,
    }),
  }),
  parameters: z
    .record(z.unknown())
    .optional()
    .default({}),
  priority: z
    .number()
    .int()
    .min(1)
    .max(10)
    .optional()
    .default(5),
});

export type TransformDatasetDto = z.infer<typeof transformDatasetSchema>;
