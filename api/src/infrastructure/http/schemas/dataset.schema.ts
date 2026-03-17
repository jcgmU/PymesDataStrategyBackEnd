import { z } from 'zod';

/**
 * Schema for creating a dataset via file upload.
 * The file itself is handled by multer, these are the additional fields.
 */
export const createDatasetSchema = z.object({
  name: z
    .string()
    .min(1, 'Name is required')
    .max(255, 'Name must be at most 255 characters'),
  description: z
    .string()
    .max(1000, 'Description must be at most 1000 characters')
    .optional(),
  metadata: z
    .string()
    .optional()
    .transform((val) => {
      if (val === undefined || val === '') return undefined;
      try {
        return JSON.parse(val) as Record<string, unknown>;
      } catch {
        return undefined;
      }
    }),
});

export type CreateDatasetDto = z.infer<typeof createDatasetSchema>;

/**
 * Allowed MIME types for dataset uploads.
 */
export const ALLOWED_MIME_TYPES = [
  'text/csv',
  'application/vnd.ms-excel',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/json',
  'text/plain',
] as const;

/**
 * Maximum file size: 100MB
 */
export const MAX_FILE_SIZE = 100 * 1024 * 1024;

/**
 * Validate that the uploaded file has an allowed MIME type.
 */
export function isAllowedMimeType(mimeType: string): boolean {
  return ALLOWED_MIME_TYPES.includes(mimeType as typeof ALLOWED_MIME_TYPES[number]);
}
