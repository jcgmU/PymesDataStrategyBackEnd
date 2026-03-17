import type { ErrorRequestHandler } from 'express';
import { DomainError } from '../../../domain/errors/DomainError.js';
import { ValidationError } from '../../../domain/errors/ValidationError.js';
import { NotFoundError } from '../../../domain/errors/NotFoundError.js';
import { pino } from 'pino';

const logger = pino({ name: 'error-handler' });

interface ErrorResponse {
  error: {
    code: string;
    message: string;
    details?: Record<string, unknown> | undefined;
  };
}

/**
 * Global error handler middleware.
 * Converts errors to consistent JSON responses.
 */
export const errorHandler: ErrorRequestHandler = (err, _req, res, _next): void => {
  // Log the error
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
  logger.error({ err }, 'Request error');

  // Handle domain errors
  if (err instanceof ValidationError) {
    const response: ErrorResponse = {
      error: {
        code: err.code,
        message: err.message,
        details: err.field !== undefined ? { field: err.field } : undefined,
      },
    };
    res.status(400).json(response);
    return;
  }

  if (err instanceof NotFoundError) {
    const response: ErrorResponse = {
      error: {
        code: err.code,
        message: err.message,
        details: {
          entityType: err.entityType,
          entityId: err.entityId,
        },
      },
    };
    res.status(404).json(response);
    return;
  }

  if (err instanceof DomainError) {
    const response: ErrorResponse = {
      error: {
        code: err.code,
        message: err.message,
      },
    };
    res.status(400).json(response);
    return;
  }

  // Handle unknown errors
  const isProduction = process.env['NODE_ENV'] === 'production';
  const response: ErrorResponse = {
    error: {
      code: 'INTERNAL_ERROR',
      message: isProduction ? 'An unexpected error occurred' : String(err),
    },
  };

  res.status(500).json(response);
};
