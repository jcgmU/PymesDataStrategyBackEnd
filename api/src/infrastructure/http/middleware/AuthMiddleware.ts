import type { Request, Response, NextFunction } from 'express';
import type { IJwtService } from '../../../domain/ports/services/JwtService.js';

// Extend Express Request to include userId
declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      userId?: string;
    }
  }
}

/**
 * Middleware that validates the Authorization: Bearer <token> header.
 * Attaches req.userId if the token is valid.
 * Returns 401 if the token is missing or invalid.
 */
export function createAuthMiddleware(jwtService: IJwtService) {
  return function authMiddleware(req: Request, res: Response, next: NextFunction): void {
    const authHeader = req.headers.authorization;

    if (authHeader?.startsWith('Bearer ') !== true) {
      res.status(401).json({
        error: {
          code: 'UNAUTHORIZED',
          message: 'Authorization header is required',
        },
      });
      return;
    }

    const token = authHeader.slice(7); // Remove "Bearer " prefix

    try {
      const payload = jwtService.verify(token);
      req.userId = payload.userId;
      next();
    } catch {
      res.status(401).json({
        error: {
          code: 'UNAUTHORIZED',
          message: 'Invalid or expired token',
        },
      });
    }
  };
}
