import jwt from 'jsonwebtoken';
import type { IJwtService, JwtPayload } from '../../domain/ports/services/JwtService.js';
import { ValidationError } from '../../domain/errors/ValidationError.js';

/**
 * JWT service adapter using the jsonwebtoken library.
 */
export class JwtServiceAdapter implements IJwtService {
  constructor(
    private readonly secret: string,
    private readonly expiresIn: string
  ) {}

  sign(payload: JwtPayload): string {
    return jwt.sign(payload, this.secret, { expiresIn: this.expiresIn } as jwt.SignOptions);
  }

  verify(token: string): JwtPayload {
    try {
      const decoded = jwt.verify(token, this.secret);
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      if (typeof decoded !== 'object' || decoded === null) {
        throw new ValidationError('Invalid token payload');
      }
      const { userId, email } = decoded as Record<string, unknown>;
      if (typeof userId !== 'string' || typeof email !== 'string') {
        throw new ValidationError('Invalid token payload structure');
      }
      return { userId, email };
    } catch (error) {
      if (error instanceof ValidationError) {
        throw error;
      }
      throw new ValidationError('Invalid or expired token');
    }
  }
}
