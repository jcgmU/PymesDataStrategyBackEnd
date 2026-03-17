import type { Request, Response, NextFunction } from 'express';
import type { Container } from '../../config/container.js';
import { RegisterUseCase } from '../../../application/use-cases/RegisterUseCase.js';
import { LoginUseCase } from '../../../application/use-cases/LoginUseCase.js';
import { GetCurrentUserUseCase } from '../../../application/use-cases/GetCurrentUserUseCase.js';
import { UpdateUserUseCase } from '../../../application/use-cases/UpdateUserUseCase.js';
import { registerSchema, loginSchema, updateUserSchema } from '../schemas/auth.schema.js';
import { ValidationError } from '../../../domain/errors/ValidationError.js';

/**
 * Controller for authentication and user profile endpoints.
 */
export class AuthController {
  constructor(private readonly container: Container) {}

  /**
   * POST /api/v1/auth/register
   */
  async register(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const parseResult = registerSchema.safeParse(req.body);
      if (!parseResult.success) {
        const firstError = parseResult.error.errors[0];
        throw new ValidationError(
          firstError?.message ?? 'Invalid request body',
          firstError?.path.join('.') ?? 'body'
        );
      }

      const useCase = new RegisterUseCase(
        this.container.userRepository,
        this.container.passwordService
      );

      const result = await useCase.execute(parseResult.data);

      res.status(201).json({
        success: true,
        data: result,
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * POST /api/v1/auth/login
   */
  async login(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const parseResult = loginSchema.safeParse(req.body);
      if (!parseResult.success) {
        const firstError = parseResult.error.errors[0];
        throw new ValidationError(
          firstError?.message ?? 'Invalid request body',
          firstError?.path.join('.') ?? 'body'
        );
      }

      const useCase = new LoginUseCase(
        this.container.userRepository,
        this.container.passwordService,
        this.container.jwtService
      );

      const result = await useCase.execute(parseResult.data);

      res.json({
        success: true,
        data: result,
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * GET /api/v1/users/me
   * Requires auth middleware — req.userId is set.
   */
  async getMe(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const userId = req.userId ?? '';
      if (userId === '') {
        throw new ValidationError('User ID not found in request', 'userId');
      }

      const useCase = new GetCurrentUserUseCase(this.container.userRepository);
      const result = await useCase.execute(userId);

      res.json({
        success: true,
        data: result,
      });
    } catch (error) {
      next(error);
    }
  }

  /**
   * PATCH /api/v1/users/me
   * Requires auth middleware — req.userId is set.
   */
  async updateMe(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const userId = req.userId ?? '';
      if (userId === '') {
        throw new ValidationError('User ID not found in request', 'userId');
      }

      const parseResult = updateUserSchema.safeParse(req.body);
      if (!parseResult.success) {
        const firstError = parseResult.error.errors[0];
        throw new ValidationError(
          firstError?.message ?? 'Invalid request body',
          firstError?.path.join('.') ?? 'body'
        );
      }

      const useCase = new UpdateUserUseCase(this.container.userRepository);
      const { name, email } = parseResult.data;
      const result = await useCase.execute({
        userId,
        ...(name !== undefined ? { name } : {}),
        ...(email !== undefined ? { email } : {}),
      });

      res.json({
        success: true,
        data: result,
      });
    } catch (error) {
      next(error);
    }
  }
}
