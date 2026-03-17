import { describe, it, expect, vi } from 'vitest';
import type { Request, Response } from 'express';
import { HealthController } from '../controllers/HealthController.js';
import type { Container } from '../../config/container.js';

// Mock container factory
function createMockContainer(overrides: {
  database?: boolean;
  redis?: boolean;
} = {}): Container {
  return {
    checkDatabase: vi.fn().mockResolvedValue(overrides.database ?? true),
    checkRedis: vi.fn().mockResolvedValue(overrides.redis ?? true),
  } as unknown as Container;
}

// Mock request/response factory
function createMockRequestResponse() {
  const req = {} as Request;
  const res = {
    status: vi.fn().mockReturnThis(),
    json: vi.fn().mockReturnThis(),
  } as unknown as Response;
  return { req, res };
}

describe('HealthController', () => {
  describe('check', () => {
    it('should return ok status when all services are healthy', async () => {
      // Arrange
      const container = createMockContainer({ database: true, redis: true });
      const controller = new HealthController(container);
      const { req, res } = createMockRequestResponse();

      // Act
      await controller.check(req, res);

      // Assert
      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'ok',
          checks: { database: true, redis: true },
        })
      );
    });

    it('should return degraded status when some services are unhealthy', async () => {
      // Arrange
      const container = createMockContainer({ database: true, redis: false });
      const controller = new HealthController(container);
      const { req, res } = createMockRequestResponse();

      // Act
      await controller.check(req, res);

      // Assert
      expect(res.status).toHaveBeenCalledWith(503);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'degraded',
          checks: { database: true, redis: false },
        })
      );
    });

    it('should return unhealthy status when all services are down', async () => {
      // Arrange
      const container = createMockContainer({ database: false, redis: false });
      const controller = new HealthController(container);
      const { req, res } = createMockRequestResponse();

      // Act
      await controller.check(req, res);

      // Assert
      expect(res.status).toHaveBeenCalledWith(503);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'unhealthy',
          checks: { database: false, redis: false },
        })
      );
    });

    it('should include timestamp in response', async () => {
      // Arrange
      const container = createMockContainer();
      const controller = new HealthController(container);
      const { req, res } = createMockRequestResponse();

      // Act
      await controller.check(req, res);

      // Assert
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({
          timestamp: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/),
        })
      );
    });

    it('should call container health check methods', async () => {
      // Arrange
      const container = createMockContainer();
      const controller = new HealthController(container);
      const { req, res } = createMockRequestResponse();

      // Act
      await controller.check(req, res);

      // Assert
      expect(container.checkDatabase).toHaveBeenCalledTimes(1);
      expect(container.checkRedis).toHaveBeenCalledTimes(1);
    });
  });

  describe('status codes', () => {
    it('should return 200 when fully healthy', async () => {
      const container = createMockContainer({ database: true, redis: true });
      const controller = new HealthController(container);
      const { req, res } = createMockRequestResponse();

      await controller.check(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it('should return 503 when partially healthy', async () => {
      const container = createMockContainer({ database: false, redis: true });
      const controller = new HealthController(container);
      const { req, res } = createMockRequestResponse();

      await controller.check(req, res);

      expect(res.status).toHaveBeenCalledWith(503);
    });

    it('should return 503 when fully unhealthy', async () => {
      const container = createMockContainer({ database: false, redis: false });
      const controller = new HealthController(container);
      const { req, res } = createMockRequestResponse();

      await controller.check(req, res);

      expect(res.status).toHaveBeenCalledWith(503);
    });
  });
});
