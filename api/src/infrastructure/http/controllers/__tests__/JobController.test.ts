import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Request, Response, NextFunction } from 'express';
import { JobController } from '../JobController.js';
import type { Container } from '../../../config/container.js';
import { ValidationError } from '../../../../domain/errors/ValidationError.js';

describe('JobController', () => {
  let controller: JobController;
  let mockContainer: Container;
  let mockReq: Partial<Request>;
  let mockRes: Partial<Response>;
  let mockNext: NextFunction;

  const mockJobQueue = {
    enqueue: vi.fn(),
    getStatus: vi.fn(),
    cancel: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();

    mockContainer = {
      jobQueue: mockJobQueue,
    } as unknown as Container;

    controller = new JobController(mockContainer);

    mockRes = {
      status: vi.fn().mockReturnThis(),
      json: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
    };

    mockNext = vi.fn();
  });

  describe('getStatus', () => {
    it('should return job status when job exists', async () => {
      mockReq = { params: { jobId: 'job-abc-123' } };

      mockJobQueue.getStatus.mockResolvedValue({
        jobId: 'job-abc-123',
        status: 'processing',
      });

      await controller.getStatus(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.json).toHaveBeenCalledWith({
        success: true,
        data: { jobId: 'job-abc-123', status: 'processing' },
      });
      expect(mockNext).not.toHaveBeenCalled();
    });

    it('should return 404 when job is not found', async () => {
      mockReq = { params: { jobId: 'non-existent' } };

      mockJobQueue.getStatus.mockResolvedValue(null);

      await controller.getStatus(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.status).toHaveBeenCalledWith(404);
      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          success: false,
          error: expect.objectContaining({ code: 'NOT_FOUND' }),
        })
      );
    });

    it('should throw ValidationError when jobId is missing', async () => {
      mockReq = { params: {} };

      await controller.getStatus(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(ValidationError));
    });

    it('should return queued status', async () => {
      mockReq = { params: { jobId: 'job-queued' } };

      mockJobQueue.getStatus.mockResolvedValue({
        jobId: 'job-queued',
        status: 'queued',
      });

      await controller.getStatus(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          success: true,
          data: expect.objectContaining({ status: 'queued' }),
        })
      );
    });

    it('should return completed status', async () => {
      mockReq = { params: { jobId: 'job-done' } };

      mockJobQueue.getStatus.mockResolvedValue({
        jobId: 'job-done',
        status: 'completed',
      });

      await controller.getStatus(mockReq as Request, mockRes as Response, mockNext);

      expect(mockRes.json).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ status: 'completed' }),
        })
      );
    });

    it('should call next with error on unexpected failure', async () => {
      mockReq = { params: { jobId: 'job-error' } };

      const unexpectedError = new Error('Redis connection lost');
      mockJobQueue.getStatus.mockRejectedValue(unexpectedError);

      await controller.getStatus(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledWith(unexpectedError);
    });
  });
});
