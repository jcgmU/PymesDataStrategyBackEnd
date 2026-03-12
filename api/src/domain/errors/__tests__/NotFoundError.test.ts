import { describe, it, expect } from 'vitest';
import { NotFoundError } from '../NotFoundError.js';
import { DomainError } from '../DomainError.js';

describe('NotFoundError', () => {
  describe('constructor', () => {
    it('should create error with entity type and id', () => {
      const error = new NotFoundError('Dataset', '123e4567-e89b-12d3-a456-426614174000');

      expect(error.entityType).toBe('Dataset');
      expect(error.entityId).toBe('123e4567-e89b-12d3-a456-426614174000');
      expect(error.code).toBe('NOT_FOUND');
      expect(error.name).toBe('NotFoundError');
    });

    it('should generate proper message from entity type and id', () => {
      const error = new NotFoundError('User', 'user-456');

      expect(error.message).toBe("User with id 'user-456' not found");
    });

    it('should be instance of DomainError', () => {
      const error = new NotFoundError('Entity', 'id');

      expect(error).toBeInstanceOf(DomainError);
      expect(error).toBeInstanceOf(Error);
    });
  });

  describe('toJSON', () => {
    it('should serialize error with all properties', () => {
      const error = new NotFoundError('Dataset', 'ds-789');
      const json = error.toJSON();

      expect(json).toEqual({
        code: 'NOT_FOUND',
        message: "Dataset with id 'ds-789' not found",
        name: 'NotFoundError',
        entityType: 'Dataset',
        entityId: 'ds-789',
      });
    });

    it('should include all required fields', () => {
      const error = new NotFoundError('TransformationJob', 'job-001');
      const json = error.toJSON();

      expect(json).toHaveProperty('code');
      expect(json).toHaveProperty('message');
      expect(json).toHaveProperty('name');
      expect(json).toHaveProperty('entityType');
      expect(json).toHaveProperty('entityId');
    });
  });

  describe('stack trace', () => {
    it('should have proper stack trace', () => {
      const error = new NotFoundError('Resource', 'res-123');

      expect(error.stack).toBeDefined();
      expect(error.stack).toContain('NotFoundError');
    });
  });

  describe('common entity scenarios', () => {
    it('should handle Dataset not found', () => {
      const error = new NotFoundError('Dataset', 'abc-123');

      expect(error.message).toBe("Dataset with id 'abc-123' not found");
      expect(error.entityType).toBe('Dataset');
    });

    it('should handle User not found', () => {
      const error = new NotFoundError('User', 'usr-456');

      expect(error.message).toBe("User with id 'usr-456' not found");
      expect(error.entityType).toBe('User');
    });

    it('should handle TransformationJob not found', () => {
      const error = new NotFoundError('TransformationJob', 'job-789');

      expect(error.message).toBe("TransformationJob with id 'job-789' not found");
      expect(error.entityType).toBe('TransformationJob');
    });

    it('should handle empty id', () => {
      const error = new NotFoundError('Entity', '');

      expect(error.message).toBe("Entity with id '' not found");
      expect(error.entityId).toBe('');
    });
  });
});
