import { describe, it, expect } from 'vitest';
import { ValidationError } from '../ValidationError.js';
import { DomainError } from '../DomainError.js';

describe('ValidationError', () => {
  describe('constructor', () => {
    it('should create error with message only', () => {
      const error = new ValidationError('Invalid input');

      expect(error.message).toBe('Invalid input');
      expect(error.field).toBeUndefined();
      expect(error.code).toBe('VALIDATION_ERROR');
      expect(error.name).toBe('ValidationError');
    });

    it('should create error with message and field', () => {
      const error = new ValidationError('Email is required', 'email');

      expect(error.message).toBe('Email is required');
      expect(error.field).toBe('email');
      expect(error.code).toBe('VALIDATION_ERROR');
    });

    it('should be instance of DomainError', () => {
      const error = new ValidationError('Test error');

      expect(error).toBeInstanceOf(DomainError);
      expect(error).toBeInstanceOf(Error);
    });
  });

  describe('toJSON', () => {
    it('should serialize error without field', () => {
      const error = new ValidationError('Invalid format');
      const json = error.toJSON();

      expect(json).toEqual({
        code: 'VALIDATION_ERROR',
        message: 'Invalid format',
        name: 'ValidationError',
      });
      expect(json).not.toHaveProperty('field');
    });

    it('should serialize error with field', () => {
      const error = new ValidationError('Must be positive', 'amount');
      const json = error.toJSON();

      expect(json).toEqual({
        code: 'VALIDATION_ERROR',
        message: 'Must be positive',
        name: 'ValidationError',
        field: 'amount',
      });
    });
  });

  describe('stack trace', () => {
    it('should have proper stack trace', () => {
      const error = new ValidationError('Stack test');

      expect(error.stack).toBeDefined();
      expect(error.stack).toContain('ValidationError');
    });
  });

  describe('common validation scenarios', () => {
    it('should handle empty field name', () => {
      const error = new ValidationError('Invalid value', '');

      expect(error.field).toBe('');
      const json = error.toJSON();
      expect(json.field).toBe('');
    });

    it('should handle nested field paths', () => {
      const error = new ValidationError('Invalid nested value', 'user.address.zipCode');

      expect(error.field).toBe('user.address.zipCode');
    });
  });
});
