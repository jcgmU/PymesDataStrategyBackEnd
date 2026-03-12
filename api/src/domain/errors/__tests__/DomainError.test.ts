import { describe, it, expect } from 'vitest';
import { DomainError } from '../DomainError.js';
import { ValidationError } from '../ValidationError.js';
import { NotFoundError } from '../NotFoundError.js';

// Concrete implementation for testing abstract class behavior
class TestDomainError extends DomainError {
  readonly code = 'TEST_ERROR';

  constructor(message: string) {
    super(message);
  }
}

describe('DomainError', () => {
  describe('abstract class behavior', () => {
    it('should not be directly instantiable (abstract)', () => {
      // TypeScript prevents direct instantiation at compile time
      // At runtime, we verify that concrete implementations work
      const error = new TestDomainError('Test message');

      expect(error).toBeInstanceOf(DomainError);
      expect(error).toBeInstanceOf(Error);
    });

    it('should require code property in subclasses', () => {
      const error = new TestDomainError('Test');

      expect(error.code).toBe('TEST_ERROR');
    });
  });

  describe('constructor', () => {
    it('should set message correctly', () => {
      const error = new TestDomainError('Custom error message');

      expect(error.message).toBe('Custom error message');
    });

    it('should set name to constructor name', () => {
      const error = new TestDomainError('Test');

      expect(error.name).toBe('TestDomainError');
    });

    it('should capture stack trace', () => {
      const error = new TestDomainError('Stack test');

      expect(error.stack).toBeDefined();
      expect(error.stack).toContain('TestDomainError');
    });
  });

  describe('toJSON', () => {
    it('should serialize to JSON with code, message, and name', () => {
      const error = new TestDomainError('Serialization test');
      const json = error.toJSON();

      expect(json).toEqual({
        code: 'TEST_ERROR',
        message: 'Serialization test',
        name: 'TestDomainError',
      });
    });

    it('should be suitable for API responses', () => {
      const error = new TestDomainError('API error');
      const json = error.toJSON();

      // Verify it can be stringified
      const stringified = JSON.stringify(json);
      const parsed = JSON.parse(stringified);

      expect(parsed.code).toBe('TEST_ERROR');
      expect(parsed.message).toBe('API error');
      expect(parsed.name).toBe('TestDomainError');
    });
  });

  describe('inheritance hierarchy', () => {
    it('ValidationError should extend DomainError', () => {
      const error = new ValidationError('Validation failed');

      expect(error).toBeInstanceOf(DomainError);
      expect(error).toBeInstanceOf(Error);
    });

    it('NotFoundError should extend DomainError', () => {
      const error = new NotFoundError('Entity', 'id-123');

      expect(error).toBeInstanceOf(DomainError);
      expect(error).toBeInstanceOf(Error);
    });

    it('should allow catching all domain errors', () => {
      const errors: DomainError[] = [
        new ValidationError('Invalid'),
        new NotFoundError('User', '123'),
        new TestDomainError('Test'),
      ];

      errors.forEach((error) => {
        expect(error).toBeInstanceOf(DomainError);
        expect(error.code).toBeDefined();
        expect(error.toJSON()).toHaveProperty('code');
      });
    });
  });

  describe('error differentiation', () => {
    it('should be distinguishable from infrastructure errors', () => {
      const domainError = new TestDomainError('Domain issue');
      const infraError = new Error('Connection failed');

      expect(domainError).toBeInstanceOf(DomainError);
      expect(infraError).not.toBeInstanceOf(DomainError);
    });

    it('should have unique codes per error type', () => {
      const validation = new ValidationError('Invalid');
      const notFound = new NotFoundError('User', '123');
      const test = new TestDomainError('Test');

      expect(validation.code).toBe('VALIDATION_ERROR');
      expect(notFound.code).toBe('NOT_FOUND');
      expect(test.code).toBe('TEST_ERROR');

      // All codes should be different
      const codes = [validation.code, notFound.code, test.code];
      expect(new Set(codes).size).toBe(3);
    });
  });
});
