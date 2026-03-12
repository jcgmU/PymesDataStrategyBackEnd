import { describe, test, expect } from 'vitest'
import { Email } from '../Email.js'

describe('Email', () => {
  describe('create', () => {
    test('should create email with valid format', () => {
      const email = Email.create('test@example.com')

      expect(email.toString()).toBe('test@example.com')
    })

    test('should normalize email to lowercase', () => {
      const email = Email.create('Test@EXAMPLE.COM')

      expect(email.toString()).toBe('test@example.com')
    })

    test('should trim whitespace', () => {
      const email = Email.create('  test@example.com  ')

      expect(email.toString()).toBe('test@example.com')
    })

    test('should throw error for invalid email without @', () => {
      expect(() => Email.create('invalid-email')).toThrow('Invalid email format')
    })

    test('should throw error for email without domain', () => {
      expect(() => Email.create('test@')).toThrow('Invalid email format')
    })

    test('should throw error for email without local part', () => {
      expect(() => Email.create('@example.com')).toThrow('Invalid email format')
    })

    test('should throw error for email with spaces', () => {
      expect(() => Email.create('test @example.com')).toThrow('Invalid email format')
    })

    test('should throw error for empty string', () => {
      expect(() => Email.create('')).toThrow('Invalid email format')
    })
  })

  describe('fromString', () => {
    test('should be alias for create', () => {
      const email = Email.fromString('test@example.com')

      expect(email.toString()).toBe('test@example.com')
    })
  })

  describe('equals', () => {
    test('should return true for same email', () => {
      const email1 = Email.create('test@example.com')
      const email2 = Email.create('test@example.com')

      expect(email1.equals(email2)).toBe(true)
    })

    test('should return true for same email different case', () => {
      const email1 = Email.create('test@example.com')
      const email2 = Email.create('TEST@EXAMPLE.COM')

      expect(email1.equals(email2)).toBe(true)
    })

    test('should return false for different emails', () => {
      const email1 = Email.create('test@example.com')
      const email2 = Email.create('other@example.com')

      expect(email1.equals(email2)).toBe(false)
    })
  })

  describe('getDomain', () => {
    test('should return domain part', () => {
      const email = Email.create('test@example.com')

      expect(email.getDomain()).toBe('example.com')
    })

    test('should return subdomain', () => {
      const email = Email.create('test@mail.example.com')

      expect(email.getDomain()).toBe('mail.example.com')
    })
  })
})
