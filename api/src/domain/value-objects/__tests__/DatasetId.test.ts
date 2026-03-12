import { describe, test, expect } from 'vitest'
import { DatasetId } from '../DatasetId.js'

describe('DatasetId', () => {
  describe('generate', () => {
    test('should generate unique IDs', () => {
      const id1 = DatasetId.generate()
      const id2 = DatasetId.generate()

      expect(id1.toString()).not.toBe(id2.toString())
    })

    test('should generate valid UUID format', () => {
      const id = DatasetId.generate()
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

      expect(id.toString()).toMatch(uuidRegex)
    })
  })

  describe('fromString', () => {
    test('should create DatasetId from valid string', () => {
      const idString = '123e4567-e89b-12d3-a456-426614174000'

      const id = DatasetId.fromString(idString)

      expect(id.toString()).toBe(idString)
    })

    test('should throw error for empty string', () => {
      expect(() => DatasetId.fromString('')).toThrow('DatasetId cannot be empty')
    })

    test('should throw error for whitespace-only string', () => {
      expect(() => DatasetId.fromString('   ')).toThrow('DatasetId cannot be empty')
    })
  })

  describe('equals', () => {
    test('should return true for same ID', () => {
      const idString = '123e4567-e89b-12d3-a456-426614174000'
      const id1 = DatasetId.fromString(idString)
      const id2 = DatasetId.fromString(idString)

      expect(id1.equals(id2)).toBe(true)
    })

    test('should return false for different IDs', () => {
      const id1 = DatasetId.generate()
      const id2 = DatasetId.generate()

      expect(id1.equals(id2)).toBe(false)
    })
  })

  describe('toString', () => {
    test('should return the ID string value', () => {
      const idString = 'custom-id-123'
      const id = DatasetId.fromString(idString)

      expect(id.toString()).toBe(idString)
    })
  })
})
