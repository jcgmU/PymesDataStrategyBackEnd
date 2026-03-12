import { describe, test, expect } from 'vitest'
import { User, type UserRole } from '../User.js'

describe('User', () => {
  const createValidProps = () => ({
    id: 'user-123',
    email: 'test@example.com',
    name: 'Test User',
    role: 'USER' as UserRole,
    preferences: { theme: 'dark' },
  })

  describe('create', () => {
    test('should create user with provided properties', () => {
      const props = createValidProps()

      const user = User.create(props)

      expect(user.id).toBe('user-123')
      expect(user.email).toBe('test@example.com')
      expect(user.name).toBe('Test User')
      expect(user.role).toBe('USER')
    })

    test('should set createdAt and updatedAt to current date', () => {
      const before = new Date()
      const user = User.create(createValidProps())
      const after = new Date()

      expect(user.createdAt.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(user.createdAt.getTime()).toBeLessThanOrEqual(after.getTime())
    })

    test('should set lastLoginAt to null initially', () => {
      const user = User.create(createValidProps())

      expect(user.lastLoginAt).toBeNull()
    })
  })

  describe('reconstitute', () => {
    test('should reconstitute user with all properties', () => {
      const props = {
        ...createValidProps(),
        createdAt: new Date('2024-01-01'),
        updatedAt: new Date('2024-01-02'),
        lastLoginAt: new Date('2024-01-03'),
      }

      const user = User.reconstitute(props)

      expect(user.lastLoginAt).toEqual(new Date('2024-01-03'))
    })
  })

  describe('isAdmin', () => {
    test('should return true for ADMIN role', () => {
      const user = User.create({ ...createValidProps(), role: 'ADMIN' })

      expect(user.isAdmin()).toBe(true)
    })

    test('should return false for USER role', () => {
      const user = User.create({ ...createValidProps(), role: 'USER' })

      expect(user.isAdmin()).toBe(false)
    })

    test('should return false for VIEWER role', () => {
      const user = User.create({ ...createValidProps(), role: 'VIEWER' })

      expect(user.isAdmin()).toBe(false)
    })
  })

  describe('canEdit', () => {
    test('should return true for ADMIN role', () => {
      const user = User.create({ ...createValidProps(), role: 'ADMIN' })

      expect(user.canEdit()).toBe(true)
    })

    test('should return true for USER role', () => {
      const user = User.create({ ...createValidProps(), role: 'USER' })

      expect(user.canEdit()).toBe(true)
    })

    test('should return false for VIEWER role', () => {
      const user = User.create({ ...createValidProps(), role: 'VIEWER' })

      expect(user.canEdit()).toBe(false)
    })
  })

  describe('recordLogin', () => {
    test('should set lastLoginAt to current date', () => {
      const user = User.create(createValidProps())
      expect(user.lastLoginAt).toBeNull()

      const before = new Date()
      user.recordLogin()
      const after = new Date()

      expect(user.lastLoginAt).not.toBeNull()
      expect(user.lastLoginAt!.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(user.lastLoginAt!.getTime()).toBeLessThanOrEqual(after.getTime())
    })

    test('should update updatedAt', () => {
      const user = User.create(createValidProps())
      const initialUpdatedAt = user.updatedAt

      user.recordLogin()

      expect(user.updatedAt.getTime()).toBeGreaterThanOrEqual(initialUpdatedAt.getTime())
    })
  })

  describe('updatePreferences', () => {
    test('should update preferences', () => {
      const user = User.create(createValidProps())
      const newPrefs = { theme: 'light', language: 'es' }

      user.updatePreferences(newPrefs)

      expect(user.preferences).toEqual(newPrefs)
    })

    test('should return immutable copy of preferences', () => {
      const user = User.create(createValidProps())

      const prefs1 = user.preferences
      const prefs2 = user.preferences

      expect(prefs1).toEqual(prefs2)
      expect(prefs1).not.toBe(prefs2)
    })
  })
})
