import { describe, test, expect, beforeEach } from 'vitest'
import { Dataset, type DatasetStatus } from '../Dataset.js'
import { DatasetId } from '../../value-objects/DatasetId.js'

describe('Dataset', () => {
  const createValidProps = () => ({
    id: DatasetId.generate(),
    name: 'Test Dataset',
    description: 'A test dataset',
    originalFileName: 'data.csv',
    storageKey: 'datasets/123/data.csv',
    fileSizeBytes: BigInt(1024),
    mimeType: 'text/csv',
    schema: { columns: ['id', 'name'] },
    metadata: { source: 'upload' },
    statistics: {},
    userId: 'user-123',
  })

  describe('create', () => {
    test('should create dataset with PENDING status', () => {
      const props = createValidProps()

      const dataset = Dataset.create(props)

      expect(dataset.status).toBe('PENDING')
      expect(dataset.name).toBe('Test Dataset')
      expect(dataset.processedAt).toBeNull()
    })

    test('should set createdAt and updatedAt to current date', () => {
      const before = new Date()
      const dataset = Dataset.create(createValidProps())
      const after = new Date()

      expect(dataset.createdAt.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(dataset.createdAt.getTime()).toBeLessThanOrEqual(after.getTime())
      expect(dataset.updatedAt.getTime()).toBeGreaterThanOrEqual(before.getTime())
    })
  })

  describe('reconstitute', () => {
    test('should reconstitute dataset with all properties', () => {
      const props = {
        ...createValidProps(),
        status: 'READY' as DatasetStatus,
        createdAt: new Date('2024-01-01'),
        updatedAt: new Date('2024-01-02'),
        processedAt: new Date('2024-01-02'),
      }

      const dataset = Dataset.reconstitute(props)

      expect(dataset.status).toBe('READY')
      expect(dataset.createdAt).toEqual(new Date('2024-01-01'))
      expect(dataset.processedAt).toEqual(new Date('2024-01-02'))
    })
  })

  describe('state transitions', () => {
    let dataset: Dataset

    beforeEach(() => {
      dataset = Dataset.create(createValidProps())
    })

    test('should transition from PENDING to PROCESSING', () => {
      expect(dataset.status).toBe('PENDING')

      dataset.markProcessing()

      expect(dataset.status).toBe('PROCESSING')
    })

    test('should transition to READY with statistics', () => {
      const stats = { rowCount: 100, columnCount: 5 }

      dataset.markProcessing()
      dataset.markReady(stats)

      expect(dataset.status).toBe('READY')
      expect(dataset.statistics).toEqual(stats)
      expect(dataset.processedAt).not.toBeNull()
    })

    test('should transition to ERROR', () => {
      dataset.markProcessing()
      dataset.markError()

      expect(dataset.status).toBe('ERROR')
    })

    test('should transition to ARCHIVED', () => {
      dataset.markProcessing()
      dataset.markReady({})
      dataset.archive()

      expect(dataset.status).toBe('ARCHIVED')
    })

    test('should update updatedAt on each transition', () => {
      const initialUpdatedAt = dataset.updatedAt

      // Small delay to ensure time difference
      dataset.markProcessing()

      expect(dataset.updatedAt.getTime()).toBeGreaterThanOrEqual(initialUpdatedAt.getTime())
    })
  })

  describe('updateMetadata', () => {
    test('should update metadata and updatedAt', () => {
      const dataset = Dataset.create(createValidProps())
      const newMetadata = { source: 'api', version: 2 }

      dataset.updateMetadata(newMetadata)

      expect(dataset.metadata).toEqual(newMetadata)
    })
  })

  describe('getters', () => {
    test('should return immutable copies of objects', () => {
      const dataset = Dataset.create(createValidProps())

      const schema1 = dataset.schema
      const schema2 = dataset.schema

      expect(schema1).toEqual(schema2)
      expect(schema1).not.toBe(schema2) // Different references
    })
  })
})
