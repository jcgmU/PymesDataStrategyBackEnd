import { defineConfig } from 'vitest/config'
import path from 'path'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/**/*.{test,spec}.ts', 'tests/**/*.{test,spec}.ts'],
    exclude: ['**/node_modules/**', '**/dist/**'],

    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      reportsDirectory: './coverage',
      include: ['src/**/*.ts'],
      exclude: [
        'src/**/*.test.ts',
        'src/**/*.spec.ts',
        'src/**/index.ts',
        'src/**/*.d.ts',
        // Wiring / bootstrap files — not unit-testable by design
        'src/infrastructure/http/server.ts',
        'src/infrastructure/http/swagger.ts',
        'src/infrastructure/http/routes/**',
        'src/infrastructure/http/middleware/errorHandler.ts',
        'src/infrastructure/config/container.ts',
        'src/infrastructure/persistence/prisma/client.ts',
        // Port interfaces — pure TypeScript interfaces, no runtime logic
        'src/domain/ports/**',
        // Entity not yet used in unit tests
        'src/domain/entities/TransformationJob.ts',
        // Auth adapter with minimal testable logic outside integration
        'src/infrastructure/auth/BcryptPasswordService.ts',
      ],
      thresholds: {
        lines: 70,
        functions: 75,
        branches: 80,
        statements: 70,
      },
    },

    clearMocks: true,
    restoreMocks: true,
    mockReset: true,

    testTimeout: 60000,
    hookTimeout: 60000,
  },

  resolve: {
    alias: {
      '@domain': path.resolve(__dirname, './src/domain'),
      '@application': path.resolve(__dirname, './src/application'),
      '@infrastructure': path.resolve(__dirname, './src/infrastructure'),
    },
  },
})
