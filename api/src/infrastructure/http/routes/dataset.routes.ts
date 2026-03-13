import { Router } from 'express';
import multer from 'multer';
import type { Container } from '../../config/container.js';
import { DatasetController } from '../controllers/DatasetController.js';
import { MAX_FILE_SIZE } from '../schemas/dataset.schema.js';

/**
 * Configure multer for memory storage.
 * Files are stored in memory as Buffer objects.
 */
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: MAX_FILE_SIZE,
  },
});

/**
 * Create dataset routes.
 * All routes are prefixed with /api/v1/datasets
 */
export function createDatasetRoutes(container: Container): Router {
  const router = Router();
  const controller = new DatasetController(container);

  // POST /api/v1/datasets - Upload new dataset
  router.post('/', upload.single('file'), (req, res, next) => {
    controller.create(req, res, next).catch(next);
  });

  // GET /api/v1/datasets - List datasets
  router.get('/', (req, res, next) => {
    controller.list(req, res, next).catch(next);
  });

  // GET /api/v1/datasets/:id/download - Get signed download URL
  router.get('/:id/download', (req, res, next) => {
    controller.download(req, res, next).catch(next);
  });

  // POST /api/v1/datasets/:id/transform - Trigger a transformation job
  router.post('/:id/transform', (req, res, next) => {
    controller.transform(req, res, next).catch(next);
  });

  // GET /api/v1/datasets/:id - Get dataset by ID
  router.get('/:id', (req, res, next) => {
    controller.getById(req, res, next).catch(next);
  });

  // DELETE /api/v1/datasets/:id - Delete dataset
  router.delete('/:id', (req, res, next) => {
    controller.delete(req, res, next).catch(next);
  });

  return router;
}
