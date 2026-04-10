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

  /**
   * @openapi
   * /datasets:
   *   post:
   *     tags:
   *       - Datasets
   *     summary: Upload a new dataset file
   *     description: |
   *       Accepts a multipart file upload (CSV, Excel, JSON, or TXT up to 100 MB).
   *       On success, enqueues an async parsing job and returns the dataset record
   *       with its `jobId` for polling or SSE streaming.
   *     security:
   *       - bearerAuth: []
   *     requestBody:
   *       required: true
   *       content:
   *         multipart/form-data:
   *           schema:
   *             type: object
   *             required:
   *               - file
   *               - name
   *             properties:
   *               file:
   *                 type: string
   *                 format: binary
   *                 description: Dataset file (CSV, XLS, XLSX, JSON, TXT — max 100 MB)
   *               name:
   *                 type: string
   *                 maxLength: 255
   *                 example: Sales Q1 2024
   *               description:
   *                 type: string
   *                 maxLength: 1000
   *                 example: Quarterly sales data
   *               metadata:
   *                 type: string
   *                 description: JSON-serialised key/value metadata object
   *                 example: '{"source":"CRM","region":"EU"}'
   *     responses:
   *       '201':
   *         description: Dataset created and parsing job enqueued
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 success:
   *                   type: boolean
   *                   example: true
   *                 data:
   *                   type: object
   *                   properties:
   *                     id:
   *                       type: string
   *                       format: uuid
   *                     storageKey:
   *                       type: string
   *                     status:
   *                       type: string
   *                       example: PENDING
   *                     jobId:
   *                       type: string
   *       '400':
   *         description: Missing file, invalid MIME type, or validation error
   *         content:
   *           application/json:
   *             schema:
   *               $ref: '#/components/schemas/ValidationErrorResponse'
   *       '413':
   *         description: File exceeds 100 MB limit
   */
  // POST /api/v1/datasets - Upload new dataset
  router.post('/', upload.single('file'), (req, res, next) => {
    controller.create(req, res, next).catch(next);
  });

  /**
   * @openapi
   * /datasets:
   *   get:
   *     tags:
   *       - Datasets
   *     summary: List datasets with optional pagination
   *     security:
   *       - bearerAuth: []
   *     parameters:
   *       - in: query
   *         name: userId
   *         schema:
   *           type: string
   *         description: Filter datasets by owner user ID
   *       - in: query
   *         name: limit
   *         schema:
   *           type: integer
   *           default: 20
   *           maximum: 100
   *         description: Maximum number of results to return
   *       - in: query
   *         name: offset
   *         schema:
   *           type: integer
   *           default: 0
   *         description: Number of results to skip
   *     responses:
   *       '200':
   *         description: Paginated list of datasets
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 success:
   *                   type: boolean
   *                   example: true
   *                 data:
   *                   type: array
   *                   items:
   *                     $ref: '#/components/schemas/DatasetListItem'
   *                 pagination:
   *                   type: object
   *                   properties:
   *                     limit:
   *                       type: integer
   *                     offset:
   *                       type: integer
   */
  // GET /api/v1/datasets - List datasets
  router.get('/', (req, res, next) => {
    controller.list(req, res, next).catch(next);
  });

  /**
   * @openapi
   * /datasets/{id}/download:
   *   get:
   *     tags:
   *       - Datasets
   *     summary: Get a signed download URL for a dataset file
   *     description: Returns a time-limited pre-signed S3 URL to download the raw file.
   *     security:
   *       - bearerAuth: []
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *           format: uuid
   *         description: Dataset ID
   *       - in: query
   *         name: expiresIn
   *         schema:
   *           type: integer
   *           description: URL expiration time in seconds (default set by storage service)
   *     responses:
   *       '200':
   *         description: Signed download URL generated
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 success:
   *                   type: boolean
   *                   example: true
   *                 data:
   *                   type: object
   *                   properties:
   *                     datasetId:
   *                       type: string
   *                       format: uuid
   *                     downloadUrl:
   *                       type: string
   *                       format: uri
   *                     expiresIn:
   *                       type: integer
   *                       description: Seconds until the URL expires
   *                     fileName:
   *                       type: string
   *       '404':
   *         description: Dataset not found
   *         content:
   *           application/json:
   *             schema:
   *               $ref: '#/components/schemas/ErrorResponse'
   */
  // GET /api/v1/datasets/:id/download - Get signed download URL
  router.get('/:id/download', (req, res, next) => {
    controller.download(req, res, next).catch(next);
  });

  // GET /api/v1/datasets/:id/stream - Stream file directly through API (supports ?token= auth)
  router.get('/:id/stream', (req, res, next) => {
    controller.stream(req, res, next).catch(next);
  });

  /**
   * @openapi
   * /datasets/{id}/transform:
   *   post:
   *     tags:
   *       - Datasets
   *     summary: Trigger a transformation job on a dataset
   *     description: |
   *       Enqueues an async ETL transformation job.  Returns the new `jobId`
   *       which can be polled via `GET /api/v1/jobs/{jobId}` or streamed via SSE.
   *     security:
   *       - bearerAuth: []
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *           format: uuid
   *         description: Dataset ID to transform
   *     requestBody:
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             required:
   *               - transformationType
   *             properties:
   *               transformationType:
   *                 type: string
   *                 enum:
   *                   - CLEAN_NULLS
   *                   - NORMALIZE
   *                   - AGGREGATE
   *                   - FILTER
   *                   - MERGE
   *                   - CUSTOM
   *                 example: CLEAN_NULLS
   *               parameters:
   *                 type: object
   *                 additionalProperties: true
   *                 description: Transformation-specific parameters
   *                 example: {}
   *               priority:
   *                 type: integer
   *                 minimum: 1
   *                 maximum: 10
   *                 default: 5
   *                 description: Queue priority (1 = lowest, 10 = highest)
   *     responses:
   *       '201':
   *         description: Transformation job created
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 success:
   *                   type: boolean
   *                   example: true
   *                 data:
   *                   type: object
   *                   properties:
   *                     jobId:
   *                       type: string
   *                     datasetId:
   *                       type: string
   *                       format: uuid
   *                     status:
   *                       type: string
   *       '400':
   *         description: Validation error (invalid transformationType)
   *         content:
   *           application/json:
   *             schema:
   *               $ref: '#/components/schemas/ValidationErrorResponse'
   *       '404':
   *         description: Dataset not found
   *         content:
   *           application/json:
   *             schema:
   *               $ref: '#/components/schemas/ErrorResponse'
   */
  // POST /api/v1/datasets/:id/transform - Trigger a transformation job
  router.post('/:id/transform', (req, res, next) => {
    controller.transform(req, res, next).catch(next);
  });

  /**
   * @openapi
   * /datasets/{id}/anomalies:
   *   get:
   *     tags:
   *       - Datasets
   *       - HITL
   *     summary: Get anomalies detected for a dataset
   *     description: Returns all anomalies flagged during ETL processing that require human review.
   *     security:
   *       - bearerAuth: []
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *           format: uuid
   *         description: Dataset ID
   *     responses:
   *       '200':
   *         description: List of anomalies
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 success:
   *                   type: boolean
   *                   example: true
   *                 data:
   *                   type: array
   *                   items:
   *                     $ref: '#/components/schemas/Anomaly'
   *       '404':
   *         description: Dataset not found
   *         content:
   *           application/json:
   *             schema:
   *               $ref: '#/components/schemas/ErrorResponse'
   */
  // GET /api/v1/datasets/:id/anomalies - Get anomalies for a dataset
  router.get('/:id/anomalies', (req, res, next) => {
    controller.getAnomalies(req, res, next).catch(next);
  });

  /**
   * @openapi
   * /datasets/{id}/decisions:
   *   post:
   *     tags:
   *       - Datasets
   *       - HITL
   *     summary: Submit human decisions for anomalies (Human-in-the-Loop)
   *     description: |
   *       Resolves one or more anomalies by submitting human decisions.
   *       Each decision can APPROVE, CORRECT, or DISCARD an anomaly.
   *     security:
   *       - bearerAuth: []
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *           format: uuid
   *         description: Dataset ID
   *     requestBody:
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             required:
   *               - decisions
   *             properties:
   *               decisions:
   *                 type: array
   *                 items:
   *                   type: object
   *                   required:
   *                     - anomalyId
   *                     - action
   *                   properties:
   *                     anomalyId:
   *                       type: string
   *                       format: uuid
   *                     action:
   *                       type: string
   *                       enum:
   *                         - APPROVED
   *                         - CORRECTED
   *                         - DISCARDED
   *                     correction:
   *                       type: string
   *                       description: Required when action is CORRECTED
   *     responses:
   *       '201':
   *         description: Decisions submitted successfully
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 success:
   *                   type: boolean
   *                   example: true
   *                 data:
   *                   type: object
   *                   properties:
   *                     resolved:
   *                       type: integer
   *                       description: Number of anomalies resolved
   *                     results:
   *                       type: array
   *                       items:
   *                         type: object
   *       '400':
   *         description: Validation error
   *         content:
   *           application/json:
   *             schema:
   *               $ref: '#/components/schemas/ValidationErrorResponse'
   *       '404':
   *         description: Dataset not found
   *         content:
   *           application/json:
   *             schema:
   *               $ref: '#/components/schemas/ErrorResponse'
   */
  /**
   * @openapi
   * /datasets/{id}/anomalies/{anomalyId}/parse-instruction:
   *   post:
   *     tags:
   *       - Datasets
   *       - HITL
   *     summary: Parse a natural language instruction into a validated IR tree
   *     description: |
   *       Converts a free-text Spanish instruction (e.g. "usa la mediana") into
   *       a validated IR node tree. Does NOT persist anything — used for preview.
   *       Rule-based parsing is tried first; Gemini is used as fallback.
   *     security:
   *       - bearerAuth: []
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *         description: Dataset ID
   *       - in: path
   *         name: anomalyId
   *         required: true
   *         schema:
   *           type: string
   *         description: Anomaly ID
   *     requestBody:
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             required:
   *               - instruction
   *             properties:
   *               instruction:
   *                 type: string
   *                 example: "usa la mediana"
   *     responses:
   *       '200':
   *         description: Instruction parsed successfully — IR + preview returned
   *       '400':
   *         description: Invalid instruction (column not found, type incompatibility, etc.)
   *       '422':
   *         description: Gemini unavailable or parse failure
   *       '404':
   *         description: Dataset or anomaly not found
   */
  // POST /api/v1/datasets/:id/anomalies/:anomalyId/parse-instruction
  router.post('/:id/anomalies/:anomalyId/parse-instruction', (req, res, next) => {
    controller.parseInstruction(req, res, next).catch(next);
  });

  // POST /api/v1/datasets/:id/decisions - Submit human decisions (HITL)
  router.post('/:id/decisions', (req, res, next) => {
    controller.submitDecisions(req, res, next).catch(next);
  });

  /**
   * @openapi
   * /datasets/{id}:
   *   get:
   *     tags:
   *       - Datasets
   *     summary: Get a dataset by ID
   *     security:
   *       - bearerAuth: []
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *           format: uuid
   *         description: Dataset ID
   *     responses:
   *       '200':
   *         description: Dataset details
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 success:
   *                   type: boolean
   *                   example: true
   *                 data:
   *                   $ref: '#/components/schemas/Dataset'
   *       '404':
   *         description: Dataset not found
   *         content:
   *           application/json:
   *             schema:
   *               $ref: '#/components/schemas/ErrorResponse'
   */
  // GET /api/v1/datasets/:id - Get dataset by ID
  router.get('/:id', (req, res, next) => {
    controller.getById(req, res, next).catch(next);
  });

  /**
   * @openapi
   * /datasets/{id}:
   *   delete:
   *     tags:
   *       - Datasets
   *     summary: Delete a dataset
   *     description: Deletes the dataset record and removes the file from storage.
   *     security:
   *       - bearerAuth: []
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *           format: uuid
   *         description: Dataset ID
   *     responses:
   *       '204':
   *         description: Dataset deleted successfully (no content)
   *       '404':
   *         description: Dataset not found
   *         content:
   *           application/json:
   *             schema:
   *               $ref: '#/components/schemas/ErrorResponse'
   */
  // DELETE /api/v1/datasets/:id - Delete dataset
  router.delete('/:id', (req, res, next) => {
    controller.delete(req, res, next).catch(next);
  });

  return router;
}
