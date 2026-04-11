import type { Request, Response } from 'express';

/** SSE keepalive interval in ms. */
const KEEPALIVE_INTERVAL_MS = 15_000;

/** Max SSE stream duration in ms (10 minutes). */
const MAX_STREAM_DURATION_MS = 10 * 60 * 1_000;

/**
 * Global SSE controller for workspace-level dataset events.
 *
 * Maintains an in-memory Map<userId, Response[]> of active SSE connections.
 * DatasetStatusSyncListener calls EventsController.emit() when a dataset
 * changes state so every connected client for that user gets notified.
 */
export class EventsController {
  /** Active SSE connections keyed by userId. */
  private static connections = new Map<string, Response[]>();

  /**
   * Emit an SSE event to all connections belonging to a specific user.
   *
   * @param userId - The owner of the dataset that changed.
   * @param event  - SSE event name (e.g. 'dataset:status_changed').
   * @param data   - Payload to JSON-serialize as the SSE data field.
   */
  static emit(userId: string, event: string, data: Record<string, unknown>): void {
    const conns = EventsController.connections.get(userId);
    if (!conns || conns.length === 0) return;

    const frame = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    for (const res of conns) {
      try {
        res.write(frame);
      } catch {
        // Connection already closed — will be cleaned up on 'close' event
      }
    }
  }

  /**
   * GET /api/v1/events
   * Opens a persistent SSE connection for the authenticated user.
   * The userId comes from req.user injected by AuthMiddleware.
   */
  subscribe(req: Request, res: Response): void {
    // AuthMiddleware attaches the decoded userId directly as req.userId
    const userId = req.userId;

    if (!userId) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }

    // ── SSE headers ──────────────────────────────────────────────────────────
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    // ── Register connection ───────────────────────────────────────────────────
    const existing = EventsController.connections.get(userId) ?? [];
    existing.push(res);
    EventsController.connections.set(userId, existing);

    // ── Helper ────────────────────────────────────────────────────────────────
    let closed = false;
    const cleanup = (): void => {
      if (closed) return;
      closed = true;
      clearInterval(keepalive);
      clearTimeout(timeout);

      const conns = EventsController.connections.get(userId) ?? [];
      const filtered = conns.filter((r) => r !== res);
      if (filtered.length === 0) {
        EventsController.connections.delete(userId);
      } else {
        EventsController.connections.set(userId, filtered);
      }

      res.end();
    };

    // ── Keepalive comment every 15s ───────────────────────────────────────────
    const keepalive = setInterval(() => {
      if (!closed) res.write(': keepalive\n\n');
    }, KEEPALIVE_INTERVAL_MS);

    // ── Hard timeout after 10 minutes ────────────────────────────────────────
    const timeout = setTimeout(() => {
      if (!closed) {
        res.write(`event: timeout\ndata: ${JSON.stringify({ message: 'Stream closed after 10 minutes' })}\n\n`);
        cleanup();
      }
    }, MAX_STREAM_DURATION_MS);

    // ── Client disconnect ─────────────────────────────────────────────────────
    req.on('close', cleanup);
  }
}
