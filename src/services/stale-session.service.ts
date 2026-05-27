import { Session } from "../models/session.model.js";
import { ConversationTurn } from "../models/conversation-turn.model.js";
import { enqueueMemoryExtraction } from "../queues/memory.queue.js";
import { logger } from "../utils/logger.js";

const IDLE_TIMEOUT_MS = 30 * 60 * 1000;  // 30 min with no turns → stale
const CHECK_INTERVAL_MS = 5 * 60 * 1000; // run every 5 min

async function closeStaleSession(sessionId: string, startedAt: Date, userId: string, characterId: string, hadTurns: boolean): Promise<void> {
  const now = new Date();
  const duration_seconds = Math.floor((now.getTime() - startedAt.getTime()) / 1000);

  await Session.updateOne(
    { _id: sessionId, status: "active" },
    { $set: { status: "interrupted", ended_at: now, duration_seconds } },
  );

  if (hadTurns) {
    await enqueueMemoryExtraction({ sessionId, characterId, userId }).catch((err) =>
      logger.error({ err, sessionId }, "Stale cleanup: failed to enqueue memory extraction"),
    );
  }
}

async function runCleanup(): Promise<void> {
  const idleCutoff = new Date(Date.now() - IDLE_TIMEOUT_MS);

  // Sessions open for at least 30 min (started before cutoff) and still active
  const candidates = await Session.find({
    status: "active",
    started_at: { $lt: idleCutoff },
  })
    .select("_id user_id character_id started_at")
    .lean();

  if (candidates.length === 0) return;

  // For each candidate, check whether there is a turn in the last 30 min.
  // Sessions with a recent turn are still live; skip them.
  const results = await Promise.allSettled(
    candidates.map(async (session) => {
      const recentTurn = await ConversationTurn.findOne({
        session_id: session._id,
        created_at: { $gte: idleCutoff },
      })
        .select("_id")
        .lean();

      if (recentTurn) return; // still active — leave it

      // No turn in the idle window — count total turns to decide whether to extract memories
      const turnCount = await ConversationTurn.countDocuments({ session_id: session._id });

      await closeStaleSession(
        session._id.toString(),
        session.started_at,
        session.user_id,
        session.character_id.toString(),
        turnCount > 0,
      );

      logger.info({ sessionId: session._id, turnCount }, "Stale session auto-closed");
    }),
  );

  const failed = results.filter((r) => r.status === "rejected");
  if (failed.length > 0) {
    logger.error({ count: failed.length }, "Stale cleanup: some sessions failed to close");
  }
}

export function startStaleSessionCleanup(): void {
  // Run once at startup (catches anything left open from a previous crash/restart),
  // then on the regular interval.
  void runCleanup().catch((err) => logger.error({ err }, "Stale cleanup: initial run failed"));
  setInterval(() => {
    void runCleanup().catch((err) => logger.error({ err }, "Stale cleanup: interval run failed"));
  }, CHECK_INTERVAL_MS);

  logger.info("Stale session cleanup started (interval: 5 min, idle threshold: 30 min)");
}
