import type { FastifyInstance } from "fastify";
import { z } from "zod";
import { Types } from "mongoose";
import { Session } from "../models/session.model.js";
import { initSessionContext } from "../services/session-context.service.js";
import { enqueueMemoryExtraction } from "../queues/memory.queue.js";
import { logger } from "../utils/logger.js";

const StartBodySchema = z.object({
  user_id: z.string().min(1),
  character_id: z.string().min(1),
  session_type: z.enum(["text", "voice_call", "voice_note"]),
});

const EndBodySchema = z.object({
  ended_at: z.string().optional(),
});

const PaginationSchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  limit: z.coerce.number().int().min(1).max(50).default(20),
});

export async function sessionRoutes(app: FastifyInstance): Promise<void> {
  // POST /api/v1/sessions/start
  app.post("/start", async (request, reply) => {
    const parsed = StartBodySchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.status(400).send({
        success: false,
        error: parsed.error.errors[0]?.message ?? "Invalid input",
      });
    }

    const { user_id, character_id, session_type } = parsed.data;

    const session = await Session.create({
      user_id,
      character_id: new Types.ObjectId(character_id),
      session_type,
      mode: "companion",
      status: "active",
      started_at: new Date(),
    });

    await initSessionContext(session._id.toString());

    return reply.status(201).send({
      success: true,
      data: { session_id: session._id.toString() },
    });
  });

  // POST /api/v1/sessions/:id/end
  app.post<{ Params: { id: string } }>("/:id/end", async (request, reply) => {
    const session = await Session.findById(request.params.id);
    if (!session) {
      return reply.status(404).send({ success: false, error: "Session not found" });
    }

    const parsed = EndBodySchema.safeParse(request.body);
    const endedAt = parsed.success && parsed.data.ended_at
      ? new Date(parsed.data.ended_at)
      : new Date();

    const duration_seconds = Math.floor(
      (endedAt.getTime() - session.started_at.getTime()) / 1000
    );

    session.ended_at = endedAt;
    session.duration_seconds = duration_seconds;
    session.status = "completed";
    await session.save();

    void enqueueMemoryExtraction({
      sessionId: session._id.toString(),
      characterId: session.character_id.toString(),
      userId: session.user_id,
    }).catch((err) => logger.error({ err, sessionId: session._id }, "Failed to enqueue memory extraction"));

    return reply.send({
      success: true,
      data: {
        session_id: session._id.toString(),
        duration_seconds,
      },
    });
  });

  // GET /api/v1/sessions/character/:character_id
  app.get<{ Params: { character_id: string } }>(
    "/character/:character_id",
    async (request, reply) => {
      const qParsed = PaginationSchema.safeParse(request.query);
      const { page, limit } = qParsed.success
        ? qParsed.data
        : { page: 1, limit: 20 };

      const character_id = new Types.ObjectId(request.params.character_id);
      const skip = (page - 1) * limit;

      const [sessions, total] = await Promise.all([
        Session.find({ character_id })
          .sort({ started_at: -1 })
          .skip(skip)
          .limit(limit)
          .lean(),
        Session.countDocuments({ character_id }),
      ]);

      return reply.send({
        success: true,
        data: {
          sessions,
          pagination: {
            page,
            limit,
            total,
            has_more: skip + sessions.length < total,
          },
        },
      });
    }
  );
}
