import type { FastifyInstance } from "fastify";
import { z } from "zod";
import { Character } from "../models/character.model.js";
import {
  createCompanion,
  CompanionValidationError,
} from "../services/character.service.js";

const CreateBodySchema = z.object({
  user_id: z.string().min(1),
  archetype: z.enum(["mentor", "bestfriend", "challenger", "partner"]),
  gender: z.enum(["male", "female", "nonbinary"]),
  voice_id: z.string().min(1),
  name: z.string().min(1).max(30),
});

export async function characterRoutes(app: FastifyInstance): Promise<void> {
  // POST /api/v1/characters/create — standalone companion creation
  // (post-onboarding "Add companion" flow). Shape mirrors the companion block
  // of /users/onboard but lives independent of user creation.
  app.post("/create", async (request, reply) => {
    const parsed = CreateBodySchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.status(400).send({
        success: false,
        error: parsed.error.errors[0]?.message ?? "Invalid input",
        code: "VALIDATION_ERROR",
      });
    }

    try {
      const character = await createCompanion(parsed.data);
      return reply.status(201).send({ success: true, data: character });
    } catch (err) {
      if (err instanceof CompanionValidationError) {
        return reply.status(400).send({
          success: false,
          error: err.message,
          code: "VALIDATION_ERROR",
          field: err.field,
        });
      }
      throw err;
    }
  });

  // GET /api/v1/characters/:id
  app.get<{ Params: { id: string } }>("/:id", async (request, reply) => {
    const character = await Character.findById(request.params.id).lean();
    if (!character) {
      return reply
        .status(404)
        .send({ success: false, error: "Character not found" });
    }
    return reply.send({ success: true, data: character });
  });
}
