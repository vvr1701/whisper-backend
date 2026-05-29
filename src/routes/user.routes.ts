import type { FastifyInstance } from "fastify";
import { z } from "zod";
import { User } from "../models/user.model.js";
import { getArchetypeConfig } from "../data/archetypes.js";
import {
  createCompanion,
  CompanionValidationError,
} from "../services/character.service.js";
import type { OnboardResponse } from "../types/user.types.js";

const OnboardBodySchema = z.object({
  display_name: z.string().min(1).max(50),
  gender: z.enum(["male", "female", "nonbinary", "undisclosed"]),
  date_of_birth: z.string().refine((v) => !isNaN(Date.parse(v)), {
    message: "date_of_birth must be a valid ISO date string",
  }),
  communication_style: z.enum(["warm", "direct", "funny", "calm"]),
  intent: z.string().min(1),
  companion: z.object({
    name: z.string().min(1).max(30),
    archetype: z.enum(["mentor", "bestfriend", "challenger", "partner"]),
    gender: z.enum(["male", "female", "nonbinary"]),
    voice_id: z.string().min(1),
    personality_sliders: z
      .object({
        warmth: z.number().min(0).max(100).optional(),
        humor: z.number().min(0).max(100).optional(),
        directness: z.number().min(0).max(100).optional(),
        energy: z.number().min(0).max(100).optional(),
        formality: z.number().min(0).max(100).optional(),
      })
      .optional(),
  }),
});

function calculateIsMinor(dob: string): boolean {
  const birth = new Date(dob);
  const today = new Date();
  const age =
    today.getFullYear() -
    birth.getFullYear() -
    (today < new Date(today.getFullYear(), birth.getMonth(), birth.getDate())
      ? 1
      : 0);
  return age < 18;
}

export async function userRoutes(app: FastifyInstance): Promise<void> {
  // POST /api/v1/users/onboard
  app.post("/onboard", async (request, reply) => {
    const parsed = OnboardBodySchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.status(400).send({
        success: false,
        error: parsed.error.errors.map((e) => e.message).join(", "),
      });
    }

    const body = parsed.data;
    const isMinor = calculateIsMinor(body.date_of_birth);

    const user = await User.create({
      display_name: body.display_name,
      gender: body.gender,
      date_of_birth: new Date(body.date_of_birth),
      is_minor: isMinor,
      communication_style: body.communication_style,
      onboarding_completed: true,
    });

    const archetypeDef = getArchetypeConfig(body.companion.archetype);

    // Onboard preserves its archetype-tuned defaults (different from the
    // flat 50/50/50/50/50 the standalone /characters/create uses).
    const sliders = {
      ...archetypeDef.default_sliders,
      ...body.companion.personality_sliders,
    };

    try {
      const character = await createCompanion({
        user_id: user._id.toString(),
        archetype: body.companion.archetype,
        gender: body.companion.gender,
        voice_id: body.companion.voice_id || archetypeDef.default_voice_id,
        name: body.companion.name,
        personality_sliders: sliders,
      });

      const response: OnboardResponse = {
        user_id: user._id.toString(),
        character_id: character._id.toString(),
      };

      return reply.status(201).send({ success: true, data: response });
    } catch (err) {
      if (err instanceof CompanionValidationError) {
        // Roll back the just-created user so onboard stays atomic from the
        // caller's perspective.
        await User.deleteOne({ _id: user._id });
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

  // GET /api/v1/users/:id
  app.get<{ Params: { id: string } }>("/:id", async (request, reply) => {
    const user = await User.findById(request.params.id).lean();
    if (!user) {
      return reply.status(404).send({ success: false, error: "User not found" });
    }
    return reply.send({ success: true, data: user });
  });
}
