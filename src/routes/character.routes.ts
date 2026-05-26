import type { FastifyInstance } from "fastify";
import { Character } from "../models/character.model.js";

export async function characterRoutes(app: FastifyInstance): Promise<void> {
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
