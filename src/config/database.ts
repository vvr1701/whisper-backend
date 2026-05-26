import mongoose from "mongoose";
import { env } from "./env.js";
import { logger } from "../utils/logger.js";

export async function connectDatabase(): Promise<void> {
  mongoose.connection.on("connected", () =>
    logger.info("MongoDB connected")
  );
  mongoose.connection.on("disconnected", () =>
    logger.warn("MongoDB disconnected")
  );
  mongoose.connection.on("error", (err) =>
    logger.error({ err }, "MongoDB connection error")
  );

  await mongoose.connect(env.MONGODB_URI, {
    serverSelectionTimeoutMS: 5000,
  });
}

export async function disconnectDatabase(): Promise<void> {
  await mongoose.disconnect();
}
