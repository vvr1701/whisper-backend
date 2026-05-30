import { Types } from "mongoose";
import {
  AudioFrame,
  AudioSource,
  AudioStream,
  LocalAudioTrack,
  RoomEvent,
  TrackKind,
  TrackPublishOptions,
  TrackSource,
  type RemoteAudioTrack,
  type RemoteParticipant,
  type RemoteTrack,
  type RemoteTrackPublication,
} from "@livekit/rtc-node";
import type { JobContext } from "@livekit/agents";
import { AutoSubscribe } from "@livekit/agents";
import { getOpenAI, MODELS } from "../config/openai.js";
import { Character } from "../models/character.model.js";
import { ConversationTurn } from "../models/conversation-turn.model.js";
import { Session } from "../models/session.model.js";
import { checkModeration, getCrisisResponse } from "./safety.service.js";
import {
  getSessionContext,
  appendTurn,
  cacheCharacterConfig,
  getCachedCharacterConfig,
} from "./session-context.service.js";
import { assemblePrompt } from "./prompt.service.js";
import { compressIfNeeded } from "./context-compression.service.js";
import { retrieveMemories } from "./memory-retrieval.service.js";
import { getLatestUsageSummary, formatUsageSummary } from "./memory-summary.service.js";
import { approximateTokens } from "../utils/token-counter.js";
import { enqueueMemoryExtraction } from "../queues/memory.queue.js";
import { logger } from "../utils/logger.js";
import type { IPersonaConfig, ICharacter } from "../types/character.types.js";
import type { IRedisSessionContext } from "../types/prompt.types.js";
import {
  DeepgramSTTSession,
  DEEPGRAM_SAMPLE_RATE,
  DEEPGRAM_CHANNELS,
} from "./voice-stt.service.js";
import {
  HumeTTSSession,
  TTS_OUTPUT_SAMPLE_RATE,
  TTS_OUTPUT_CHANNELS,
} from "./voice-tts.service.js";

const FRAME_DURATION_MS = 10;
const SAMPLES_PER_FRAME = (TTS_OUTPUT_SAMPLE_RATE * FRAME_DURATION_MS) / 1000; // 480
const LATENCY_WARN_MS = 1500;
const SENTENCE_SPLIT_REGEX = /([^.!?\n]+[.!?\n]+)/g;
const UI_STATE_TOPIC = "ui";
const IDLE_DEBOUNCE_MS = 400;

export type AgentState = "idle" | "listening" | "thinking" | "speaking";
export type PublishState = (state: AgentState) => void;

const EMPTY_CTX: IRedisSessionContext = {
  compressed_summary: "",
  turns: [],
  total_token_count: 0,
};

async function getPersonaConfig(characterId: string): Promise<IPersonaConfig> {
  const cached = await getCachedCharacterConfig(characterId);
  if (cached) return cached as IPersonaConfig;

  const character = await Character.findById(characterId).lean();
  if (!character) throw new Error(`Character not found: ${characterId}`);

  await cacheCharacterConfig(characterId, character.persona_config);
  return character.persona_config;
}

interface RunTurnParams {
  sessionId: string;
  characterId: string;
  userId: string;
  transcript: string;
  finalTranscriptAt: number;
  tts: HumeTTSSession;
  signal: AbortSignal;
  onLatency: (ms: number) => void;
  publishState: PublishState;
}

/**
 * Run a single conversation turn for the voice pipeline.
 * Mirrors the structure of conversation.service.streamConversation,
 * but pipes assistant tokens to Hume TTS → AudioSource instead of SSE.
 */
async function runTurn(params: RunTurnParams): Promise<void> {
  const {
    sessionId,
    characterId,
    userId,
    transcript,
    finalTranscriptAt,
    tts,
    signal,
    onLatency,
    publishState,
  } = params;

  // We have a final transcript and are about to call the LLM — UI hint.
  publishState("thinking");

  // Parallel I/O: persona, session context, safety, memory, summary
  const [personaConfig, rawSessionCtx, modResult, memoryBlock, latestSummary] =
    await Promise.all([
      getPersonaConfig(characterId),
      getSessionContext(sessionId).then((ctx) => ctx ?? EMPTY_CTX),
      checkModeration(transcript),
      retrieveMemories(characterId, transcript).catch((err) => {
        logger.error({ err, characterId }, "Memory retrieval failed in voice turn");
        return "";
      }),
      getLatestUsageSummary(characterId).catch((err) => {
        logger.error({ err, characterId }, "Usage summary fetch failed in voice turn");
        return null;
      }),
    ]);

  if (signal.aborted) {
    logger.info({ sessionId }, "Turn aborted before LLM call");
    return;
  }

  const sessionObjId = new Types.ObjectId(sessionId);
  const characterObjId = new Types.ObjectId(characterId);
  const assistantTurnId = new Types.ObjectId();

  // Prepare TTS for this turn — reuses the persistent socket, just resets per-turn latency state.
  await tts.ensureConnected();
  tts.beginTurn();

  // Crisis path: speak the crisis response, skip LLM
  if (modResult.is_crisis) {
    const crisis = getCrisisResponse();
    tts.sendText(crisis);
    tts.flush();

    await persistTurns({
      sessionObjId,
      characterObjId,
      userId,
      userMessage: transcript,
      assistantMessage: crisis,
      assistantTurnId,
      latency_ms: 0,
      tokensInput: 0,
      tokensOutput: 0,
      userFlagged: modResult.flagged,
      userCategories: modResult.categories,
    });
    await appendTurn(sessionId, "user", transcript);
    await appendTurn(sessionId, "assistant", crisis);
    return;
  }

  if (modResult.flagged) {
    logger.warn({ sessionId, userId }, "User voice input flagged (not crisis) — proceeding");
  }

  const sessionCtx = await compressIfNeeded(sessionId, rawSessionCtx);
  const usageSummaryText = latestSummary ? formatUsageSummary(latestSummary) : null;

  const { messages, total_tokens } = assemblePrompt(
    personaConfig.system_prompt,
    sessionCtx,
    transcript,
    memoryBlock || null,
    usageSummaryText
  );

  // Stream from LLM, buffer to sentences, push each completed sentence to Hume.
  // AbortSignal cancels the upstream stream cleanly on barge-in.
  const openai = getOpenAI();
  let fullContent = "";
  let buffer = "";
  let outputTokens = 0;
  let aborted = false;

  try {
    const stream = await openai.chat.completions.create(
      {
        model: MODELS.CONVERSATION,
        messages,
        stream: true,
        max_completion_tokens: 400,
        stream_options: { include_usage: true },
      },
      { signal }
    );

    for await (const chunk of stream) {
      if (signal.aborted) {
        aborted = true;
        break;
      }
      const token = chunk.choices[0]?.delta?.content;
      if (token) {
        fullContent += token;
        buffer += token;

        // Flush every complete sentence
        let match: RegExpExecArray | null;
        SENTENCE_SPLIT_REGEX.lastIndex = 0;
        let lastEnd = 0;
        while ((match = SENTENCE_SPLIT_REGEX.exec(buffer)) !== null) {
          const sentence = match[1].trim();
          if (sentence) tts.sendText(sentence);
          lastEnd = match.index + match[0].length;
        }
        if (lastEnd > 0) buffer = buffer.slice(lastEnd);
      }
      if (chunk.usage) {
        outputTokens = chunk.usage.completion_tokens;
      }
    }

    if (!aborted) {
      const tail = buffer.trim();
      if (tail) tts.sendText(tail);
      tts.flush();
    }
  } catch (err) {
    const isAbort =
      err instanceof Error &&
      (err.name === "AbortError" || err.message.includes("aborted") || err.message.includes("Abort"));
    if (isAbort || signal.aborted) {
      logger.info({ sessionId }, "LLM stream aborted (barge-in)");
      return;
    }
    logger.error({ err, sessionId }, "LLM streaming error in voice turn");
    return;
  }

  if (aborted || signal.aborted) {
    logger.info({ sessionId }, "Turn aborted mid-stream — skipping persistence");
    return;
  }

  // Wait briefly for first audio byte to compute latency.
  const firstAudioByteAt = await waitForFirstAudioByte(tts, signal, 5000);
  const latency_ms = firstAudioByteAt > 0 ? firstAudioByteAt - finalTranscriptAt : 0;
  if (firstAudioByteAt > 0) {
    onLatency(latency_ms);
    if (latency_ms > LATENCY_WARN_MS) {
      logger.warn({ sessionId, latency_ms }, `Voice turn latency exceeded ${LATENCY_WARN_MS}ms`);
    }
  }

  // Persist
  await persistTurns({
    sessionObjId,
    characterObjId,
    userId,
    userMessage: transcript,
    assistantMessage: fullContent,
    assistantTurnId,
    latency_ms,
    tokensInput: total_tokens,
    tokensOutput: outputTokens || approximateTokens(fullContent),
    userFlagged: modResult.flagged,
    userCategories: modResult.categories,
  });
  await appendTurn(sessionId, "user", transcript);
  await appendTurn(sessionId, "assistant", fullContent);

  // Output moderation (log-only)
  checkModeration(fullContent)
    .then((m) => {
      if (m.flagged) logger.warn({ sessionId }, "Voice LLM output flagged by moderation");
    })
    .catch((err) => logger.error({ err }, "Output moderation failed in voice turn"));
}

async function waitForFirstAudioByte(
  tts: HumeTTSSession,
  signal: AbortSignal,
  timeoutMs: number
): Promise<number> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (signal.aborted) return 0;
    const t = tts.getFirstChunkAt();
    if (t > 0) return t;
    await new Promise((r) => setTimeout(r, 10));
  }
  return 0;
}

interface PersistTurnsParams {
  sessionObjId: Types.ObjectId;
  characterObjId: Types.ObjectId;
  userId: string;
  userMessage: string;
  assistantMessage: string;
  assistantTurnId: Types.ObjectId;
  latency_ms: number;
  tokensInput: number;
  tokensOutput: number;
  userFlagged: boolean;
  userCategories: Record<string, boolean>;
}

async function persistTurns(p: PersistTurnsParams): Promise<void> {
  const now = new Date();
  await ConversationTurn.insertMany([
    {
      session_id: p.sessionObjId,
      character_id: p.characterObjId,
      user_id: p.userId,
      role: "user",
      content_text: p.userMessage,
      content_audio_url: null,
      safety_flags: { categories: p.userCategories, flagged: p.userFlagged },
      tokens_used: { input: p.tokensInput, output: 0 },
      model_used: MODELS.CONVERSATION,
      latency_ms: 0,
      created_at: now,
    },
    {
      _id: p.assistantTurnId,
      session_id: p.sessionObjId,
      character_id: p.characterObjId,
      user_id: p.userId,
      role: "assistant",
      content_text: p.assistantMessage,
      content_audio_url: null,
      safety_flags: { categories: {}, flagged: false },
      tokens_used: { input: 0, output: p.tokensOutput },
      model_used: MODELS.CONVERSATION,
      latency_ms: p.latency_ms,
      created_at: new Date(now.getTime() + 1),
    },
  ]);
}

/**
 * Serializes captureFrame calls onto an AudioSource. LiveKit's FFI cannot
 * handle concurrent captureFrame calls on the same source (throws InvalidState),
 * so multiple Hume chunks arriving in parallel must funnel through one drainer.
 */
interface AudioWriterCallbacks {
  onFirstChunk?: () => void;
  onDrained?: () => void;
}

class SerialAudioWriter {
  private queue: Int16Array[] = [];
  private draining = false;
  private idleTimer: NodeJS.Timeout | null = null;

  constructor(private source: AudioSource, private callbacks: AudioWriterCallbacks = {}) {}

  enqueue(pcm: Int16Array): void {
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
    const wasEmpty = this.queue.length === 0 && !this.draining;
    this.queue.push(pcm);
    if (wasEmpty) this.callbacks.onFirstChunk?.();
    void this.drain();
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (this.queue.length > 0) {
        const pcm = this.queue.shift()!;
        for (let offset = 0; offset < pcm.length; offset += SAMPLES_PER_FRAME) {
          const slice = pcm.subarray(offset, Math.min(offset + SAMPLES_PER_FRAME, pcm.length));
          const frame = AudioFrame.create(
            TTS_OUTPUT_SAMPLE_RATE,
            TTS_OUTPUT_CHANNELS,
            slice.length
          );
          frame.data.set(slice);
          try {
            await this.source.captureFrame(frame);
          } catch (err) {
            logger.error({ err }, "captureFrame failed — clearing queue");
            this.queue.length = 0;
            return;
          }
        }
      }
    } finally {
      this.draining = false;
      // Audio has drained — schedule "idle" if no new chunks arrive within debounce window.
      if (this.callbacks.onDrained) {
        this.idleTimer = setTimeout(() => {
          this.idleTimer = null;
          this.callbacks.onDrained?.();
        }, IDLE_DEBOUNCE_MS);
      }
    }
  }

  /** Drop pending PCM and flush the underlying source (barge-in path). */
  clear(): void {
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
    this.queue.length = 0;
    this.source.clearQueue();
  }
}

export async function runVoicePipeline(ctx: JobContext): Promise<void> {
  // Room name lives on the proto job dispatch (available before connect).
  // `ctx.room.name` is undefined until ctx.connect() resolves.
  const roomName = ctx.job.room?.name;
  if (!roomName) {
    logger.error("Voice agent dispatched without a room name — aborting");
    return;
  }

  if (!Types.ObjectId.isValid(roomName)) {
    logger.error({ roomName }, "Room name is not a valid session_id");
    return;
  }

  const sessionId = roomName;
  logger.info({ sessionId }, "Voice agent starting pipeline");

  const session = await Session.findById(sessionId);
  if (!session) {
    logger.error({ sessionId }, "Session not found — aborting voice pipeline");
    return;
  }

  const character = await Character.findById(session.character_id).lean<ICharacter>();
  if (!character) {
    logger.error({ sessionId, characterId: session.character_id }, "Character not found");
    return;
  }

  const characterId = session.character_id.toString();
  const userId = session.user_id;
  const voiceName = character.voice_id;

  await ctx.connect(undefined, AutoSubscribe.AUDIO_ONLY);

  // UI state publisher: emits {kind: "agent_state", state} on DataChannel topic "ui"
  // so the mobile client can drive its orb without local guessing.
  let lastPublishedState: AgentState | null = null;
  const stateEncoder = new TextEncoder();
  const publishState: PublishState = (state) => {
    if (state === lastPublishedState) return;
    lastPublishedState = state;
    const payload = stateEncoder.encode(JSON.stringify({ kind: "agent_state", state }));
    const lp = ctx.room.localParticipant;
    if (!lp) return;
    lp.publishData(payload, { reliable: true, topic: UI_STATE_TOPIC }).catch((err) =>
      logger.warn({ err, state }, "publishData(agent_state) failed")
    );
  };

  // Publish agent's output audio track (Hume → LiveKit)
  const audioSource = new AudioSource(TTS_OUTPUT_SAMPLE_RATE, TTS_OUTPUT_CHANNELS);
  const audioWriter = new SerialAudioWriter(audioSource, {
    onFirstChunk: () => publishState("speaking"),
    onDrained: () => publishState("idle"),
  });
  const agentTrack = LocalAudioTrack.createAudioTrack("agent-audio", audioSource);
  const publishOpts = new TrackPublishOptions();
  publishOpts.source = TrackSource.SOURCE_MICROPHONE;
  if (!ctx.agent) {
    logger.error({ sessionId }, "ctx.agent not available after connect — aborting");
    return;
  }
  await ctx.agent.publishTrack(agentTrack, publishOpts);

  // Wait for the human participant
  const participant = await ctx.waitForParticipant(userId);
  logger.info({ sessionId, participantIdentity: participant.identity }, "Participant joined");

  // Persistent Hume TTS — one socket per session. Audio chunks always route
  // into the SerialAudioWriter; barge-in clears the writer + closes Hume socket
  // (next sendText auto-reconnects).
  const tts = new HumeTTSSession(voiceName, {
    onAudioChunk: ({ pcm }) => audioWriter.enqueue(pcm),
    onError: (err) => logger.error({ err, sessionId }, "Hume TTS error"),
    onClose: () => logger.debug({ sessionId }, "Hume TTS socket closed (will reopen on next send)"),
  });
  try {
    await tts.ensureConnected();
  } catch (err) {
    logger.error({ err, sessionId }, "Initial Hume connect failed — will retry lazily");
  }

  // Turn state. Only one turn runs at a time; new transcripts during an active
  // turn either (a) trigger barge-in if bot is speaking, or (b) queue for the next turn.
  let activeTurnAbort: AbortController | null = null;
  let botStartedSpeakingAt = 0;
  const BARGE_IN_GRACE_MS = 1500;
  const COALESCE_SAFETY_MS = 1500; // fallback if Deepgram UtteranceEnd doesn't arrive
  const latencies: number[] = [];

  // Coalescing state — accumulate is_final segments until UtteranceEnd (or safety timer).
  let pendingTranscript = "";
  let coalesceSafetyTimer: NodeJS.Timeout | null = null;

  const triggerTurn = (): void => {
    const text = pendingTranscript.trim();
    pendingTranscript = "";
    if (coalesceSafetyTimer) {
      clearTimeout(coalesceSafetyTimer);
      coalesceSafetyTimer = null;
    }
    if (!text) return;

    const finalTranscriptAt = Date.now();
    botStartedSpeakingAt = finalTranscriptAt;

    const abortCtrl = new AbortController();
    activeTurnAbort = abortCtrl;
    logger.info({ sessionId, text }, "Coalesced transcript — starting turn");

    runTurn({
      sessionId,
      characterId,
      userId,
      transcript: text,
      finalTranscriptAt,
      tts,
      signal: abortCtrl.signal,
      onLatency: (ms) => latencies.push(ms),
      publishState,
    })
      .catch((err) => logger.error({ err, sessionId }, "runTurn failed"))
      .finally(() => {
        if (activeTurnAbort === abortCtrl) activeTurnAbort = null;
      });
  };

  const stt = new DeepgramSTTSession({
    onSpeechStarted: () => {
      // Within the barge-in grace window the "speech" is almost certainly the bot's
      // own audio bleeding back into the mic — don't flip the orb to listening.
      if (activeTurnAbort) {
        const elapsed = Date.now() - botStartedSpeakingAt;
        if (elapsed < BARGE_IN_GRACE_MS) {
          logger.debug({ sessionId, elapsedMs: elapsed }, "Ignoring SpeechStarted within barge-in grace");
          return;
        }
        logger.info({ sessionId, elapsedMs: elapsed }, "Barge-in — cancelling turn");
        activeTurnAbort.abort();
        activeTurnAbort = null;
        tts.cancelTurn();        // close Hume socket; next turn reopens
        audioWriter.clear();     // drop already-queued audio frames
      }
      // Either no active turn (fresh utterance) or we just bargedin → user is speaking.
      publishState("listening");
    },
    onFinalSegment: ({ text }) => {
      // Accumulate; do not trigger a turn yet.
      pendingTranscript += (pendingTranscript ? " " : "") + text;
      logger.debug({ sessionId, segment: text, pending: pendingTranscript }, "Accumulated final segment");
      // Reset safety timer — fires only if UtteranceEnd never arrives.
      if (coalesceSafetyTimer) clearTimeout(coalesceSafetyTimer);
      coalesceSafetyTimer = setTimeout(() => {
        logger.warn({ sessionId }, "UtteranceEnd timed out — firing turn from safety timer");
        triggerTurn();
      }, COALESCE_SAFETY_MS);
    },
    onUtteranceEnd: () => {
      triggerTurn();
    },
    onError: (err) => logger.error({ err }, "STT error"),
    onClose: () => logger.info({ sessionId }, "STT session closed"),
  });
  await stt.connect();

  // Forward subscribed audio tracks to Deepgram.
  // Handle BOTH cases: tracks already subscribed before this listener attached,
  // and tracks subscribed afterwards.
  const pipedTrackSids = new Set<string>();
  const tryPipeTrack = (track: RemoteTrack | undefined, sid: string | undefined): void => {
    if (!track || !sid || pipedTrackSids.has(sid)) return;
    if (track.kind !== TrackKind.KIND_AUDIO) return;
    pipedTrackSids.add(sid);
    logger.info({ sessionId, sid }, "Piping user audio track to STT");
    void pipeTrackToSTT(track as RemoteAudioTrack, stt);
  };

  // Pick up tracks the participant already published before we attached the listener
  logger.info(
    { sessionId, trackCount: participant.trackPublications.size },
    "Inspecting existing participant tracks"
  );
  for (const pub of participant.trackPublications.values()) {
    logger.info(
      { sessionId, sid: pub.sid, kind: pub.kind, subscribed: (pub as RemoteTrackPublication).subscribed, hasTrack: Boolean(pub.track) },
      "Existing track publication"
    );
    tryPipeTrack(pub.track as RemoteTrack | undefined, pub.sid);
  }

  ctx.room.on(RoomEvent.TrackSubscribed, (track: RemoteTrack, pub: RemoteTrackPublication, p: RemoteParticipant) => {
    if (p.identity !== userId) return;
    tryPipeTrack(track, pub.sid);
  });

  // Handle participant disconnect → end session
  ctx.room.on(RoomEvent.ParticipantDisconnected, async (p: RemoteParticipant) => {
    if (p.identity !== userId) return;
    logger.info({ sessionId }, "User disconnected — ending voice session");
    await endVoiceSession(sessionId);
    ctx.shutdown("user-disconnected");
  });

  ctx.room.on(RoomEvent.Disconnected, async () => {
    logger.info({ sessionId }, "Room disconnected — ending voice session");
    await endVoiceSession(sessionId);
  });

  // Cleanup on shutdown
  ctx.addShutdownCallback(async () => {
    logger.info({ sessionId }, "Shutting down voice pipeline");
    if (activeTurnAbort) activeTurnAbort.abort();
    if (coalesceSafetyTimer) clearTimeout(coalesceSafetyTimer);
    stt.close();
    tts.close();
    await audioSource.close();
    if (latencies.length > 0) {
      const p50 = percentile(latencies, 0.5);
      logger.info(
        { sessionId, p50_latency_ms: p50, turn_count: latencies.length },
        "Voice session latency summary"
      );
    }
  });
}

async function pipeTrackToSTT(track: RemoteAudioTrack, stt: DeepgramSTTSession): Promise<void> {
  const stream = new AudioStream(track, DEEPGRAM_SAMPLE_RATE, DEEPGRAM_CHANNELS);
  try {
    for await (const frame of stream) {
      // frame.data is Int16Array — Deepgram expects raw linear16 bytes
      const buf = Buffer.from(frame.data.buffer, frame.data.byteOffset, frame.data.byteLength);
      stt.sendAudio(buf);
    }
  } catch (err) {
    logger.error({ err }, "Audio stream piping to STT failed");
  }
}

async function endVoiceSession(sessionId: string): Promise<void> {
  try {
    const session = await Session.findById(sessionId);
    if (!session || session.status !== "active") return;

    const endedAt = new Date();
    const duration_seconds = Math.floor(
      (endedAt.getTime() - session.started_at.getTime()) / 1000
    );
    session.ended_at = endedAt;
    session.duration_seconds = duration_seconds;
    session.voice_minutes_consumed = Math.ceil(duration_seconds / 60);
    session.status = "completed";
    await session.save();

    void enqueueMemoryExtraction({
      sessionId: session._id.toString(),
      characterId: session.character_id.toString(),
      userId: session.user_id,
    }).catch((err) =>
      logger.error({ err, sessionId }, "Failed to enqueue memory extraction (voice)")
    );
  } catch (err) {
    logger.error({ err, sessionId }, "endVoiceSession failed");
  }
}

function percentile(values: number[], p: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx] ?? 0;
}
