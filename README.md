# Whisper Backend

Voice-first AI companion backend. Whisper gives users a persistent, emotionally intelligent companion that remembers across sessions, adapts its personality, and responds in real time via streaming.

Built with **Fastify · MongoDB Atlas · Redis · BullMQ · OpenAI**.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Node.js 22 + TypeScript (ESM) |
| HTTP Framework | Fastify 5 |
| Database | MongoDB Atlas (Mongoose 8) |
| Cache / Queue broker | Redis 7 (ioredis) |
| Background jobs | BullMQ 5 |
| AI | OpenAI SDK — GPT-4o Mini, text-embedding-3-small |
| Streaming | Server-Sent Events (SSE) |
| Dev tooling | tsx, nodemon |

---

## Architecture Overview

```
Client
  │
  ├── POST /conversations/send ──► SSE stream ──► GPT (gpt-5.4-mini)
  │                                    │
  │                              Redis session context
  │                              (compressed rolling window, ≤4k tokens)
  │
  ├── POST /sessions/end ──► BullMQ ──► Job 1: Memory Extraction
  │                                         │   (GPT-4o Mini + embeddings)
  │                                         │
  │                                     Job 2: Usage Summary
  │                                         (triggers after 50 turns or 5 sessions)
  │
  └── GET /memories/:character_id ──► MongoDB Atlas Vector Search
```

### Prompt structure (every turn)
```
[System — persona config       ~500 tokens]
[System — long-term memory     ~200-300 tokens]   ← vector search top-10, re-ranked
[System — usage-based summary  ~200-300 tokens]   ← latest MemorySummary doc
[System — compressed history   variable         ]   ← older turns, GPT-compressed
[User/Assistant turns          ~2000-4000 tokens]   ← last 20 turns verbatim
[User — current message        ~50-200 tokens   ]
```

---

## Project Structure

```
src/
├── config/          # DB, Redis, OpenAI, BullMQ connections
├── data/            # Archetype definitions (mentor, bestfriend, challenger, partner)
├── middleware/      # Error handler
├── models/          # Mongoose schemas
│   ├── user.model.ts
│   ├── character.model.ts
│   ├── session.model.ts
│   ├── conversation-turn.model.ts
│   ├── memory.model.ts
│   └── memory-summary.model.ts
├── queues/          # BullMQ queue + job helpers
├── routes/          # Fastify route handlers
├── services/        # Business logic
│   ├── conversation.service.ts     # LLM streaming pipeline
│   ├── prompt.service.ts           # Prompt assembly
│   ├── safety.service.ts           # OpenAI Moderation + crisis detection
│   ├── session-context.service.ts  # Redis session context
│   ├── memory-extraction.service.ts # Post-session fact extraction
│   ├── memory-retrieval.service.ts  # Vector search + re-ranking
│   ├── memory-summary.service.ts    # Usage-based summary generation
│   └── context-compression.service.ts # In-session rolling compression
├── types/           # TypeScript interfaces
├── utils/           # Logger, token counter
└── workers/         # BullMQ worker (memory jobs)
```

---

## Setup

### Prerequisites
- Node.js 22+
- Docker (for local Redis) or a Redis URL
- MongoDB Atlas cluster
- OpenAI API key

### 1. Install dependencies

```bash
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
PORT=3000
NODE_ENV=development

MONGODB_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/whisper
REDIS_URL=redis://localhost:6379
OPENAI_API_KEY=sk-...
```

### 3. Start Redis (Docker)

```bash
docker run -d -p 6379:6379 redis:7-alpine
```

Or use the included compose file:

```bash
docker-compose up -d redis
```

### 4. Create Atlas Vector Search index

In **Atlas UI → your cluster → Search → Create Index → JSON Editor**, select the `memories` collection and use this definition:

```json
{
  "fields": [
    { "type": "vector", "path": "embedding", "numDimensions": 1536, "similarity": "cosine" },
    { "type": "filter", "path": "character_id" },
    { "type": "filter", "path": "is_deleted" },
    { "type": "filter", "path": "type" }
  ]
}
```

Name: **`memory_vector_index`**

### 5. Run

```bash
# Development (hot reload)
npm run dev

# Production
npm run build && npm start
```

---

## API Reference

### Health

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Server health check |

### Users

| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/users/onboard` | Create user + companion in one step |
| GET | `/api/v1/users/:id` | Fetch user profile |

**POST /api/v1/users/onboard**
```json
{
  "display_name": "Alex",
  "gender": "male",
  "date_of_birth": "1995-06-15",
  "communication_style": "direct",
  "intent": "stay motivated",
  "companion": {
    "name": "Sage",
    "archetype": "bestfriend",
    "gender": "female",
    "voice_id": "hume-voice-bestfriend-default"
  }
}
```

Archetypes: `mentor` · `bestfriend` · `challenger` · `partner`

---

### Characters

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/characters/:id` | Fetch character profile |

---

### Sessions

| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/sessions/start` | Start a session, initialize Redis context |
| POST | `/api/v1/sessions/:id/end` | End session, trigger memory extraction job |
| GET | `/api/v1/sessions/character/:character_id` | Paginated session history |

---

### Conversations

| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/conversations/send` | Send message — **SSE streaming response** |
| GET | `/api/v1/conversations/:session_id` | Paginated turn history |

**POST /api/v1/conversations/send** — returns Server-Sent Events:
```
event: chunk
data: {"content":"Hey, that"}

event: chunk
data: {"content":" sounds stressful..."}

event: done
data: {"turn_id":"...", "tokens_used": {"input": 612, "output": 87}}
```

Special events: `crisis` (self-harm detected, 988 hotline injected), `error`

---

### Memories

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/memories/:character_id` | All non-deleted memories. Filter: `?type=fact\|emotion\|event\|preference` |
| DELETE | `/api/v1/memories/:memory_id` | Soft-delete one memory |
| DELETE | `/api/v1/memories/character/:character_id` | Bulk soft-delete all memories for a character |

---

## Memory System

### How memories are created
1. User ends a session → `POST /sessions/:id/end`
2. BullMQ enqueues **Job 1 (memory-extraction)**
3. Job reads all conversation turns → GPT-4o Mini extracts facts
4. Each fact is embedded with `text-embedding-3-small` (1536 dims)
5. Memories inserted into MongoDB `memories` collection

### How memories are used
On every conversation turn:
1. User message is embedded
2. Atlas `$vectorSearch` returns top-10 nearest memories for that character
3. Re-ranked: `0.6 × cosine_similarity + 0.4 × recency_decay`
4. Deduplicated (cosine > 0.85 = merge)
5. Formatted as `[Known about this person]` block, injected as system message

### Usage-based summary
Triggers automatically when **50 turns** or **5 sessions** accumulate since the last summary. GPT-4o Mini generates `mood_summary`, `recurring_topics`, `emotional_patterns`, `relationship_trajectory` — injected into every subsequent prompt.

### Session context compression
When the Redis session context exceeds **3,500 tokens**, the oldest 10 turns are compressed into an ~80-token summary via GPT-4o Mini. The last 20 turns are always kept verbatim. Cost: ~$0.00015 per compression.

---

## Companion Archetypes

| Archetype | Personality | Use case |
|---|---|---|
| `mentor` | Wise, Socratic, growth-focused | Personal development |
| `bestfriend` | Warm, casual, validating | Emotional support |
| `challenger` | Blunt, direct, no excuses | Accountability |
| `partner` | Deeply attentive, intimate | Connection |

Each archetype has its own system prompt, behavioral rules, safety overrides, and default personality sliders (warmth, humor, directness, energy, formality).

---

## Safety

- **OpenAI Moderation API** runs on every user message and every LLM output
- **Crisis detection**: self-harm score > 0.5 → bypasses LLM, injects 988 Suicide & Crisis Lifeline response
- **Minor protection**: `is_minor` flag set at onboarding via DOB calculation
- Output moderation: flagged responses are logged but never blocked (Phase 1)

---

## Background Jobs (BullMQ)

| Job | Trigger | What it does |
|---|---|---|
| `memory-extraction` | Session end | Extract facts → embed → insert memories |
| `usage-summary` | After extraction, if 50 turns or 5 sessions | Generate compressed summary of all turns since last summary |

Queue: `whisper-memory` · Retries: 3 × exponential backoff · Concurrency: 2

---

## Sprint Progress

- [x] **Sprint 1** — Project setup, Mongoose models, onboarding API
- [x] **Sprint 2** — LLM streaming chat, session management, safety classifier, Redis context
- [x] **Sprint 3** — Memory extraction, vector retrieval, context compression, usage-based summaries
- [ ] **Sprint 4** — Voice (LiveKit + Deepgram + Hume AI)

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `PORT` | No (default 3000) | HTTP port |
| `MONGODB_URI` | Yes | MongoDB Atlas connection string |
| `REDIS_URL` | Yes | Redis connection URL |
| `OPENAI_API_KEY` | Yes | OpenAI API key |
| `LIVEKIT_API_KEY` | Sprint 4 | LiveKit API key |
| `LIVEKIT_API_SECRET` | Sprint 4 | LiveKit API secret |
| `LIVEKIT_URL` | Sprint 4 | LiveKit server URL |
| `DEEPGRAM_API_KEY` | Sprint 4 | Deepgram API key |
| `HUME_API_KEY` | Sprint 4 | Hume AI API key |
