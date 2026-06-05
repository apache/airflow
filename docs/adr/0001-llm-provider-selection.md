# ADR 0001: LLM Provider Selection for v1 and Data Privacy Strategy

Date: 2026-06-04

## Status

Proposed

## Context

The Airflow AI assistant feature requires an LLM provider to process operator-submitted
log content and surface actionable diagnostics. Two concerns must be resolved before v1 ships:

1. **Which LLM provider to use** — balancing cost, response latency, and output quality
   against operational constraints.
2. **Data privacy** — operators send real task log content (which may include secrets,
   PII, or proprietary pipeline details) to a hosted model. Enterprise customers have
   raised data residency and compliance requirements (SOC 2, GDPR, HIPAA) that restrict
   which third-party processors may receive that data.

The decision is currently **blocked on legal/compliance input** from a pilot enterprise
customer. To avoid delaying the rest of the build, the mitigation is to implement a
**provider abstraction layer** (tracked separately as Issue B) so the provider choice
is decoupled from core feature development. This ADR records the comparison and will
be updated with a final decision once legal input is received.

---

## Provider Comparison

The following providers were evaluated across the criteria most critical for this use case:
cost per token, p50 response latency, data residency controls, and compliance certifications.

### Evaluated Providers

#### 1. OpenAI GPT-4o

| Attribute | Detail |
|---|---|
| **Model** | GPT-4o (gpt-4o-2024-08-06) |
| **Input cost** | $2.50 / 1M tokens |
| **Output cost** | $10.00 / 1M tokens |
| **p50 latency** | ~500 ms (non-streaming) |
| **Data residency** | US by default; EU available via Azure OpenAI |
| **Zero data retention** | Available via API (opt-in, no training on inputs) |
| **SOC 2 Type II** | Yes |
| **HIPAA BAA** | Yes (Enterprise tier) |
| **GDPR DPA** | Yes |
| **On-prem option** | No (cloud-only) |

**Strengths:** Highest benchmark scores on instruction-following and structured output;
large ecosystem of tooling; fastest iteration cadence.

**Weaknesses:** Data leaves the operator's environment by default; EU residency requires
routing through Azure, adding operational complexity; highest output cost in this comparison.

---

#### 2. Anthropic Claude 3.5 Sonnet

| Attribute | Detail |
|---|---|
| **Model** | claude-sonnet-4-6 |
| **Input cost** | $3.00 / 1M tokens |
| **Output cost** | $15.00 / 1M tokens |
| **p50 latency** | ~600 ms (non-streaming) |
| **Data residency** | US by default; EU via AWS Bedrock |
| **Zero data retention** | Yes (no training on API inputs by default) |
| **SOC 2 Type II** | Yes |
| **HIPAA BAA** | Yes |
| **GDPR DPA** | Yes |
| **On-prem option** | No (cloud-only); available via AWS Bedrock in customer VPC |

**Strengths:** Strong performance on long-context log analysis; no training on inputs
by default (lower legal risk than OpenAI default tier); available via Bedrock for
customers who need AWS-native data governance.

**Weaknesses:** Higher output cost; slightly higher latency; no native EU endpoint
(must route via Bedrock).

---

#### 3. AWS Bedrock (managed hosting of multiple models)

| Attribute | Detail |
|---|---|
| **Models available** | Claude 3.5 Sonnet, Llama 3, Titan, Mistral, and others |
| **Input cost** | Varies by model; Claude 3.5 Sonnet ~$3.00 / 1M tokens |
| **Output cost** | Varies by model; Claude 3.5 Sonnet ~$15.00 / 1M tokens |
| **p50 latency** | ~700–900 ms (additional routing vs. direct API) |
| **Data residency** | Stays within the customer's AWS region; no cross-region by default |
| **Zero data retention** | Yes — data never leaves the customer's AWS account |
| **SOC 2 Type II** | Yes (AWS) |
| **HIPAA BAA** | Yes (standard AWS BAA) |
| **GDPR DPA** | Yes |
| **On-prem option** | Effectively yes — runs inside customer's AWS VPC |

**Strengths:** Strongest data residency story — log content never leaves the operator's
own AWS account; single BAA covers all models; best fit for enterprise customers with
existing AWS data governance policies.

**Weaknesses:** Higher latency than direct API calls; operational dependency on AWS;
model selection is constrained to what Bedrock supports; some newer model versions
lag behind direct API availability.

---

#### 4. Self-hosted / On-premises (e.g. Ollama + Llama 3)

| Attribute | Detail |
|---|---|
| **Models available** | Llama 3 70B, Mistral 7B, CodeLlama, and others |
| **Input cost** | $0 (compute cost only) |
| **Output cost** | $0 (compute cost only) |
| **p50 latency** | 1–5 s depending on hardware (GPU required for acceptable latency) |
| **Data residency** | Fully on-premises; data never leaves operator infrastructure |
| **Zero data retention** | Yes — no third-party involvement at all |
| **SOC 2 / HIPAA** | N/A (operator's own infrastructure) |
| **GDPR** | Operator controls all data processing |
| **On-prem option** | Yes — this is the on-prem option |

**Strengths:** Absolute data privacy; no per-token cost; satisfies the most restrictive
compliance requirements without legal negotiation.

**Weaknesses:** Significantly lower output quality on complex log diagnostics (70B
models still lag GPT-4o/Claude on structured reasoning tasks); requires operator-side
GPU infrastructure; high operational burden for deployment and model updates.

---

### Summary Comparison Table

| | GPT-4o | Claude 3.5 Sonnet | AWS Bedrock | Self-hosted |
|---|---|---|---|---|
| **Input cost / 1M tokens** | $2.50 | $3.00 | ~$3.00 | ~$0 |
| **Output cost / 1M tokens** | $10.00 | $15.00 | ~$15.00 | ~$0 |
| **p50 latency** | ~500 ms | ~600 ms | ~800 ms | 1–5 s |
| **Data leaves operator env** | Yes (default) | Yes (default) | No | No |
| **EU residency** | Via Azure | Via Bedrock | Yes | Yes |
| **SOC 2 / HIPAA BAA** | Yes | Yes | Yes | N/A |
| **On-prem option** | No | No | Effectively yes | Yes |
| **Output quality (log diag.)** | ★★★★★ | ★★★★★ | ★★★★★ | ★★★☆☆ |

---

## Decision

**Pending legal/compliance sign-off from the enterprise pilot customer.**

Interim decision to unblock development:

> Implement the **provider abstraction layer** (Issue B) with a well-defined interface
> (`LLMProvider.generate(prompt: str) -> str`) before committing to a specific provider.
> Ship v1 with **AWS Bedrock (Claude 3.5 Sonnet)** as the default, since it satisfies
> the strictest anticipated data residency requirements without waiting for legal input.
> Operators may override the provider via configuration.

Rationale for the interim default:
- Bedrock keeps log content inside the operator's AWS account — this is the safest
  default from a compliance standpoint and requires no legal negotiation for most
  enterprise AWS customers.
- If the pilot customer's legal team approves direct API calls, switching to
  `claude-sonnet-4-6` direct reduces latency and simplifies the stack with a
  one-line config change.
- The abstraction layer means the default can be changed in a follow-up without
  touching feature code.

---

## Provider Abstraction Interface

All providers must implement the following interface. This is the boundary that
decouples the feature build from the provider decision.

```python
from abc import ABC, abstractmethod

class LLMProvider(ABC):
    @abstractmethod
    def generate(self, prompt: str, *, max_tokens: int = 1024) -> str:
        """Send prompt and return the model's text response."""
        ...

    @abstractmethod
    def name(self) -> str:
        """Human-readable provider identifier for logging."""
        ...
```

Concrete implementations live in `airflow/providers/llm/`:

```
airflow/providers/llm/
├── __init__.py
├── base.py              # LLMProvider ABC
├── openai.py            # OpenAIProvider
├── anthropic.py         # AnthropicProvider
├── bedrock.py           # BedrockProvider (default)
└── local.py             # OllamaProvider
```

Operators select the provider via `airflow.cfg`:

```ini
[llm]
provider = bedrock          # bedrock | anthropic | openai | local
model    = anthropic.claude-sonnet-4-6-20250219-v1:0
region   = us-east-1        # for bedrock
```

---

## Data Privacy Mitigations (all providers)

Regardless of which hosted provider is chosen, the following mitigations apply at
the application layer:

1. **Log scrubbing before dispatch** — a configurable redaction pass strips
   Airflow connection URIs, environment variable patterns matching `*_PASSWORD`,
   `*_SECRET`, `*_TOKEN`, and `*_KEY`, and common credential patterns before the
   prompt is sent.
2. **Operator opt-in** — the LLM feature is disabled by default. Operators must
   explicitly set `[llm] enabled = true` to activate it.
3. **Prompt auditing** — every outbound prompt and inbound response is logged
   (at DEBUG level) to the operator's own logging backend, never to a third-party
   service, so operators retain a full audit trail.
4. **No storage at provider** — only providers that offer zero-retention APIs
   (no training on inputs) are in scope for this feature.

---

## Consequences

- The feature build is **unblocked** — teams implement against the `LLMProvider`
  interface and are not blocked on the provider decision.
- Switching providers in future requires only a config change and a concrete
  implementation class; no feature code changes.
- AWS Bedrock as default means operators need an AWS account and Bedrock access;
  this should be documented as a prerequisite in the operator guide.
- Self-hosted (Ollama) is a supported option for air-gapped environments but is
  explicitly noted as lower quality; operators choosing it accept that trade-off.
- This ADR must be revisited and **Accepted** (or superseded) once the enterprise
  pilot customer's legal/compliance input is received.

---
