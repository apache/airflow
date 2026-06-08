# Team Retrospective: Milestone Mavericks

**Course:** CSS 566A, Software Management, Spring 2026
**Instructor:** Prof. Mia Champion
**Team:** Sohail Anwar (Scrum Master), Poorani T S, Sharan Saravanan
**Project:** AI-Assisted DAG Failure Triage Plugin for Apache Airflow

---

## Methodology

This retrospective follows the Scrum retrospective pattern (Lecture 6):
What went well, what did not go well, action items carried forward. Each
sprint section closes with the action items applied to the next sprint,
demonstrating the closed-loop Scrum learning cycle rather than isolated
sprint reviews. A cross-sprint synthesis section at the end captures
second-order lessons that emerged only when viewed across the full six
sprint arc.

---

## Sprint 1: Team formation, fork, HW1 to HW3 (Weeks 1 to 2)

**What went well**
- Team roles assigned cleanly using the Scrum framework: one Scrum
  Master, two engineers with overlapping product scoping coverage
- Apache Airflow selected as base repo because it offered both
  monorepo complexity and a real upstream community for persona
  evidence
- Initial Kanban board created with the 5-column flow recommended
  in Lecture 2

**What did not go well**
- Kanban board was submitted with draft cards rather than labeled
  issues, costing 2 points on HW2. Draft cards in GitHub Projects
  cannot carry repository-level labels, which we did not realize
  until after submission
- WIP limit on Backlog column was set too restrictive (5), causing
  artificial blockage during sprint planning

**Action items applied to Sprint 2**
- Removed Backlog WIP limit; kept In Progress at 3 and In Review at 5
- Established rule: every backlog item must be converted from draft
  to repo issue before sprint commitment so labels apply

---

## Sprint 2: Problem framing, persona research, plugin scaffold, code-quality fixes (Weeks 3 to 4)

**What went well**
- Three target personas grounded in upstream Apache Airflow community
  issues (platform engineer, ML engineer, on-call operational data
  user). This satisfied Working Backwards customer-obsession before
  any feature commitment
- Shift-Left CI gate established: Ruff, mypy, ASYNC lint, and pytest
  run on every commit (Lecture 8). This caught the ASYNC110 violation
  in the edge3 worker (PR #27) before manual review
- Milestone Tag Assistant CI workflow (issue #39) automated milestone
  hygiene, removing manual overhead from Scrum ceremonies

**What did not go well**
- User stories were written as task titles rather than in the proper
  "As a [role], I want [capability], so that [outcome]" form. HW5
  lost 5 points on this exact gap
- Backlog priority ranking was implicit rather than explicit, which
  later cost rubric points on the midterm PRFAQ paper

**Action items applied to Sprint 3**
- Reauthored 8 backlog issues into INVEST-compliant user stories
  with acceptance criteria and explicit INVEST checks (Lecture 4)
- Created a GitHub issue template (#24) to enforce the user story
  format on every future backlog item, removing the failure mode
  at the source rather than catching it in review

---

## Sprint 3: PRFAQ midterm, plugin scaffold, classifier implementation (Weeks 5 to 6)

**What went well**
- Heuristic classifier (PR for #47) shipped with TDD discipline:
  12 pytest cases written alongside implementation, all passing.
  This was our most disciplined sprint in terms of test-first development
- Log normalization layer (PR for #48) shipped with 4 tests, malformed
  line skip behavior covered explicitly rather than left to chance
- Remediation KB (PR for #49) seeded with 8 patterns spanning all 5
  failure categories, with 10 tests confirming category coverage and
  case-insensitive signature matching
- ARCHITECTURE.md documented the 4-layer pipeline with explicit SOLID
  rationale per layer, giving us a reference artifact for the midterm
  paper and the final report

**What did not go well**
- Midterm PRFAQ paper lost points in 4 rubric areas: customer
  experience voice slipped into past tense rather than forward-looking,
  backlog priority ranking was not explicit, novel synthesis read as
  description rather than framework application, and Figure 2 scope
  was too narrow
- Regrade request was declined, which made these specific failure
  modes concrete learning rather than recoverable points

**Action items applied to Sprint 4**
- Customer-facing language standardized to forward-looking present
  tense across all subsequent deliverables
- Backlog items reranked with explicit numeric priority before
  sprint planning, removing implicit assumptions
- Novel synthesis defined as: a strategic management decision must
  flow through a named framework and into a documented consequence,
  not framework definition pasted next to a project description

---

## Sprint 4: Classifier extension, summarizer, remediation checklist, UI rendering (Weeks 7 to 8)

**What went well**
- Architecture Decision Record raised (PR #55, for issue #30)
  documenting hybrid classification strategy: rule-based ranked
  classifier as primary, optional LLM summarization when
  local_only is disabled. Three alternatives compared with
  explicit trade-offs
- LLM provider decision documented (PR #51, for issue #36) with
  interim AWS Bedrock default and LLMProvider abstraction boundary,
  unblocking the summarization layer without committing to a
  provider before legal data-residency sign-off
- Remediation checklist generator implemented with session-local
  tried flag (issue #34), satisfying the no-persistence constraint
  for v1 while supporting the Sprint 5 UI surface
- Classifier returned ranked candidates by design, never asserting a
  single label. This was a deliberate Goodhart's Law application,
  discussed in detail in the cross-sprint synthesis below

**What did not go well**
- Sprint 4 milestone went past due by 13 days. Underestimation of
  the LLM provider decision review cycle caused the slip
- Usability session with two external Airflow users planned for
  Sprint 4 was deferred to Sprint 6, then dropped due to recruiting
  difficulty

**Action items applied to Sprint 5**
- Decision items that require external review (legal, security)
  are now flagged as blocked dependencies at sprint planning, not
  treated as ordinary backlog items
- Usability evidence will be sourced from upstream Airflow issue
  tracker comments rather than requiring live recruitment, since
  the issue tracker already contains real operator pain narratives

---

## Sprint 5: MVP demo, plugin packaging, sample failed DAG (Weeks 9 to 10)

**What went well**
- End-to-end demo committed (issue #37) with both a sample failed
  DAG and a runnable demo_triage.py script. The script exercises
  the full 4-layer pipeline in isolation, producing visible
  classification output and remediation steps for the final paper
- All 8 user stories closed with proper INVEST format, acceptance
  criteria checked, and linked PR or commit evidence in the
  Development panel. The HW5 user-story failure mode was fully
  remediated by Sprint 5

**What did not go well**
- GPG signing on the local dev environment blocked the first
  commit attempt for PR #1 documentation. Resolved by disabling
  commit signing at the repo level, but cost 20 minutes of
  unplanned debugging
- No evaluation harness was built to produce concrete numbers
  for the "under two minutes time-to-first-diagnosis" outcome
  metric. The metric is defensible by design but lacks empirical
  validation at submission time

**Action items applied to Sprint 6**
- Local dev environment standardization documented as part of the
  team_project README for future contributors
- Outcome metric framed in the final paper as a design target
  with the evaluation harness explicitly named as a v1.1
  deferred deliverable, rather than overclaiming validation

---

## Sprint 6: Hardening, retrospective, final paper (Weeks 11 to 12)

**What went well**
- Full team review of Kanban board, every issue closed with three
  team member comments demonstrating real collaboration evidence
- This retrospective document committed as a first-class repo
  artifact, not a one-off submission attachment
- Final paper structured to address every rubric dimension
  explicitly, with framework grounding called out per section
  rather than left implicit

**What did not go well**
- Some PRs (#21, #26 from earlier sprints) carried less explicit
  team-authored evidence than ideal, since they were merged before
  we established the three-comment per issue convention

---

## Aggregate Cross-Sprint Lessons (Novel Synthesis)

These lessons emerged only when the full six sprint arc was reviewed
together. Each is grounded in a course framework and tied to a
documented design decision rather than asserted in the abstract.

### 1. Goodhart's Law as a design constraint, not just a warning

Lecture material introduces Goodhart's Law as the risk that a measure
becomes a target and loses its meaning. We applied it as a positive
design constraint: the heuristic classifier in `classifier.py`
intentionally returns a ranked list of candidate categories rather
than a single label. If we had optimized the classifier for
"always return one category," operator visibility into ambiguous
cases would collapse, the metric "classification accuracy" would
improve, and real triage outcomes would worsen. Preserving operator
judgment when confidence is low is the engineering manifestation of
refusing to let the measure become the target. This decision is the
single most important design judgment in the project.

### 2. Working Backwards forced clarity before code

The PRFAQ midterm forced the team to articulate customer pain,
the outcome metric (median time-to-first-diagnosis under two minutes),
and acceptance language before writing any classifier code. Sprint 3
implementation was faster as a direct consequence: the failure
category taxonomy and remediation step format were already settled
in the PRFAQ, leaving only the technical execution. Working Backwards
shifted ambiguity discovery left by two sprints.

### 3. Shift-Left CI changed the failure economics

Ruff, mypy, ASYNC lint, and pytest gating on every commit caught
the ASYNC110 violation in the edge3 worker before human review.
The traditional alternative (catching in code review) would have
cost reviewer attention, a re-push cycle, and potential latent merge
of subtly broken code. The CI gate cost zero attention because it
ran in the background. Shift-Left is not just a quality practice,
it is an attention-budget reallocation toward higher-leverage review
questions.

### 4. INVEST violations compound

The HW5 user-story format failure looked like a 5-point procedural
loss. In practice it cascaded: backlog items that were not Negotiable
or Testable led to under-specified sprint commitments, which surfaced
as midterm rubric losses in the backlog priority ranking dimension.
A single procedural shortcut on one assignment created downstream
rubric damage two assignments later. The lesson is that INVEST is
not a documentation tax, it is a load-bearing structural beam.

### 5. SOLID applied to AI integration boundaries

The LLM provider abstraction (LLMProvider interface, decided in
PR #51) is a direct application of the dependency inversion principle.
We deferred the OpenAI versus Bedrock versus local-model choice
until legal sign-off without blocking the summarization layer
implementation. SOLID is most valuable not when applied to a single
class, but when applied at the boundary where the system meets an
externally evolving dependency.

---

## Looking Forward

The team's Scrum practice matured visibly across the six sprints.
Sprint 1 ceremonies were performative, Sprint 6 ceremonies were
structural. The retrospective ritual itself, run honestly each
sprint, was the single highest-leverage practice we adopted.

For future projects, the team will carry forward three practices
explicitly:

1. Define the outcome metric and the customer pain narrative
   before the first commit, in PRFAQ form
2. Treat INVEST as a structural requirement at backlog grooming,
   not a documentation check at story closure
3. Use ranked candidate outputs anywhere a single-label decision
   would collapse operator visibility, applying Goodhart's Law as
   a design constraint