# Constitution

## 0. Position

This document is the highest-level constraint document for the CommonGround kernel.

It defines only the system ontology, authority boundaries, default legal effects, and design axioms that must not be broken. It does not record migration history, freeze implementation shape, describe concrete integration flows, or promote temporary implementation techniques into long-term ontology.

If later documents, implementations, or interfaces conflict with this document, the design should first return to these axioms. If sustained implementation evidence proves that an axiom is wrong, the constitution should be amended before the implementation is expanded.

## 0.0 First Principle

CommonGround follows this first principle:

> Assume nothing beyond what constraints demand.

System design and constitutional design should not assume anything beyond the minimum demanded by the constraints.

This principle constrains not only system design, but also how the constitution is written, interpreted, and amended. Every addition, interpretation, or amendment should be treated as a minimum update: write one more step only when new constraints, sustained counterexamples, or long-running conflicts force it. Do not freeze local implementation preferences, temporary workarounds, or upper-layer strategy space into fundamental law.

From that principle, this document follows these constitutional rules:

1. Only constraints that are necessary for the system to exist and run consistently are eligible for the constitution.
2. The constitution defines only minimal ontology, authority boundaries, default legal effects, and unbreakable relationships. Implementation, process, deployment, strategy, and temporary techniques must not pretend to be ontology.
3. Subjects, authority, ownership, responsibility, invalidation conditions, and legal effects must be modeled explicitly. Effects that are not explicitly granted must not arise automatically.
4. Clauses must not contradict each other. Every clause must be directly verifiable by lower-level design, implementation, and review.
5. Before an upper constraint is broken, the constitution must not freeze variable implementation shapes too early. Amendments must be driven by new constraints, sustained counterexamples, or long-running conflicts. Temporary exceptions must be declared through explicit amendments.

All clauses below primarily defend the architecture from excessive assumptions during evolution.

## 0.1 Amendment Mechanism

Constitutional amendments are explicit, auditable exceptions to this document.

An amendment may temporarily relax some restrictions within a clear scope, or temporarily add assumptions for PoC or MVP validation.

An amendment must not silently change the long-term system ontology, and must not upgrade a temporary implementation technique into a default axiom.

Every amendment must state the applicable version or phase, the clauses it relaxes or adds, temporary assumptions, risks, exit criteria, and related issues or decision records.

Within its declared scope and term, an amendment takes precedence over the corresponding clauses in this document. Outside that scope, this document returns as the default constraint.

There are currently no active constitutional amendments. Retired amendments are historical audit material only; they do not outrank this document.

## 1. Kernel Position

This section defines CommonGround's legal responsibility and boundary as a kernel.

### 1.1 CommonGround Is A Public-Fact And Causal-Coordination Kernel

The legal responsibility of the CommonGround Kernel is to establish a public fact substrate and a recoverable causal coordination foundation for multi-Agent collaboration.

It registers formal participants, establishes durable work boundaries, stores Turn-owned semantics and key causal facts, and provides shared ground for pull-first recovery, observation, and audit.

It is not an Agent brain, business orchestrator, deployment system, or message bus. It therefore does not automatically become the default carrier for Agent-private memory, business strategy, or runtime continuity.

It should not smuggle in business orchestration, topology preference, or automatic flow assumptions. At the ontology level, the common foundation it provides is Agent, Turn, and complete semantics.

It does not reason and does not overstep by deciding for Agents. It should keep a minimal shape so different forms of Agent collaboration can rest on it.

## 2. Legal Ontology

This section defines CommonGround's legal ontology and its basic ownership relations.

### 2.1 Agent Is A Stable Logical Executor

An Agent is a formal executor in CommonGround that can be delegated to, can bear responsibility, and can produce semantic facts.

An Agent's existence does not depend on whether it is backed by AI, a human, a service, a script, an organization process, or a hybrid system. Any node that wants to enter CommonGround as an entity that can be delegated to, authorized, held accountable, and produce formal semantic facts must be modeled homomorphically in the Agent plane. Human nodes are not ontology exceptions.

To avoid ambiguity in product, interface, or deployment contexts, lower-level docs may call some non-conversational, non-prompt-facing Agents backed by deterministic services or controlled backend systems a Service, such as an Admin Service. Service is not a new ontology outside Agent, and it does not introduce a new subject plane. If a Service needs to enter CommonGround as a subject that can be authorized, held accountable, and produce machine-authoritative effects, it must still be modeled in the Agent plane and obey the same control boundary, authorization, audit, and invalidation rules.

Agent identity continuity depends on logical identity, not on a specific host, process, container, session, working directory, or network endpoint.

Deployment shape can change while logical identity remains continuous. Physical reachability, online status, and recent activity do not automatically equal execution authority.

### 2.2 Turn Is The Minimum Durable Work Boundary

A Turn is the minimum durable work boundary in CommonGround. It is also the legal boundary for a unit of work, delegation, interaction, or recovery loop.

Every Turn must have an explicit source, a source of later control authority, a lifecycle state, a terminal state, and complete semantics owned by that Turn.

A Turn can be long-running, asynchronous, suspendable, and able to derive new work boundaries. It is not a synchronous reply, and the act of initiation does not automatically grant later control authority to the initiator.

### 2.3 Complete Semantics Belongs To A Turn

Complete semantics is the semantic-fact boundary formally owned by a Turn.

It carries that Turn's input, observations actively absorbed by the current Turn, formal process records, final deliverables, and termination reasons. This makes the Turn's work process and result recoverable, auditable, and accountable.

Complete semantics must be owned by Turn, not by Agent, host, view, content layer, or another external object.

### 2.4 Duality Of Turn Semantic Content

Semantic material carried by a Turn can be divided by source into two categories, and the categories have different durability obligations.

Cross-Agent boundary I/O must be durably stored as the minimum recoverable fact. This includes root input, dispatch input, child bootstrap, child final deliverable, final parent absorption of a child result, terminal final payload, and key I/O in request/reply semantics. These materials carry responsibility, causality, recovery, and audit. They are not transient caches in runtime or Agent-private layers.

Public work-process records produced by an Agent inside a Turn should be reported to the current Turn on a best-effort basis. These include key decisions, actively absorbed observations, handoff reasons, validation evidence, artifact references, and failure reasons. Their absence should not block deliverable production, and should not automatically invalidate completed work.

This clause does not change the default legal effect that Agent-private internal state is not CommonGround truth. That private state includes LLM token-level reasoning, chain-of-thought, runtime-local scratchpads, and session continuity substrate.

## 3. Control Order

This section defines how control authority, execution authority, derivation, and judgment boundaries arise.

### 3.1 Initiation Does Not Imply Control

The subject that initiates a Turn can cause the Turn to be born, but does not thereby gain the authority to advance, stop, resume, or write semantics to that Turn after birth.

After a Turn is born, every later control action must be judged against that Turn's legal control boundary. It must not be derived automatically from who initiated the Turn.

The Turn birth boundary must explicitly establish how later control authority arises, or explicitly establish the later resolution mechanism.

After birth, control authority must not be implicitly reassigned because of implementation accident, observation signals, historical visibility, call location, or unmodeled default rules.

If more complex delegation, handoff, arbitration, or resolution mechanisms are needed in the future, they must enter the system as explicit institutions. They must not arise silently through implementation convention.

### 3.2 Execution Authority Must Be Temporary, Verifiable, And Invalidatable

Authority to advance a Turn lifecycle or authoritatively write Turn semantics must be protected by currently valid execution authorization.

That authorization must be verifiable, invalidatable, not reusable after invalidation, and bound to the concrete Turn's control boundary.

Invalid execution authorization does not automatically prove that the whole Agent is dead. It only means the subject cannot continue advancing that Turn with the old authorization.

### 3.3 Capability Is Not Authorization

Capability describes what work an Agent is suitable for. Authorization describes which actions a subject is allowed to control.

Having a capability does not authorize control over arbitrary Turns. Capability declarations, availability, health, willingness to accept work, and other observation signals cannot replace formal authorization.

If a capability is claimed to belong to an Agent, the business semantics of that capability must not be impersonated by the CommonGround kernel. The kernel may store truth, validate boundaries, and carry data published by Agents, but it must not pretend to answer capability semantics that should be closed by that Agent or by its constrained integration subject.

### 3.4 Child Derivation Must Be Explicit

A Turn may derive a child Turn, but the derivation relation must be explicitly recorded as causality and must be durably observable later.

A child has its own work boundary, lifecycle, control boundary, and complete semantics. Child completion does not automatically advance the parent, and the child must not directly rewrite the parent's complete semantics or lifecycle.

Whether a parent absorbs a child result must be actively decided by a legal subject inside the parent's control boundary after observing the facts.

### 3.5 Kernel Records Facts, Not Strategy Judgments

The kernel's responsibility is to stably store public facts, key causal relationships, and explicit control-boundary states.

The kernel can record that a Turn has terminated, an authorization has expired, a child has completed, or a fact has appeared.

The kernel does not decide on behalf of upper-layer subjects whether a result satisfies business conditions, whether a fact should be absorbed, whether a parent should resume, or whether a complex collaboration strategy has succeeded.

### 3.6 Human Nodes Are Not Control Exceptions

Actions from humans, operators, interfaces, group chats, external platform accounts, or trusted ingress points must still pass through explicit control boundaries if they advance Turn lifecycle, authoritatively write Turn semantics, change control boundaries, or produce another machine-authoritative effect.

Human origin, UI location, platform account, group-chat context, or trusted ingress does not automatically grant control authority and does not automatically produce legal effect above other formal subjects.

### 3.7 Kernel Must Not Interpret Record Payloads

The kernel may stably store a record envelope, including owning Turn, actor, claim, write order, content reference, and time. It must not interpret business fields outside that envelope.

The semantics of a record payload, including whether a field means approval, selection, valid evidence, or worth reuse, must be interpreted outside the kernel by the current Agent, another Agent, a human, an LLM-enabled Agent, an external system, a projection, or Knowledge Team.

The kernel should not automatically produce machine-authoritative effect because a payload contains a specific field name or value.

This clause does not prohibit schema validation, indexing, or projection over the record envelope. It prohibits the kernel from treating business semantics inside payloads as its own judgment basis.

## 4. Legal-Effect Order

This section defines the default legal effect of system materials and which effects do not arise by default.

### 4.1 Legal Status Of Projection And Push

Timelines, dashboards, tree views, wait objects, aggregate views, and other read views may exist. They are projections, not kernel truth.

Projection can help humans and systems understand facts, but it must not define facts in reverse.

Similarly, notifications, doorbells, webhooks, buses, and other push mechanisms are acceleration layers only. They signal that facts may be worth reading. They are not facts themselves and must not be the sole precondition for system correctness.

### 4.2 Default Effect Of Old Turn History

Old Turn semantics, context, feed, process records, intermediate deliverables, observation summaries, and diagnostics provide inspect, audit, reference, and shared-observation effects by default.

Current subjects may read, cite, and absorb them to understand context, recover judgment for the current Turn, or support later audit.

Unless explicitly modeled otherwise, these historical materials do not automatically produce Agent-private state recovery, identity-continuity proof, authorization-continuity proof, contract effect, or another machine-authoritative effect.

### 4.3 CommonGround Is Not Presumed To Be Agent-Native Memory Truth

CommonGround carries the public facts needed for cross-subject collaboration. It is not the default truth source for private Agent internal state.

User-facing session continuity, internal thinking, long-term memory, runtime-local work state, and private continuity substrate after reboot should not be modeled as CommonGround truth by default.

This constraint does not forbid an Agent or runtime from reading old Turn history as external reference. It limits CommonGround's default truth status; it does not close upper-layer strategy space.

### 4.4 Machine-Authoritative Effect Must Be Explicitly Modeled

If a record needs to produce machine-authoritative effect on the current Turn, a current contract, a current control boundary, or another formal relationship, that effect must be explicitly modeled.

The effect must not be inferred merely because a record was persisted, included in semantics or context, programmatically extractable, found in history, or placed in a convenient carrier.

Explicit modeling constrains how legal effect arises. It does not freeze a single implementation shape.

## 5. Interpretation And Review Entry Points

This document carries formal law only.

The explanatory expansion of the three planes is [02-three-plane-model.md](02-three-plane-model.md). The design review checklist is [03-design-review-principles.md](03-design-review-principles.md).
