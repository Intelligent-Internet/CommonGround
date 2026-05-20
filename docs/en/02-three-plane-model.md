# Three-Plane Model

This document explains the boundaries, positive meaning, and relationships of the Agent, Turn, and complete-semantics planes defined by [01-constitution.md](01-constitution.md).

It does not add system ontology or change the legal effect of the constitution. If this document conflicts with the constitution, the constitution wins.

## 0. How To Read This Document

The constitution gives formal law. It answers what must hold, what does not hold by default, and which relationships must not be broken.

This document answers another set of questions:

- On which plane do those clauses land structurally?
- Why are they written that way?
- What order do they actively establish?
- What implementation decisions are they deliberately not making?

If the constitution legislates, this document interprets.

## 1. Agent Plane

### 1.1 What Agent Means

An Agent is a stable logical subject in CommonGround that can be delegated to, bear responsibility, and produce semantic facts.

"Stable logical subject" has two meanings:

1. It must be a formal object in the collaboration system that can be referenced, authorized, and held accountable.
2. It cannot be directly replaced by operational substrate such as host, process, container, session, or network endpoint.

This means an Agent can be redeployed, moved to another host, or continue work in a different process without losing its logical identity.

### 1.2 Agent Implementation Shape Is Not Ontology

The constitution requires Agent to be a stable logical subject. It does not require one cognitive mechanism, model type, or interaction pattern.

CommonGround Agents may be backed by:

- LLM-based conversation agents;
- provision agents that launch other subjects;
- deterministic service agents that execute business rules;
- gateway agents that connect external systems;
- human-machine hybrids, script-driven actors, or other constrained runtimes.

These examples explain current implementations and integration styles. They are not a new ontology enumeration and not the only allowed classification.

The deciding question is not whether an object looks like a chatbot. The deciding question is whether it enters the collaboration order with its own stable identity, can be delegated to, authorized, held accountable, and produce formal semantic facts under that identity.

Therefore the system must not treat "LLM-driven" as the condition for Agent existence. It must also not treat "a current service, gateway, or worker process provides the capability" as proof that the process itself is a formal subject.

In product, interface, or deployment contexts, some non-conversational, non-prompt-facing Agents backed by controlled backend systems may be called Services, such as Admin Service, NanoBot Provision Service, or Gateway Service. That name only reduces confusion. A Service still lives in the Agent plane. It is not a fourth plane, and it does not bypass Agent authorization, audit, control boundary, or invalidation rules.

Concrete integration guidance should be written in current guides when it is part of the active public surface. Historical integration notes do not define current truth.

### 1.3 Boundary Between Agent Identity And Deployment Substrate

CommonGround may observe whether an Agent was recently active, from which ingress it connected, or which runtime currently serves it. These are collaboration and operations signals.

They may affect admission, routing, explanation, and troubleshooting. They do not automatically constitute formal identity and do not automatically constitute execution authority.

The system must not treat "a process is currently reachable" as "this process is the formal subject." It must not treat "an Agent is currently offline" as "its logical identity no longer exists."

### 1.4 Capability, Availability, And Authorization

Agents can declare capabilities, expose suitability, and report health, load, and willingness to accept work.

These facts have positive value:

- they help systems and humans understand who is suitable for which work;
- they help admission and routing make constrained judgments;
- they help operators observe the current work shape.

But they are not control authority.

Capability is a statement about what can be done. Authorization is the formal boundary around what control action is allowed. The constitution requires these to stay separate; otherwise capability descriptions silently become authority sources.

### 1.5 Agent Concurrency And Continuity Are Not Natural Defaults

Whether the same Agent can advance multiple Turns concurrently should not be decided by implementation accident.

If the system allows concurrency, it must answer:

- whether semantics are isolated;
- whether sessions are isolated;
- how execution authorization prevents conflicts;
- how historical material is read;
- which layer carries continuity effects.

Therefore "single-threaded Agent" is not a constitutional axiom, and neither is "naturally concurrent Agent." Both are design choices that must be explicitly modeled.

## 2. Turn Plane

### 2.1 What Turn Means

A Turn is the minimum durable work boundary in CommonGround.

It gathers a piece of formal work into a boundary that can be referenced, audited, and recovered. At minimum, the boundary should answer:

- where the work came from;
- how later control authority arises;
- what state it is in now;
- whether it has terminated;
- what its formal semantic boundary is.

A Turn is not a single message or a single synchronous reply. It is closer to a work shell that can be observed over time and traced causally.

### 2.2 Turn Birth Boundary

A Turn must be created through a controlled birth boundary.

The positive role of that birth boundary is to establish at once:

- the work boundary;
- the initial semantic boundary;
- the causal source;
- the later control-authority source or explicit resolution mechanism.

The key point is not that birth must bind one concrete implementation object. The key point is that after birth, who can continue control and under which institution must be explainable from the birth boundary.

### 2.3 Requester, Owner, And Controller

A Turn can involve different roles:

- requester: the party that causes birth;
- owner or other legal subject inside the later control boundary: the party that advances the Turn;
- observer or operator: a party that can observe or intervene under constrained conditions.

The constitution emphasizes that these roles must not be automatically collapsed.

The positive meaning is that the system can support delegation, supervision, derivation, and recovery without writing "who spoke first" as "who has authority forever."

### 2.4 Turn Lifecycle

A Turn can move through states such as waiting for execution, running, suspended while waiting for facts, and terminated.

The purpose is not merely to display a state machine. Lifecycle states provide formal context for recovery and accountability:

- when work may still advance;
- who may still advance it;
- which actions are no longer legal;
- which facts are already terminal.

The kernel may perform safety-oriented state convergence. It must not encode complex business strategy as a built-in automaton.

### 2.5 Turn Control Boundary And Recovery

Recovery must be grounded in durable facts.

When a Turn is suspended while waiting for external facts, later subjects should return to facts through durable feed, query, or another formal observation surface, then decide whether to continue.

The key is not whether the system sent an automatic notification. The key is that even if notification is lost, the system can return from durable truth to correct judgment.

### 2.6 Child Derivation

A Turn may derive a child Turn, but the child is a new work boundary, not a hidden continuation of the parent.

Therefore:

- the child has its own lifecycle;
- the child has its own complete semantics;
- the child completes its own work;
- the parent only observes child facts;
- whether the parent absorbs the child result must be decided by a legal subject inside the parent's control boundary.

This distinction is central to preventing CommonGround from sliding back into a central shared session pool.

## 3. Complete-Semantics Plane

### 3.1 Positive Meaning Of Complete Semantics

Complete semantics is not a slogan for storing everything. It is the semantic boundary formally owned by a Turn.

Its positive meaning is:

- to provide recoverable judgment material for the Turn;
- to create auditable records for the Turn's result and process;
- to provide semantic basis for later observation, dispute handling, and accountability.

Complete semantics is therefore a formal ownership relation, not merely a log container.

### 3.2 What Complete Semantics Can Carry

Complete semantics can carry many types of formal semantic material, including:

- initial input;
- observations actively absorbed by the current Turn;
- formal process records;
- tool or external observation summaries;
- final deliverables;
- error and termination reasons.

The point is not the list itself. The point is that once these materials enter complete semantics, they become semantic facts formally owned by that Turn.

### 3.3 Separate Ownership From Content Ontology

Complete semantics answers:

- which Turn owns which semantic facts;
- in which order and role those facts appear inside the Turn.

The content layer carries concrete content objects.

Complete semantics may reference the content layer, but the content layer does not replace semantic ownership. A content object reused in many places does not automatically belong to every Turn.

### 3.4 Initial Semantics Is A Birth Exception

At Turn birth, upstream can provide initial semantic context for the new Turn.

This exception exists because a new work boundary cannot be born in a complete contextual vacuum.

The purpose is to help the new Turn establish its own work boundary. It does not let upstream continue writing into that Turn's complete semantics after birth through an unmodeled channel.

### 3.5 Reading Complete Semantics Recovers Current-Turn Judgment

When a subject claims, resumes, or audits a Turn, it needs to read the Turn's existing complete semantics and causal facts to recover judgment for that Turn.

"Recover judgment" means:

- understand what has already happened in the Turn;
- identify which observations have been formally absorbed;
- judge what legal actions remain.

It does not mean restoring Agent-private state by default. It also does not mean upgrading old history into an Agent-native memory substrate.

## 4. Default Legal Effect

### 4.1 Readable Does Not Mean Legally Effective

Many materials in the system can be read, displayed, searched, summarized, and parsed.

But "readable" is not "automatically legally effective."

This distinction is critical in CommonGround because large amounts of material are durable. Without a distinction between existence and effect, the system would soon treat everything it can read as truth or authority.

### 4.2 Default Effect Of Old Turn History

Old Turn semantics, context, feed, process records, intermediate deliverables, observation summaries, and diagnostics provide by default:

- inspect;
- audit;
- reference;
- shared observation.

These effects are positive and necessary. Without them, the system loses explainability, recoverability, and auditability.

But by default, their effect stops there.

### 4.3 Effects That Do Not Arise By Default

Unless explicitly modeled otherwise, old Turn history does not automatically produce:

- Agent-private state recovery effect;
- identity-continuity proof effect;
- authorization-continuity proof effect;
- contract effect;
- other machine-authoritative effect.

The constitution does not restrict reading history. It restricts history automatically becoming an authority or continuity substrate.

### 4.4 CommonGround Is Not Agent-Native Memory Truth By Default

CommonGround stores public facts needed for cross-subject collaboration, not the default truth source for Agent-private internal state.

User-facing session continuity, internal thinking, long-term memory, runtime-local work state, and private continuity substrate after reboot should not be modeled as CommonGround truth by default.

This does not forbid an Agent or runtime from reading old history as external reference. It rejects only the default presumption that CommonGround automatically owns Agent-private continuity truth.

### 4.5 Machine-Authoritative Effect Must Be Explicitly Modeled

If a record is to produce a machine-authoritative effect, the system must be able to answer:

- what effect it produces;
- which boundary the effect acts on;
- why the effect does not arise merely from persistence.

"Explicit modeling" constrains how effect arises. It does not prescribe one schema, parser, or implementation path.

## 5. Relationships Between Planes

### 5.1 References Are Allowed; Collapse Is Not

The three planes can reference each other:

- a Turn can reference an Agent;
- complete semantics can reference a Turn;
- durable facts can reference Agents, Turns, and semantic objects.

But reference is not collapse.

Agent is not Turn.
Turn is not complete semantics.
Complete semantics is not execution authority.
Durable facts are not automatically effective legal relationships.

### 5.2 This Does Not Freeze Implementation Shape

The three-plane model defines minimal ontology and default effect boundaries. It does not define one implementation path.

It does not directly prescribe:

- which runtime to use;
- which host model to use;
- which concurrency policy to use;
- which projection shape to use;
- which carrier to use for contract facts.

The real rule is that whatever implementation is chosen must not cross these legal boundaries.

### 5.3 Positive Institutional Meaning

This structure is not only a restriction against mistakes.

It actively establishes:

- formal subjects;
- formal work boundaries;
- formal semantic ownership;
- formal control boundaries;
- formal default legal effects.

Because these are defined, upper-layer agent runtimes, projections, companions, operator governance, and integrations have a stable public order to attach to.

## 6. Closure

[01-constitution.md](01-constitution.md) answers what CommonGround kernel's legal order is.

This document answers why that legal order is structured this way, and how it unfolds across the Agent, Turn, and complete-semantics planes.

To review whether a design violates these boundaries, continue with [03-design-review-principles.md](03-design-review-principles.md).
