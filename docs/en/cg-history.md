# CommonGround History

This page is background material. It explains why the current CommonGround shape exists, but it is not an implementation contract.

For current rules, read [01-constitution.md](01-constitution.md), [02-three-plane-model.md](02-three-plane-model.md), and [03-design-review-principles.md](03-design-review-principles.md).

## 1. Why CommonGround Moved Away From Central Control

Early CommonGround designs started from a much stronger orchestration assumption: the system could own workflow structure, task decomposition, and the lifecycle of most participating agents.

That assumption became less useful as external agents became stronger and more independent. In practice, many capable agents live inside other runtimes, products, tools, or organizations. CommonGround cannot assume it owns their internal memory, scheduler, process model, or lifecycle.

The design pressure was therefore clear:

- keep a shared collaboration substrate;
- stop pretending the substrate owns every participant;
- record facts and boundaries instead of directly controlling all behavior;
- make external agents able to cooperate without being absorbed into a central hierarchy.

## 2. The Ledger Turn

The key simplification was to treat CommonGround as a ledger-like substrate.

The useful durable questions became:

- Which stable agent identity is participating?
- Which durable work boundary is being created or advanced?
- What semantic facts, references, and outcomes were produced?
- What causal relationship connects one piece of work to another?

This moved CommonGround away from hard-coded workflow assumptions and toward a shared record of collaboration.

The important product insight was not "remove orchestration." It was "put orchestration at the right layer." Supervisors, provisioners, planners, reviewers, human operators, portals, and runtime companions can exist, but they should not become hidden kernel ontology unless they are truly part of the durable CommonGround law.

## 3. Non-Invasive Memory

One of the most important historical corrections was the rejection of invasive memory writes.

Earlier designs allowed lower-level work to directly modify a parent or requester memory space. That created tight coupling and made responsibility hard to audit.

The current direction keeps the better idea:

- work produces records;
- results are inspectable;
- a parent, requester, or later agent can decide what to trust, reuse, summarize, or ignore;
- intermediate observations do not automatically become another agent's private memory or final truth.

This is why Turn-owned semantic records and public work knowledge matter. They create reusable evidence without pretending that every consumer must accept the result automatically.

## 4. Why The Current Kernel Is Small

The current CommonGround foundation deliberately keeps the formal kernel small:

- Agent is the stable logical actor.
- Turn is the minimal durable work boundary.
- Complete semantics belong to the Turn.
- Projection can help readers, but projection is not truth.
- Push can accelerate awareness, but pull/read remains the correctness baseline.

This is the main constitutional convergence. CommonGround is not the whole socio-technical organization. It is the shared coordination kernel that lets many such organizations and runtimes cooperate.

## 5. What Moved Out Of The Kernel

Several useful ideas remain important but belong above or beside the kernel:

- team topology;
- planning and critique loops;
- provisioning policy;
- runtime-local subagents;
- operator policy;
- product UX;
- portal and management read models;
- private agent memory.

Keeping these outside the kernel is not a downgrade. It prevents implementation convenience from becoming permanent law.

## 6. Current Reading Of The History

The historical arc is:

1. Start with strong orchestration and workflow assumptions.
2. Discover that strong external agents cannot be safely owned by the substrate.
3. Reduce CommonGround to durable facts, boundaries, identity, and causality.
4. Rebuild higher-level collaboration as explicit layers above that foundation.

That is the part of the history worth preserving.

The details of old service routes, compatibility windows, and credential experiments are not current truth. They should stay out of the active documentation surface unless a new design explicitly re-adopts them.
