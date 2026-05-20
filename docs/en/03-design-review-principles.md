# Design Review Principles

This document is a review tool for [01-constitution.md](01-constitution.md).

It does not define new system ontology and does not replace constitutional clauses. Its job is to help decide whether a new design belongs in the constitution, in an explanatory document, in an implementation guide, or outside the main path entirely.

If this document conflicts with the constitution, the constitution wins.

## 1. Review Order

Review a new design in four steps:

1. **Judge the legal layer first.** Is it defining minimal ontology, authority boundaries, default legal effects, or unbreakable relationships? Or is it explanation, review, implementation, or demo material?
2. **Judge the plane.** Does it primarily belong to Agent, Turn, or complete semantics? Or does it describe lower-level structures such as projection, notification, runtime, or operator surface?
3. **Judge authority and effect.** Who gains what formal control authority? What material gains what default legal effect? Is that effect explicitly modeled?
4. **Judge whether implementation floated upward.** Does it disguise interface names, current implementation habits, runtime division of labor, demo structure, or a temporary workaround as an axiom?

If the first step shows that a design is not constitutional material, it should not be written into the constitution.

## 2. Ten Questions Before Entering The Main Path

Before a design enters the main path, answer at least these questions:

1. Is this constitution, explanation, review principle, or implementation/integration documentation?
2. Which plane does it belong to: Agent, Turn, or complete semantics?
3. Is it storing truth, or generating projection?
4. Does it promote a host, process, container, session, or network endpoint into a formal subject?
5. Who gains what control authority, and is the authority source explicitly modeled?
6. Does it confuse capability, availability, health, willingness to accept work, or another observation signal with authorization?
7. Does it let a requester, parent, observer, or operator automatically gain later control authority?
8. What object is supposed to gain what default legal effect, and has that effect been explicitly modeled?
9. Does it silently upgrade old Turn history, projection, push notification, or mere persistence into continuity, identity, authorization, or contract effect?
10. Can this rule be directly verified by lower-level design, implementation, and review?

## 3. How Constitutional Clauses Should Be Written

A good constitutional clause usually has three layers:

1. **Positive definition.** What the object is and what order it actively establishes.
2. **Boundary restriction.** What it does not automatically mean and what it does not replace.
3. **Normative consequence.** Which effects arise, and which effects do not arise by default.

If a clause is only negative, it usually has insufficient institutional meaning.

If a clause primarily lists interfaces, namespaces, fields, current data structures, or demo paths, it is usually no longer constitutional material.

## 4. Ontology And Boundary Review

### 4.1 Do Not Write Implementation Details As Ontology

These usually do not belong directly in the constitution:

- field names;
- HTTP paths;
- namespace prefixes;
- parser conventions;
- current runtime division of labor;
- temporary directory layout;
- demo names.

If an implementation detail is important enough to constrain long term, first restate it as ontology, authority boundary, default legal effect, or an unbreakable relationship. Otherwise it should not enter the constitution.

### 4.2 Do Not Freeze Strategy Space Too Early

The constitution should constrain which boundaries must hold. It should not decide the only allowed implementation shape in advance.

Valid constitutional propositions look like:

- control authority source must be explicit;
- historical materials do not automatically have legal effect;
- a child must not directly rewrite a parent.

Prematurely frozen implementation propositions look like:

- only one routing method is allowed;
- birth must bind one specific implementation entity;
- contract effect must be expressed through one specific data carrier.

Those usually belong in explanation or implementation layers.

## 5. Authority And Authorization Review

### 5.1 Separate Initiation, Observation, And Control

If a design defaults to "who initiated can later write, stop, or resume," treat it as implicit authority expansion.

If a design defaults to "who saw history is authorized to continue control," treat it as a boundary violation.

### 5.2 Capability Is Not Authorization

Reject or rebuild a design if it directly interprets any of these as a control-authority source:

- capability;
- availability;
- health;
- recent activity;
- willingness to accept work;
- aggregate score.

These can help admission, routing, and system understanding. They are not authorization.

### 5.3 Kernel Does Not Make Strategy Judgments

A design is feeding strategy back into the kernel if it requires the kernel to automatically decide:

- whether a child result satisfies business requirements;
- whether a parent should resume;
- whether an external fact should be absorbed;
- whether a complex orchestration succeeded.

## 6. Default Legal-Effect Review

### 6.1 Readable Does Not Mean Legally Effective

"Can be read" does not mean "automatically has legal effect."

CommonGround should be especially careful not to silently upgrade:

- old Turn history;
- semantics, context, and feed;
- progress or process records;
- projection;
- push notification;
- mere persistence.

If a key effect arises only because a record was stored, can be found by a parser, or happens to sit in a field, the design usually violates the constitution.

### 6.2 Default Effect And Explicit Modeling

Old Turn history should be treated by default only as:

- inspect material;
- audit material;
- reference material;
- shared observation.

If a design wants that material to carry continuity, identity proof, authorization proof, contract effect, or a kernel correctness precondition, the effect must be explicitly modeled.

### 6.3 Agent-Native Memory Boundary

A design has crossed the current constitutional boundary if it assumes CommonGround is the default truth source for:

- session continuity;
- internal thinking;
- long-term memory;
- runtime-local work state.

## 7. Projection, Push, And Historical Material

### 7.1 Projection

Projection can be powerful, rich, and productized. Its power does not allow it to define truth in reverse.

If the argument for a design is "the dashboard needs to display it this way, so kernel truth must be modeled this way," reject or remodel the design.

### 7.2 Push

Push is only an acceleration layer.

If a design cannot recover after a notification is lost, it should not enter the kernel correctness path.

### 7.3 Historical Material

Reading old history can be legal and valuable for:

- current Turn recovery judgment;
- audit;
- review;
- operator observation;
- shared observation absorption.

The restricted act is not reading. The restricted act is automatically gaining unmodeled effect after reading.

## 8. When To Amend The Constitution

These are more likely to require an amendment:

- a new minimal ontology appears;
- a new unbreakable relationship appears;
- a new default legal effect must be recognized long term;
- old clauses and long-running implementation reality have an unavoidable conflict.

These usually should not start with an amendment:

- current implementation is inconvenient;
- an interface name is already widely used;
- a demo is convenient;
- a parser or schema is temporarily sufficient;
- someone wants to freeze unvalidated strategy space early.

## 9. Closure

[01-constitution.md](01-constitution.md) legislates.

[02-three-plane-model.md](02-three-plane-model.md) interprets.

This document reviews: it helps identify what should enter the constitution, what should be pushed down, and what may be valuable but should not have constitutional effect.
