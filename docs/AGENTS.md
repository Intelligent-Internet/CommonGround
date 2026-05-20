# Documentation Guidelines

## Language

Active public documentation is split into aligned language trees:

- `docs/en/`: English.
- `docs/zh/`: Chinese.

Keep paths aligned where practical. `docs/index.md` is the language selector.

## Active Surface

The active documentation surface is intentionally small. It keeps durable CommonGround foundation documents plus concise current guides and references.

Inside `docs/`, use this priority order:

1. `en/01-constitution.md` / `zh/01-constitution.md`
2. `en/02-three-plane-model.md` / `zh/02-three-plane-model.md`
3. `en/03-design-review-principles.md` / `zh/03-design-review-principles.md`
4. `en/introduction/` / `zh/introduction/`
5. `en/guides/` / `zh/guides/`

The three foundation documents use numbered filenames to make the priority order visible at the file level. Keep guides, references, and introduction docs unnumbered unless their directory has a separate local ordering convention.

## Placement

- New-reader orientation belongs in `en/introduction/` and `zh/introduction/`.
- Minimal current operational guidance belongs in `en/guides/` and `zh/guides/`.
- Long-lived rules should converge into the constitution, three-plane model, or design review principles.
- Background notes can explain design history, but they do not outrank the current truth documents.

## Avoid Duplicate Truth

Do not redefine the same rule in README, introduction, and historical notes. RFCs can record reasoning and implementation paths, but they do not outrank the current truth documents.
