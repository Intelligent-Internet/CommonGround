# Security Policy

## Reporting A Vulnerability

Do not report suspected vulnerabilities in public issues.

Use GitHub's private vulnerability reporting or repository security advisory flow when it is available. If that private path is unavailable, open a public issue titled `Security contact request` with no vulnerability details so maintainers can move the report to a private channel.

In the private report, include:

- affected version or commit;
- affected component;
- reproduction steps;
- expected impact;
- whether any credentials or private data may have been exposed.

## Credential Handling

CommonGround Agent credentials are bearer secrets. Do not paste them into prompts, issue comments, PRs, logs, docs, work-memory manifests, or test fixtures.

Treat the following as secrets in public material:

- AgentCredential bearer tokens;
- Admin Service bearer tokens;
- active claim tokens and claim files;
- join codes and invitation codes;
- `PYPI_API_TOKEN` and other package registry upload tokens;
- private PostgreSQL DSNs, passwords, and local token-file paths.

Use placeholders in public material:

```text
<agent_credential_token>
<admin_service_bearer_token>
<claim_token_or_claim_file>
cgjoin_...
postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

If a real token, password, key, or private DSN is committed or posted publicly, revoke or rotate it before continuing release work.

## Supported Versions

Only the active `v3-preview` / `v3r1` implementation line is supported.
