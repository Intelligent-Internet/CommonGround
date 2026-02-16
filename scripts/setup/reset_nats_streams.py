import asyncio
import os
import ssl
from typing import Any, Dict, Optional

import nats
from scripts.utils.config import load_toml_config

from core.config import PROTOCOL_VERSION


def _resolve_nats_servers(cfg: Dict[str, Any]) -> list[str]:
    env_servers = os.environ.get("NATS_SERVERS")
    if env_servers:
        parsed = [s.strip() for s in env_servers.split(",") if s.strip()]
        if parsed:
            return parsed
    nats_cfg = cfg.get("nats", {}) if isinstance(cfg, dict) else {}
    servers = nats_cfg.get("servers")
    if isinstance(servers, list):
        parsed = [s.strip() for s in servers if isinstance(s, str) and s.strip()]
        if parsed:
            return parsed
    return ["nats://localhost:4222"]


async def _maybe_delete_stream(js, name: str) -> bool:
    try:
        await js.stream_info(name)
    except Exception:
        return False
    await js.delete_stream(name)
    return True


def _build_tls_kwargs(nats_cfg: Dict[str, Any]) -> Dict[str, Any]:
    tls_enabled = bool(nats_cfg.get("tls_enabled"))
    if not tls_enabled:
        return {}

    cert_dir = nats_cfg.get("cert_dir") or "./nats-js-test"
    ca_file = nats_cfg.get("ca_file") or os.path.join(cert_dir, "ca.crt")
    cert_file = nats_cfg.get("cert_file") or os.path.join(cert_dir, "client.crt")
    key_file = nats_cfg.get("key_file") or os.path.join(cert_dir, "client.key")

    missing = [p for p in (ca_file, cert_file, key_file) if not os.path.exists(p)]
    if missing:
        missing_str = ", ".join(missing)
        raise RuntimeError(
            f"TLS is enabled but certificate files are missing: {missing_str}. "
            "Please place ca.crt, client.crt, and client.key under config.toml nats.cert_dir, "
            "or configure ca_file/cert_file/key_file explicitly."
        )

    ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    ctx.load_verify_locations(cafile=ca_file)
    ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
    ctx.check_hostname = False
    return {"tls": ctx}


async def reset_streams(*, extra_streams: Optional[list[str]] = None) -> None:
    cfg = load_toml_config()
    if not cfg:
        raise RuntimeError("config.toml not found")
    nats_servers = _resolve_nats_servers(cfg)
    nats_cfg = cfg.get("nats", {}) if isinstance(cfg, dict) else {}

    cmd_stream = f"cg_cmd_{PROTOCOL_VERSION}"
    evt_stream = f"cg_evt_{PROTOCOL_VERSION}"
    streams = [cmd_stream, evt_stream]
    if extra_streams:
        streams.extend([s for s in extra_streams if isinstance(s, str) and s.strip()])

    tls_kwargs = _build_tls_kwargs(nats_cfg)
    proto = "tls" if tls_kwargs else "plain"
    print(
        f"[reset_nats_streams] NATS={nats_servers} PROTOCOL_VERSION={PROTOCOL_VERSION} proto={proto}"
    )
    nc = await nats.connect(servers=nats_servers, **tls_kwargs)
    try:
        js = nc.jetstream()
        # If there is an old stream with legacy naming overlapping current subjects, do a subject->stream reverse lookup.
        # A common case is an old stream like "cg-stream" capturing cg.v1r3.*.*.cmd.>.
        probe_subjects = [
            f"cg.{PROTOCOL_VERSION}.probe.probe.cmd.agent.worker_generic.wakeup",
            f"cg.{PROTOCOL_VERSION}.probe.probe.evt.agent.probe.task",
        ]
        for subj in probe_subjects:
            try:
                name = await js.find_stream_name_by_subject(subj)
            except Exception:
                continue
            if isinstance(name, str) and name and name not in streams:
                streams.append(name)

        for name in streams:
            deleted = await _maybe_delete_stream(js, name)
            print(f"[reset_nats_streams] delete_stream {name}: {'Deleted' if deleted else 'Skipped (not found)'}")
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(reset_streams())
