from __future__ import annotations

from CommonGround.env import load_local_env


def main() -> None:
    import uvicorn

    from .config import ServiceConfig
    from .http import create_service_app

    load_local_env()
    config = ServiceConfig.from_env()
    uvicorn.run(create_service_app(config=config), host=config.host, port=config.port, log_level=config.log_level)


def create_service_app(*args, **kwargs):
    from .http import create_service_app as _create_service_app

    return _create_service_app(*args, **kwargs)


def __getattr__(name: str):
    if name == "ServiceConfig":
        from .config import ServiceConfig

        return ServiceConfig
    raise AttributeError(name)


__all__ = ["ServiceConfig", "create_service_app", "main"]
