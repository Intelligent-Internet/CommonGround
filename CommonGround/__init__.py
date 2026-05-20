from __future__ import annotations


def __getattr__(name: str):
    if name in {"KernelApp", "build_kernel_app"}:
        from .app import KernelApp, build_kernel_app

        return {"KernelApp": KernelApp, "build_kernel_app": build_kernel_app}[name]
    raise AttributeError(name)


__all__ = ["KernelApp", "build_kernel_app"]
