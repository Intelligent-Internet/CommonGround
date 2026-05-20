class KernelError(Exception):
    """Base error for kernel contract violations."""


class NotFoundError(KernelError):
    pass


class ConflictError(KernelError):
    pass


class FencingError(KernelError):
    pass


class InvariantError(KernelError):
    pass


class UnauthorizedError(KernelError):
    pass


class ForbiddenError(KernelError):
    pass
