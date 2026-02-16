FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install system dependencies
# git is often required for git-dependencies or tools
RUN apt-get update && apt-get install -y git curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files for caching
COPY pyproject.toml uv.lock ./
COPY card-box-cg ./card-box-cg

# Install dependencies
RUN uv sync --frozen

# Environment setup
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# Default command
CMD ["tail", "-f", "/dev/null"]
