# Single image for both dagster-webserver and dagster-daemon — docker-compose.yml
# runs it as two containers sharing $DAGSTER_HOME (SQLite storage) via a volume.

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

ENV DAGSTER_HOME=/opt/dagster/home \
    UV_COMPILE_BYTECODE=1

WORKDIR /opt/dagster/app

# dependency layer first so code changes don't re-resolve packages
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY deploy/workspace.yaml ./
COPY .dlt/config.toml ./.dlt/config.toml
RUN uv sync --frozen --no-dev

# instance config; SQLite storage paths point into the dagster_data volume
RUN mkdir -p "$DAGSTER_HOME"
COPY deploy/dagster.yaml "$DAGSTER_HOME/dagster.yaml"

ENV PATH="/opt/dagster/app/.venv/bin:$PATH"

EXPOSE 3000
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
