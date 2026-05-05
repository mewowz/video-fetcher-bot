# A huge chunk of this Dockerfile is copied from uv's lead dev in 
# their docker exmample: https://github.com/astral-sh/uv-docker-example/blob/main/Dockerfile

FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 911 runner \
 && useradd --system --gid 911 --uid 911 --create-home runner

WORKDIR /srv/video-fetcher


ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=1
ENV UV_TOOL_BIN_DIR=/usr/local/bin

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

COPY --chown=runner:runner . /srv/video-fetcher
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked \
 && chown -R runner:runner /srv/video-fetcher/.venv


ENV PATH="/srv/video-fetcher/.venv/bin:$PATH"

USER runner

ENTRYPOINT [ "uv", "run", "main.py" ]
CMD [ "--modules", "all", "--log-stdout" ]
