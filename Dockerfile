# CI passes DOCKER_REGISTRY=registry.hpc.ut.ee/mirror/ so both base images come
# through the pull-through cache the publish job is logged into. The uv image
# used to be pulled from ghcr.io, which CI has no credentials for and which
# timed out on its token endpoint (job 814758); astral publishes the same image
# to Docker Hub under astral/uv.
ARG DOCKER_REGISTRY=docker.io/
FROM ${DOCKER_REGISTRY}astral/uv:0.9.4 AS uv
FROM ${DOCKER_REGISTRY}python:3.13.0-alpine3.20

# Install system dependencies
RUN apk add --no-cache \
    git \
    curl \
    build-base

# Install uv
COPY --from=uv /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy source code
COPY . .

# Bump versions when building a tagged release (safety net for manual tags)
ARG VERSION=
RUN if [ -n "$VERSION" ]; then python3 scripts/bump_versions.py "$VERSION"; fi

# Install dependencies and build workspace
RUN uv sync --all-packages --no-dev

# Create non-root user
RUN adduser -D -s /bin/sh waldur

# Set ownership
RUN chown -R waldur:waldur /app

# Switch to non-root user
USER waldur

# Set environment variables
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD waldur_site_diagnostics || exit 1

# Set entrypoint and default command
ENTRYPOINT ["waldur_site_agent"]
CMD ["--help"]
