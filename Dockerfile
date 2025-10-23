FROM ghcr.io/astral-sh/uv:0.9.4 AS uv
FROM python:3.13.0-alpine3.20

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
