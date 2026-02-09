FROM python:3.13-alpine
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY . /build
RUN uv pip install --system --no-cache /build && rm -rf /build
ENTRYPOINT ["clickhouse-optimizer"]
CMD ["--help"]
