FROM rust:1.91-slim-bookworm AS build

ENV SQLX_OFFLINE=true
WORKDIR /src

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked

COPY .sqlx ./.sqlx
COPY src ./src
RUN touch src/main.rs \
    && cargo build --release --locked

FROM python:3.13-slim-bookworm AS pgmigrate

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /venv \
    && /venv/bin/pip install --no-cache-dir yandex-pgmigrate==1.0.13

FROM python:3.13-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/venv/bin:$PATH"

COPY --from=pgmigrate /venv /venv
COPY --from=build /src/target/release/orders /orders

COPY pgmigrate /pgmigrate
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/docker-entrypoint.sh"]
