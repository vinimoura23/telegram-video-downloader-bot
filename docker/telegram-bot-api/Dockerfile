FROM debian:bookworm-slim AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        cmake \
        g++ \
        git \
        gperf \
        libssl-dev \
        make \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/telegram-bot-api

RUN git clone --recursive https://github.com/tdlib/telegram-bot-api.git . \
    && mkdir build \
    && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local .. \
    && cmake --build . --target install -j"$(nproc)"


FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        libstdc++6 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/local/bin/telegram-bot-api /usr/local/bin/telegram-bot-api

RUN useradd --system --create-home --home-dir /var/lib/telegram-bot-api botapi \
    && mkdir -p /var/lib/telegram-bot-api/tmp \
    && chown -R botapi:botapi /var/lib/telegram-bot-api

USER botapi
WORKDIR /var/lib/telegram-bot-api

EXPOSE 8081

ENTRYPOINT ["telegram-bot-api"]
