FROM 192.168.200.11:5000/library/rust:1-bookworm AS builder
WORKDIR /app
ARG GIT_SHA
ARG TARGETPLATFORM
ARG PROFILE=release
ARG FEATURES=python

RUN rm -f /etc/apt/sources.list.d/* && \
    . /etc/os-release && \
    echo "deb [trusted=yes] http://192.168.200.11:8081/repository/apt-debian-proxy ${VERSION_CODENAME} main" > /etc/apt/sources.list && \
    apt-get update && \
    apt-get -y install curl pkg-config unzip build-essential libssl-dev openssl \
    cmake clang wget postgresql postgresql-client supervisor sudo bash libsasl2-dev nodejs npm

# Cargo registry → Nexus proxy (refinery_cli 설치용)
COPY docker/cargo-nexus.toml /usr/local/cargo/config.toml

RUN cargo install refinery_cli

# Install pnpm via npm
RUN npm config set registry http://192.168.200.11:8081/repository/npm-proxy/ && \
    npm install -g pnpm@9.0.6

COPY docker/install_deps.sh /install_deps.sh
RUN sh /install_deps.sh

COPY webui/pnpm-lock.yaml webui/pnpm-lock.yaml
RUN cd webui && pnpm fetch

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

COPY crates crates
COPY webui webui

# vendored deps (cargo build 시 외부 접근 불필요)
COPY vendor vendor
COPY docker/cargo-config.toml /usr/local/cargo/config.toml

# set git hash
ENV VERGEN_GIT_SHA=$GIT_SHA
RUN cd webui && pnpm install && pnpm build

COPY docker/refinery.toml refinery.toml

ENV LIBRARY_PATH=/usr/local/lib \
    RUSTFLAGS="-L native=/usr/local/lib -Clink-arg=-Wl,-rpath,/usr/local/lib"

RUN service postgresql start && \
    sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;" && \
    sudo -u postgres createdb arroyo && \
    refinery migrate -c refinery.toml -p crates/arroyo-api/migrations && \
    CARGO_PROFILE_RELEASE_DEBUG=false cargo build --offline --features ${FEATURES} --profile ${PROFILE} --bin arroyo && \
    mv target/*/arroyo /arroyo


FROM 192.168.200.11:5000/library/debian:bookworm-slim as arroyo
WORKDIR /app

RUN rm -f /etc/apt/sources.list.d/* && \
    . /etc/os-release && \
    echo "deb [trusted=yes] http://192.168.200.11:8081/repository/apt-debian-proxy ${VERSION_CODENAME} main" > /etc/apt/sources.list && \
    apt-get update && \
    apt-get -y install libsasl2-2 ca-certificates curl pkg-config && \
    mkdir /config

COPY --from=builder /arroyo ./
COPY --from=builder /usr/local/bin/protoc /usr/local/bin/protoc
COPY --from=builder /app/include/google /usr/local/include/google
COPY --from=builder /app/python /python

RUN cp -r /python/bin/* /usr/local/bin/ && \
    cp -r /python/include/* /usr/local/include/ && \
    cp -r /python/lib/* /usr/local/lib/ && \
    cp -r /python/share/* /usr/local/share/ && \
    rm -rf /python && \
    ldconfig && \
    pip3 install --index-url http://192.168.200.11:8081/repository/pypi-proxy/simple/ --trusted-host 192.168.200.11 redis

ENV INSTALL_RUSTC=true \
    INSTALL_CLANG=true \
    ARROYO__API__RUN_HTTP_PORT=5115


EXPOSE 5115
ENTRYPOINT [ "/app/arroyo" ]
CMD ["cluster"]

FROM 192.168.200.11:5000/library/rust:slim-bookworm AS arroyo-full
WORKDIR /app
RUN rm -f /etc/apt/sources.list.d/* && \
    . /etc/os-release && \
    echo "deb [trusted=yes] http://192.168.200.11:8081/repository/apt-debian-proxy ${VERSION_CODENAME} main" > /etc/apt/sources.list && \
    apt-get update && \
    apt-get -y install libsasl2-2 ca-certificates curl pkg-config

ENV ARROYO__API__RUN_HTTP_PORT=5115

COPY --from=builder /arroyo ./
COPY --from=builder /usr/local/bin/protoc /usr/local/bin/protoc
COPY --from=builder /app/include/google /usr/local/include/google
COPY --from=builder /app/python /python

RUN cp -r /python/bin/* /usr/local/bin/ && \
    cp -r /python/include/* /usr/local/include/ && \
    cp -r /python/lib/* /usr/local/lib/ && \
    cp -r /python/share/* /usr/local/share/ && \
    rm -rf /python && \
    ldconfig
  

EXPOSE 5115
ENTRYPOINT [ "/app/arroyo" ]
CMD ["cluster"]
