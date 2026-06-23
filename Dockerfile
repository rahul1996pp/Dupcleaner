# syntax=docker/dockerfile:1

# ---- Build stage ----
# go.mod declares `go 1.25.0`, so build with the matching toolchain on Alpine.
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Cache the module graph: download deps before copying source so this layer
# is reused unless go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source. This includes index.html, which is compiled
# into the binary via //go:embed in main.go, so it must be present at build time.
COPY . .

# CGO_ENABLED=0: the app is pure Go (HEIC decoding runs via a wazero/wasm
# runtime, not a C library), so a static binary needs no libc and runs as-is
# on the minimal Alpine runtime image below.
# -trimpath strips local filesystem paths from the binary for reproducibility.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /dupcleaner .

# ---- Final stage ----
FROM alpine:3.20

# ffmpeg + ffprobe power video perceptual matching; chromaprint provides fpcalc
# for audio matching. The app shells out to these, so they must be on PATH.
# ca-certificates is included for any outbound TLS the app may perform.
RUN apk add --no-cache ffmpeg chromaprint ca-certificates

# /data is the working directory and the volume mount point. The app writes its
# runtime state relative to CWD (cache.json, session.json, tools.json,
# rules.json, dupcleaner.log, thumbs/), so mounting /data persists that state
# and is also where host media is typically mounted for scanning.
WORKDIR /data
VOLUME ["/data"]

COPY --from=builder /dupcleaner /usr/local/bin/dupcleaner

# Default listen port (overridable via the first CLI arg, see CMD).
EXPOSE 7891

# main.go reads DUPCLEANER_HOST (default 127.0.0.1). Inside a container the
# loopback default would be unreachable from the host, so bind all interfaces.
ENV DUPCLEANER_HOST=0.0.0.0

# Run as a non-root user. Create it and hand over ownership of /data so the
# app can write its runtime files there.
RUN adduser -D -H dupcleaner && chown dupcleaner /data
USER dupcleaner

ENTRYPOINT ["/usr/local/bin/dupcleaner"]
# First arg is the port; override to change it (e.g. `docker run ... image 8080`).
CMD ["7891"]
