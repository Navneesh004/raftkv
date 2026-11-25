# Build stage: compile static binaries.
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# Runtime stage: tiny image with just the binaries.
FROM alpine:3.20
COPY --from=build /out/ /usr/local/bin/
EXPOSE 7000 8080
ENTRYPOINT ["raftkv-server"]
