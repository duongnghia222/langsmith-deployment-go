# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/lsd ./cmd/lsd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/lsd /lsd
EXPOSE 50051 9090
USER nonroot:nonroot
ENTRYPOINT ["/lsd"]
