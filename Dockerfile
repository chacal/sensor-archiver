FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /sensor-archiver .

# Includes CA certificates for mqtts:// and runs as uid 65532
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /sensor-archiver /sensor-archiver
VOLUME /data
ENTRYPOINT ["/sensor-archiver"]
