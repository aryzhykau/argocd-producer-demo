# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download -x 2>/dev/null; true

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -mod=mod -o producer .

# Final stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /app/producer .

USER nobody

ENTRYPOINT ["./producer"]
