FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /server . && \
    CGO_ENABLED=0 GOOS=linux go build -o /syncdata ./cmd/syncdata

FROM alpine:3.20

RUN apk --no-cache add \
    ca-certificates \
    && addgroup -S appuser && adduser -S appuser -G appuser

COPY --from=builder /server /server
COPY --from=builder /syncdata /syncdata

EXPOSE 8080

USER appuser

CMD ["/server"]
