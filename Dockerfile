FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /server .

FROM alpine:3.20

RUN apk --no-cache add \
    ca-certificates \
    chromium \
    && addgroup -S appuser && adduser -S appuser -G appuser

COPY --from=builder /server /server

EXPOSE 8080

USER appuser

ENV CHROMIUM_PATH=/usr/bin/chromium-browser

CMD ["/server"]
