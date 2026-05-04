FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /consumer ./consumer

FROM alpine:3.20
COPY --from=builder /consumer /consumer
ENTRYPOINT ["/consumer"]