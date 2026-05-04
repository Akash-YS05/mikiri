FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /dashboard ./dashboard

FROM alpine:3.20
RUN apk add --no-cache wget
COPY --from=builder /dashboard /dashboard
ENTRYPOINT ["/dashboard"]