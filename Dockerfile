FROM golang:1.22.2 AS builder

WORKDIR /tmp/JacuteSQL

COPY ./cmd/ ./cmd
COPY ./internal/ ./internal
COPY go.mod ./
COPY go.sum ./

RUN CGO_ENABLED=0 go build -tags netgo,osusergo,static_build -o /JacuteSQL /tmp/JacuteSQL/cmd/JacuteSQL/main.go

FROM alpine:3.20.3

WORKDIR /app

COPY --from=builder /JacuteSQL .

CMD [ "/app/JacuteSQL", "--config", "/app/config/config.yaml" ]
