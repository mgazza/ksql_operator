FROM golang:1.14-alpine3.12 as build-stage
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download


COPY hack/main.go .
RUN CGO_ENABLED=0 go build

COPY . .
RUN CGO_ENABLED=0 go build -o app .

FROM alpine as production-stage
COPY --from=build-stage /build/app ./app
CMD ["./app"]