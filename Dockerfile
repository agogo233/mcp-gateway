# build stage
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mcp-gateway .

# runtime stage
FROM alpine:3.20
RUN apk add --no-cache wget ca-certificates \
 && addgroup -S app && adduser -S -G app app
COPY --from=build /out/mcp-gateway /usr/local/bin/mcp-gateway
USER app
EXPOSE 9200
ENTRYPOINT ["mcp-gateway"]
CMD ["-config", "/etc/mcp-gateway/gateway.json"]
