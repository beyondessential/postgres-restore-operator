FROM alpine:latest AS certs
RUN apk add --no-cache ca-certificates

FROM busybox:glibc

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ARG TARGETPLATFORM

COPY --chmod=0755 amd64/ /tmp/bins/amd64/
COPY --chmod=0755 arm64/ /tmp/bins/arm64/

RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
	cp /tmp/bins/amd64/* /usr/bin/; \
	elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
	cp /tmp/bins/arm64/* /usr/bin/; \
	else \
	echo "Unknown platform: $TARGETPLATFORM"; exit 1; \
	fi && \
	rm -rf /tmp/bins

USER operator
ENTRYPOINT ["operator"]
