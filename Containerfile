FROM busybox:glibc

ARG TARGETPLATFORM

# Create user and group
RUN addgroup -g 1000 operator && adduser -D -u 1000 -G operator operator

# Copy all binaries from both architectures
COPY --chmod=0755 amd64/ /tmp/bins/amd64/
COPY --chmod=0755 arm64/ /tmp/bins/arm64/

# Select and copy the correct binaries based on target platform
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
