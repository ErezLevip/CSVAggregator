FROM iron/go:dev
WORKDIR /app
ENV SRC_DIR="/go/src/Aggregator"
COPY . $SRC_DIR
WORKDIR $SRC_DIR
RUN go build -o myapp && cp myapp /app
ENTRYPOINT ["./myapp"]