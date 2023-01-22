FROM golang:latest

# Copy the Go source code
COPY . /go/src/filter

# Set the working directory
WORKDIR /go/src/filter

# Build the Go binary
RUN go build -o filter

# Run the binary when the container starts
CMD ["./filter"]