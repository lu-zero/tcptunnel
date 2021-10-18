# UDP -> TCP -> UDP tunnel

## Usage

``` sh
# Start the tunnel on the tcp -> udp side
$ tcptunnel -u <ip:port> -t <ip:port>

# Start the tunnel on the udp -> tcp side
$ tcptunnel -u <ip:port> -t <ip:port> -s
```

``` sh
# Setup two netcat in different consoles
# Input
$ nc6 -u localhost 5555
# Output
$ nc6 -ul localhost -p 6666

# Copy from input to output (NOTE: it prioritizes ipv6 over ipv4)
$ udpcopy -i udp://localhost:5555 -o udp://localhost:6666
```

## Status

- [x] UDP -> TCP
- [x] TCP -> UDP
- [x] cli
