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

``` sh
# duplicate the input to two different sockets (any number)
$ rtpbound -i udp://localhost:12340?buffer=100000 -o udp://localhost:12347?buffer=100000 -o udp://localhost:12349?buffer=100000
# read from two identical inputs and forward to an output, one input may fail and the stream stays fine
$ rtpbound -i udp://localhost:12349?buffer=100000 -i udp://localhost:12347?buffer=100000 -o udp://localhost:12348?buffer=100000
```

## Status

- [x] UDP -> TCP
- [x] TCP -> UDP
- [x] copy UDP -> UDP
- [x] copy RTP -> RTP and support multipath
- [x] cli

## Notes

Thanks to Edoardo Morandi for his help in debugging some async problems.
