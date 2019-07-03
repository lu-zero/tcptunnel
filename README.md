# UDP -> TCP -> UDP tunnel

## Usage

``` $
# Start the tunnel on the tcp -> udp side
$ tcptunnel -u <ip:port> -t <ip:port> -l

# Start the tunnel on the udp -> tcp side
$ tcptunnel -u <ip:port> -t <ip:port>
```

## Status

- [x] UDP -> TCP
- [x] TCP -> UDP
- [ ] cli
