# Nifi Redis PubSub Processor
Processor that consumes data from a Redis' PubSub queue. It uses Java Lettuce libraries to connect to Redis asynchronously.

## Tested Versions
Currently it's only been tested with Redis 4.0.X and NiFi 1.7.0

### TODO
- Add unit tests.
- Test compatibility with different Redis and NiFi versions.
- Publish to Maven Repository.