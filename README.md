I wrote this application to help ScyllaDB users simulate stress on a Scylla Cluster, and how the application and ScyllaDB would behave when using different client and server settings.

Make it easy to simulate:
- Unbound concurrency overloading the server
- Server timeout > client timeout overloading the server
- Aggressive use of client and/or server speculative retry overloading the server


After demonstrating common overload scenarios, show how those look in monitoring, then adjust parameters.
- Control concurrency
- Set server timeout less then client timeout (and avoid zombie work)
- Adjust client speculative and timeout retry policies with reasonable retries, min and max delays

Adjust server side timeouts
Adjust max_concurrent_requests_per_shard, observe how goCQL handles shed requests