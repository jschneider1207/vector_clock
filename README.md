# VectorClock

Elixir implementation of vector clocks.

## About

Vector clocks are used in distributed systems as a way of maintaining a
logical ordering of events. A vector clock consists of a list of dots,
which each dot representing a node in a distributed system. A dot consists
of an identifier for the node, it's current count, and a timestamp from
the last time it was incremented. When a node sends an event to another
node it increments the it's dot in it's vector clock and sends the clock
along side the message.  A node receiving a message can determine whether
it has seen the effect of that message already by comparing it's vector clock
with the received vector clock.

## Source

Based on the erlang version from
[`:riak_core`](https://github.com/basho/riak_core/blob/develop/src/vclock.erl).

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `vector_clock` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:vector_clock, "~> 0.1.0"}]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/vector_clock](https://hexdocs.pm/vector_clock).
