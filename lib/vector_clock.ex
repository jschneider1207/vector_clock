defmodule VectorClock do
  @moduledoc """
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
  """

  @opaque t :: [dot]
  @opaque dot :: Dot.t
  @opaque pure_dot :: PureDot.t
  @type vclock_node :: term
  @type counter :: integer
  @type timestamp :: integer

  defmodule Dot do
    @moduledoc false
    @type t :: %__MODULE__{
      node: VectorClock.vclock_node,
      counter: VectorClock.counter,
      timestamp: VectorClock.timestamp
    }
    defstruct [:node, :counter, :timestamp]
  end

  defmodule PureDot do
    @moduledoc false
    @type t :: %__MODULE__{
      node: VectorClock.vclock_node,
      counter: VectorClock.counter
    }
    defstruct [:node, :counter]
  end

  @doc """
  Create a new empty vector clock.
  """
  @spec fresh() :: t
  def fresh, do: []

  @doc """
  Create a new vectory clock with an initial dot.
  """
  @spec fresh(vclock_node, counter) :: t
  def fresh(node, count) do
    [%Dot{node: node, counter: count, timestamp: timestamp()}]
  end

  @doc """
  Check if vector clock `va` is a descendent of vector clock `vb`.
  """
  @spec descends(t, t) :: boolean
  def descends(_va, []) do
    true # all vectory clocks descend from the empty vector clock
  end
  def descends(va, vb) do
    [%{node: node_b, counter: counter_b}|rest_b] = vb
    case find_dot(va, node_b) do
      nil ->
        false
      %{counter: counter_a} ->
        (counter_a >= counter_b) and descends(va, rest_b)
    end
  end

  @doc """
  Check whether a vector clock decends from a given dot.
  """
  @spec descends_dot(t, dot) :: boolean
  def descends_dot(vclock, dot) do
    descends(vclock, [dot])
  end

  @doc """
  Converts a dot to a pure dot, for when timestamp data is not needed.
  """
  @spec pure_dot(dot) :: pure_dot
  def pure_dot(dot) do
    %PureDot{node: dot.node, counter: dot.counter}
  end

  @doc """
  Checks if vector clock `va` strictly dominates vector clock `vb`.

  A vector clock is said to dominate another when it's clock represents a later
  logical time than the other.
  """
  @spec dominates(t, t) :: boolean
  def dominates(va, vb) do
    descends(va, vb) and not descends(vb, va)
  end

  @doc """
  Combines a list of vector clocks into their least possible common descendant.
  """
  @spec merge([t]) :: t
  def merge([]), do: []
  def merge([single]), do: single
  def merge([head|tail]), do: merge(tail, sort_vclock(head))

  defp merge([], vclock), do: vclock
  defp merge([va|rest], vclock) do
    sorted_vclock = sort_vclock(va)
    merge(rest, merge(sorted_vclock, vclock, []))
  end

  defp merge([], [], acc), do: :lists.reverse(acc)
  defp merge([], left, acc), do: :lists.reverse(acc, left)
  defp merge(left, [], acc), do: :lists.reverse(acc, left)
  defp merge([%{node: node1} = dot|vclock],
             n = [%{node: node2}|_], acc) when node1 < node2 do
    merge(vclock, n, [dot|acc])
  end
  defp merge(v = [%{node: node1}|_],
             [%{node: node2} = dot|nclock], acc) when node1 > node2 do
    merge(v, nclock, [dot|acc])
  end
  defp merge([vdot|vclock], [ndot|nclock], acc) do
    {counter, timestamp} = cond do
      vdot.counter > ndot.counter -> {vdot.counter, vdot.timestamp}
      vdot.counter < ndot.counter -> {ndot.counter, ndot.timestamp}
      true -> {vdot.counter, max(vdot.timestamp, ndot.timestamp)}
    end
    merge(vclock, nclock, [%{vdot| counter: counter, timestamp: timestamp}|acc])
  end

  @doc """
  Get the counter value from a vector clock for a specific node.
  """
  @spec get_counter(t, vclock_node) :: counter
  def get_counter(vclock, node) do
    case find_dot(vclock, node) do
      nil -> 0
      dot -> dot.counter
    end
  end

  @doc """
  Get the timestamp value from a vector clock for a specific node.
  """
  @spec get_timestamp(t, vclock_node) :: timestamp | nil
  def get_timestamp(vclock, node) do
    case find_dot(vclock, node) do
      nil -> nil
      dot -> dot.timestamp
    end
  end

  @doc """
  Get the dot entry from a vector clock for a specific node.
  """
  @spec get_dot(t, vclock_node) :: {:ok, dot} | {:error, :not_found}
  def get_dot(vclock, node) do
    case find_dot(vclock, node) do
      nil -> {:error, :not_found}
      dot -> {:ok, dot}
    end
  end

  @doc """
  Checks if the given argument is a valid dot.
  """
  @spec valid_dot?(term) :: boolean
  def valid_dot?(%Dot{counter: cnt, timestamp: ts}) when is_integer(cnt) and is_integer(ts) do
    true
  end
  def valid_dot?(_), do: false

  @doc """
  Increment the vector clock at node.
  """
  @spec increment(t, vclock_node) :: t
  def increment(vclock, node) do
    increment(vclock, node, timestamp())
  end

  @doc """
  Increment the vector clock at node.
  """
  @spec increment(t, vclock_node, timestamp) :: t
  def increment(vclock, node, timestamp) do
    {new_vclock, new_counter, new_timestamp} = case nodetake(vclock, node) do
      false ->
        {vclock, 1, timestamp}
      {dot, mod_vclock} ->
        {mod_vclock, dot.counter + 1, timestamp}
    end
    [%Dot{node: node, counter: new_counter, timestamp: new_timestamp}|new_vclock]
  end

  @doc """
  Get all nodes in the vector clock.
  """
  @spec all_nodes(t) :: [vclock_node]
  def all_nodes(vclock) do
    for %{node: node} <- vclock, do: node
  end

  @days_from_gregorian_base_to_epoch (1970*365+478)
  @seconds_from_gregorian_base_to_epoch (@days_from_gregorian_base_to_epoch*24*60*60)

  @doc """
  Current timestamp for a vector clock.
  """
  @spec timestamp() :: timestamp
  def timestamp do
    {mega, sec, _} = :os.timestamp()
    @seconds_from_gregorian_base_to_epoch + mega*1000000 + sec
  end

  @doc """
  Compares vector clocks for equality.
  """
  @spec equal?(t, t) :: boolean
  def equal?(va, vb) do
    sort_vclock(va) === sort_vclock(vb)
  end


  # TODO: what should the default values be?

  @doc """
  Prunes a vector clock based on various parameters.

  Vector clocks get pruned when they are either considered too large
  or when the top-most dot is too old.  Entries are removed one-by-one
  off the top until neither of the two conditions are met.

  ## Options

    * `:small_vclock` - max size for a vector clock to be not pruned.
    * `:young_vclock` - max difference between `now` and the timestamp on
                        the latest dot for a vector clock to not be pruned.
    * `:big_vclock`   - vector clocks larger than this will be pruned.
    * `:old_vclock`   - max difference between `now` and the timestamp on
                        the latest dot for it to get pruned.
  """
  @spec prune(t, timestamp, Keyword.t) :: t
  def prune(vclock, now, opts \\ []) do
    sorted_vclock = Enum.sort(vclock, fn dot_a, dot_b ->
      {dot_a.timestamp, dot_a.node} < {dot_b.timestamp, dot_b.node}
    end)
    prune_small(sorted_vclock, now, opts)
  end

  defp prune_small(vclock, now, opts) do
    case length(vclock) <= Keyword.get(opts, :small_vclock, 100) do
      true -> vclock
      false -> prune_young(vclock, now, opts)
    end
  end

  defp prune_young(vclock, now, opts) do
    %{timestamp: head_time} = hd(vclock)
    case (now - head_time) < Keyword.get(opts, :young_vclock, 1_000) do
      true -> vclock
      false -> prune_big_or_old(vclock, now, head_time, opts)
    end
  end

  defp prune_big_or_old(vclock, now, head_time, opts) do
    case (length(vclock) > Keyword.get(opts, :big_vclock, 10_000)) or
         ((now - head_time) > Keyword.get(opts, :old_vclock, 100_000)) do
      true -> prune_small(tl(vclock), now, opts)
      false -> vclock
    end
  end

  # private helpers

  defp sort_vclock(vclock) do
    Enum.sort_by(vclock, &Map.get(&1, :node))
  end

  defp find_dot(vclock, node) do
    Enum.find(vclock, fn dot -> dot.node === node end)
  end

  defp nodetake(vclock, node, acc \\ [])
  defp nodetake([], _node, _acc) do
    false
  end
  defp nodetake([%{node: node} = dot|rest], node, acc) do
    {dot, :lists.reverse(acc, rest)}
  end
  defp nodetake([dot|rest], node, acc) do
    nodetake(rest, node, [dot|acc])
  end
end
