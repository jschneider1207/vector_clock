defmodule VectorClockTest do
  use ExUnit.Case
  alias VectorClock.Dot

  test "simple operations" do
    a = VectorClock.fresh()
    b = VectorClock.fresh()
    a1 = VectorClock.increment(a, :a)
    b1 = VectorClock.increment(b, :b)

    assert VectorClock.descends(a1, a) === true
    assert VectorClock.descends(b1, b) === true
    assert VectorClock.descends(a1, b1) === false

    a2 = VectorClock.increment(a1, :a)
    c = VectorClock.merge([a2, b1])
    c1 = VectorClock.increment(c, :c)

    assert VectorClock.descends(c1, a2) === true
    assert VectorClock.descends(c1, a1) === true
    assert VectorClock.descends(b1, c1) === false
    assert VectorClock.descends(b1, a1) === false
  end

  describe "prune tests" do
    test "less entries than small_vclock won't be touched" do
      now = VectorClock.timestamp()
      old_time = now - 32_000_000
      small_vclock = [
        %Dot{node: "1", counter: 1, timestamp: old_time},
        %Dot{node: "2", counter: 2, timestamp: old_time},
        %Dot{node: "3", counter: 3, timestamp: old_time},
      ]
      opts = [small_vclock: 4]
      pruned_vclock = VectorClock.prune(small_vclock, now, opts)

      assert sort(small_vclock) === sort(pruned_vclock)
    end

    test "all entries younger than young_vclock won't be touched" do
      now = VectorClock.timestamp()
      new_time = now - 1
      young_vclock = [
        %Dot{node: "1", counter: 1, timestamp: new_time},
        %Dot{node: "2", counter: 2, timestamp: new_time},
        %Dot{node: "3", counter: 3, timestamp: new_time},
      ]
      opts = [small_vclock: 1, young_vclock: 1_000]
      pruned_vclock = VectorClock.prune(young_vclock, now, opts)

      assert sort(young_vclock) === sort(pruned_vclock)
    end

    test "entries not preserved by small or young will be trimmed down to big_vclock" do
      now = VectorClock.timestamp()
      new_time = now - 1_000
      big_vclock = [
        %Dot{node: "1", counter: 1, timestamp: new_time},
        %Dot{node: "2", counter: 2, timestamp: new_time},
        %Dot{node: "3", counter: 3, timestamp: new_time},
      ]
      opts = [small_vclock: 1, young_vclock: 1, big_vclock: 2, old_vclock: 100_000]
      pruned_vclock = VectorClock.prune(big_vclock, now, opts)

      assert length(pruned_vclock) === 2
    end

    test "entries not previously preserved will be trimmed down to big_vclock and no more than old_vclock" do
      now = VectorClock.timestamp()
      new_time = now - 1_000
      old_time = now - 100_000
      old_vclock = [
        %Dot{node: "1", counter: 1, timestamp: new_time},
        %Dot{node: "2", counter: 2, timestamp: old_time},
        %Dot{node: "3", counter: 3, timestamp: old_time},
      ]
      opts = [small_vclock: 1, young_vclock: 1, big_vclock: 2, old_vclock: 10_000]
      pruned_vclock = VectorClock.prune(old_vclock, now, opts)

      assert length(pruned_vclock) === 1
    end

    test "vector clocks with two nodes of equal timestamp get pruned to the same node" do
      now = VectorClock.timestamp()
      old_time = now - 100_000
      old_vclock1 = [
        %Dot{node: "1", counter: 1, timestamp: old_time},
        %Dot{node: "2", counter: 2, timestamp: old_time},
      ]
      old_vclock2 = :lists.reverse(old_vclock1)

      opts = [small_vclock: 1, young_vclock: 1, big_vclock: 2, old_vclock: 10_000]
      pruned_vclock1 = VectorClock.prune(old_vclock1, now, opts)
      pruned_vclock2 = VectorClock.prune(old_vclock2, now, opts)

      assert pruned_vclock1 === pruned_vclock2
    end
  end

  test "accessor operations" do
    vector_clock = [
      %Dot{node: "1", counter: 1, timestamp: 1},
      %Dot{node: "2", counter: 2, timestamp: 2}
    ]

    assert VectorClock.get_counter(vector_clock, "1") === 1
    assert VectorClock.get_timestamp(vector_clock, "1") === 1

    assert VectorClock.get_counter(vector_clock, "2") === 2
    assert VectorClock.get_timestamp(vector_clock, "2") === 2

    assert VectorClock.get_counter(vector_clock, "3") === 0
    assert VectorClock.get_timestamp(vector_clock, "3") === nil

    assert VectorClock.all_nodes(vector_clock) == ["1", "2"]
  end

  describe "merge tests" do
    test "merge" do
      vector_clock1 = [
        %Dot{node: "1", counter: 1, timestamp: 1},
        %Dot{node: "2", counter: 2, timestamp: 2},
        %Dot{node: "4", counter: 4, timestamp: 4}
      ]
      vector_clock2 = [
        %Dot{node: "3", counter: 3, timestamp: 3},
        %Dot{node: "4", counter: 3, timestamp: 3}
      ]
      expected_clock = [
        %Dot{node: "1", counter: 1, timestamp: 1},
        %Dot{node: "2", counter: 2, timestamp: 2},
        %Dot{node: "3", counter: 3, timestamp: 3},
        %Dot{node: "4", counter: 4, timestamp: 4}
      ]

      assert VectorClock.merge(VectorClock.fresh()) === []
      assert VectorClock.merge([vector_clock1, vector_clock2]) === expected_clock
    end

    test "merge less left" do
      vector_clock1 = [
        %Dot{node: "5", counter: 5, timestamp: 5},
      ]
      vector_clock2 = [
        %Dot{node: "6", counter: 6, timestamp: 6},
        %Dot{node: "7", counter: 7, timestamp: 7}
      ]
      expected_clock = [
        %Dot{node: "5", counter: 5, timestamp: 5},
        %Dot{node: "6", counter: 6, timestamp: 6},
        %Dot{node: "7", counter: 7, timestamp: 7}
      ]

      assert VectorClock.merge([vector_clock1, vector_clock2]) === expected_clock
    end

    test "merge less right" do
      vector_clock1 = [
        %Dot{node: "6", counter: 6, timestamp: 6},
        %Dot{node: "7", counter: 7, timestamp: 7}
      ]
      vector_clock2 = [
        %Dot{node: "5", counter: 5, timestamp: 5},
      ]
      expected_clock = [
        %Dot{node: "5", counter: 5, timestamp: 5},
        %Dot{node: "6", counter: 6, timestamp: 6},
        %Dot{node: "7", counter: 7, timestamp: 7}
      ]

      assert VectorClock.merge([vector_clock1, vector_clock2]) === expected_clock
    end

    test "merge same id" do
      vector_clock1 = [
        %Dot{node: "1", counter: 1, timestamp: 2},
        %Dot{node: "2", counter: 1, timestamp: 4}
      ]
      vector_clock2 = [
        %Dot{node: "1", counter: 1, timestamp: 3},
        %Dot{node: "3", counter: 1, timestamp: 5}
      ]
      expected_clock = [
        %Dot{node: "1", counter: 1, timestamp: 3},
        %Dot{node: "2", counter: 1, timestamp: 4},
        %Dot{node: "3", counter: 1, timestamp: 5}
      ]

      assert VectorClock.merge([vector_clock1, vector_clock2]) === expected_clock
    end
  end

  test "get dot" do
    vector_clock =
      VectorClock.fresh()
      |> VectorClock.increment(:a)
      |> VectorClock.increment(:b)
      |> VectorClock.increment(:c)
      |> VectorClock.increment(:a)

    assert {:ok, %Dot{node: :a, counter: 2}} = VectorClock.get_dot(vector_clock, :a)
    assert {:ok, %Dot{node: :b, counter: 1}} = VectorClock.get_dot(vector_clock, :b)
    assert {:ok, %Dot{node: :c, counter: 1}} = VectorClock.get_dot(vector_clock, :c)
    assert {:error, :not_found} = VectorClock.get_dot(vector_clock, :d)
  end

  test "valid dot" do
    vector_clock =
      VectorClock.fresh()
      |> VectorClock.increment(:a)
      |> VectorClock.increment(:b)
      |> VectorClock.increment(:c)

    for node <- [:a, :b, :c] do
      {:ok, dot} = VectorClock.get_dot(vector_clock, node)
      assert VectorClock.valid_dot?(dot) === true
    end

    assert VectorClock.valid_dot?(nil) === false
    assert VectorClock.valid_dot?("ravenclaw") === false
    assert VectorClock.valid_dot?([]) === false
  end

  defp sort(vclock) do
    Enum.sort_by(vclock, &Map.get(&1, :node))
  end
end
