defmodule GossipGlomers.Challenges.Broadcast do
  @moduledoc """
  Links:
  - https://fly.io/dist-sys/3a/
    - Challenge description: Single-Node Broadcast
    - To test: `./maelstrom test -w broadcast --bin ./gossip_glomers broadcast --node-count 1 --time-limit 20 --rate 10`
      - Everything looks good! ヽ(‘ー`)ノ
  - https://fly.io/dist-sys/3b/
    - Challenge description: Multi-Node Broadcast
    - To test: `./maelstrom test -w broadcast --bin ./gossip_glomers broadcast --node-count 5 --time-limit 20 --rate 10`
      - Everything looks good! ヽ(‘ー`)ノ
  - https://fly.io/dist-sys/3c/
    - Challenge description: Fault Tolerant Broadcast
    - To test: `./maelstrom test -w broadcast --bin ./gossip_glomers broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition`
      - Everything looks good! ヽ(‘ー`)ノ
  - https://fly.io/dist-sys/3d/
    - Challenge description: Efficient Broadcast, Part I
    - To test: `./maelstrom test -w broadcast --bin ./gossip_glomers broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100`
      - Everything looks good! ヽ(‘ー`)ノ
    - To test: `./maelstrom test -w broadcast --bin ./gossip_glomers broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition`
      - Everything looks good! ヽ(‘ー`)ノ
  - https://fly.io/dist-sys/3e/
    - Challenge description: Efficient Broadcast, Part II
    - To test: `./maelstrom test -w broadcast --bin ./gossip_glomers broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100`
      - Everything looks good! ヽ(‘ー`)ノ
    - To test: `./maelstrom test -w broadcast --bin ./gossip_glomers broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition`
      - Everything looks good! ヽ(‘ー`)ノ
  """
  use GossipGlomers.Challenge

  @impl GenServer
  def init(_args) do
    initial_challenge_state = %{
      broadcast_messages: MapSet.new(),
      broadcast_topology: %{},
      broadcast_neighbors: []
    }

    # Start periodic gossip now that we're initialized
    Process.send_after(self(), :gossip, 1000)

    {:ok, NodeState.new(initial_challenge_state)}
  end

  @impl GenServer
  def handle_info(:gossip, state) do
    all_messages =
      Map.get(state.challenge_state, :broadcast_messages, MapSet.new()) |> MapSet.to_list()

    if Enum.any?(all_messages) do
      Logger.info("#{state.node_id}: Periodic gossip: sending #{length(all_messages)} messages")
      gossip_to_neighbors(all_messages, state)
    else
      Logger.debug("#{state.node_id}: Periodic gossip: no messages to send")
    end

    Process.send_after(self(), :gossip, 1000)

    {:noreply, state}
  end

  @impl GossipGlomers.Challenge
  def process_maelstrom_message(
        %{"body" => %{"type" => "broadcast", "message" => message}} = msg,
        state
      ) do
    messages = Map.get(state.challenge_state, :broadcast_messages, MapSet.new())
    was_new = not MapSet.member?(messages, message)
    updated_messages = MapSet.put(messages, message)

    updated_challenge_state = %{state.challenge_state | broadcast_messages: updated_messages}
    new_state = %{state | challenge_state: updated_challenge_state}

    Logger.info(
      "#{state.node_id}: Received broadcast message=#{message}, was_new=#{was_new}, total_messages=#{MapSet.size(updated_messages)}"
    )

    reply(msg, "broadcast_ok", %{"msg_id" => state.next_msg_id})

    # Gossip to neighbors (for multi-node challenges)
    if was_new do
      neighbors = Map.get(new_state.challenge_state, :broadcast_neighbors, [])

      Logger.info(
        "#{state.node_id}: Gossiping new message=#{message} to #{length(neighbors)} neighbors: #{inspect(neighbors)}"
      )

      gossip_to_neighbors(message, new_state)
    else
      Logger.debug("#{state.node_id}: Skipping gossip for duplicate message=#{message}")
    end

    NodeState.increment_msg_id(new_state)
  end

  def process_maelstrom_message(%{"body" => %{"type" => "read"}} = msg, state) do
    messages =
      Map.get(state.challenge_state, :broadcast_messages, MapSet.new()) |> MapSet.to_list()

    Logger.debug(
      "#{state.node_id}: Read request - returning #{length(messages)} messages: #{inspect(messages)}"
    )

    extra_body = %{
      "msg_id" => state.next_msg_id,
      "messages" => messages
    }

    reply(msg, "read_ok", extra_body)

    NodeState.increment_msg_id(state)
  end

  def process_maelstrom_message(
        %{"body" => %{"type" => "topology", "topology" => topology}} = msg,
        state
      ) do
    # Store topology and determine our neighbors
    neighbors = get_efficient_neighbors(topology, state.node_id, state.node_ids)

    Logger.info(
      "#{state.node_id}: Topology set - cluster_size=#{length(state.node_ids)}, my_neighbors=#{length(neighbors)}, neighbors=#{inspect(neighbors)}"
    )

    challenge_state =
      state.challenge_state
      |> Map.put(:broadcast_topology, topology)
      |> Map.put(:broadcast_neighbors, neighbors)

    new_state = %{state | challenge_state: challenge_state}

    reply(msg, "topology_ok", %{"msg_id" => state.next_msg_id})

    NodeState.increment_msg_id(new_state)
  end

  # Handle internal broadcast message from another node (gossip)
  def process_maelstrom_message(
        %{"body" => %{"type" => "gossip", "messages" => gossip_messages}} = msg,
        state
      ) do
    current_messages = Map.get(state.challenge_state, :broadcast_messages, MapSet.new())
    new_messages = MapSet.new(gossip_messages)
    updated_messages = MapSet.union(current_messages, new_messages)

    # Only propagate if we got new messages
    truly_new = MapSet.difference(new_messages, current_messages)

    Logger.info(
      "#{state.node_id}: Received gossip from #{msg["src"]}, messages=#{inspect(gossip_messages)}, truly_new=#{MapSet.size(truly_new)}, total_after=#{MapSet.size(updated_messages)}"
    )

    updated_challenge_state = %{state.challenge_state | broadcast_messages: updated_messages}
    new_state = %{state | challenge_state: updated_challenge_state}

    # Replying to gossip is not required and can fail partition tests
    # reply(msg, "gossip_ok", %{"msg_id" => state.next_msg_id})

    # Propagate new messages to our neighbors (fixed boolean logic)
    if MapSet.size(truly_new) > 0 do
      new_messages_list = MapSet.to_list(truly_new)
      neighbors = Map.get(new_state.challenge_state, :broadcast_neighbors, [])
      target_neighbors = List.delete(neighbors, msg["src"])

      Logger.info(
        "#{state.node_id}: Propagating #{length(new_messages_list)} new messages #{inspect(new_messages_list)} to #{length(target_neighbors)} neighbors (excluding sender #{msg["src"]})"
      )

      propagate_messages(new_messages_list, new_state, msg["src"])
    else
      Logger.debug("#{state.node_id}: No new messages to propagate from gossip")
    end

    NodeState.increment_msg_id(new_state)
  end

  def process_maelstrom_message(%{"body" => %{"type" => "gossip_ok"}} = msg, state) do
    Logger.debug("#{state.node_id}: Received gossip_ok from #{msg["src"]}")
    # Just acknowledge the gossip_ok, no action needed
    state
  end

  def process_maelstrom_message(message, state) do
    Logger.warning("#{__MODULE__} received unknown message",
      message: inspect(message),
      state: inspect(state)
    )

    state
  end

  # Determine efficient neighbors based on cluster size
  defp get_efficient_neighbors(topology, node_id, all_nodes) do
    cond do
      # Single node?
      is_nil(all_nodes) or length(all_nodes) == 1 ->
        Logger.info("#{node_id}: Single node cluster - no neighbors")
        []

      # Small cluster?
      length(all_nodes) < 10 ->
        get_small_cluster_neighbors(topology, node_id, all_nodes)

      # Otherwise:
      true ->
        create_efficient_topology(node_id, all_nodes)
    end
  end

  # For small clusters
  defp get_small_cluster_neighbors(topology, node_id, all_nodes) do
    neighbors =
      case Map.get(topology, node_id) do
        nil ->
          # Full mesh for small clusters
          neighbors = List.delete(all_nodes, node_id)

          Logger.info("#{node_id}: Using full mesh topology - neighbors=#{length(neighbors)}")

          neighbors

        neighbors ->
          Logger.info("#{node_id}: Using provided topology - neighbors=#{length(neighbors)}")

          neighbors
      end

    neighbors
  end

  # For large clusters
  defp create_efficient_topology(node_id, all_nodes) do
    sorted_nodes = Enum.sort(all_nodes)
    node_index = Enum.find_index(sorted_nodes, &(&1 == node_id))
    node_count = length(sorted_nodes)

    Logger.info("#{node_id}: Creating efficient topology - position=#{node_index}/#{node_count}")

    # Create a combination of ring + hub topology for efficiency
    neighbors = []

    # Ring topology - connect to next and previous nodes
    prev_index = rem(node_index - 1 + node_count, node_count)
    next_index = rem(node_index + 1, node_count)

    neighbors =
      neighbors ++
        [Enum.at(sorted_nodes, prev_index), Enum.at(sorted_nodes, next_index)]

    # Add connections to create shortcuts (for logarithmic propagation)
    # Connect to nodes at distances that are powers of 2
    shortcuts =
      for exp <- 2..trunc(:math.log2(node_count)) do
        distance = trunc(:math.pow(2, exp))
        target_index = rem(node_index + distance, node_count)
        Enum.at(sorted_nodes, target_index)
      end

    neighbors = neighbors ++ shortcuts

    # Remove self and duplicates
    final_neighbors =
      neighbors
      |> List.delete(node_id)
      |> Enum.uniq()

    Logger.info(
      "#{node_id}: Efficient topology created - ring_neighbors=2, shortcuts=#{length(shortcuts)}, final_neighbors=#{length(final_neighbors)}"
    )

    final_neighbors
  end

  defp propagate_messages(messages, state, exclude_node) do
    neighbors = Map.get(state.challenge_state, :broadcast_neighbors, [])

    target_neighbors =
      if exclude_node, do: List.delete(neighbors, exclude_node), else: neighbors

    all_messages =
      Map.get(state.challenge_state, :broadcast_messages, MapSet.new())
      |> MapSet.to_list()

    Logger.debug(
      "#{state.node_id}: Propagating all_messages=#{inspect(all_messages)} (messages=#{inspect(messages)}) to #{length(target_neighbors)} neighbors"
    )

    for neighbor <- target_neighbors do
      # if we only sent `messages`, network partitions could isolate nodes
      # and `--nemesis partition` would cause failures
      send_gossip(all_messages, neighbor, state)
    end
  end

  defp gossip_to_neighbors(message, state) when is_integer(message) do
    gossip_to_neighbors([message], state)
  end

  defp gossip_to_neighbors(messages, state) when is_list(messages) do
    neighbors = Map.get(state.challenge_state, :broadcast_neighbors, [])

    all_messages =
      Map.get(state.challenge_state, :broadcast_messages, MapSet.new())
      |> MapSet.to_list()

    Logger.debug(
      "#{state.node_id}: Gossiping messages=#{inspect(messages)} to #{length(neighbors)} neighbors"
    )

    for neighbor <- neighbors do
      send_gossip(all_messages, neighbor, state)
    end
  end

  defp send_gossip(messages, target_node, state) do
    body = %{
      "type" => "gossip",
      "msg_id" => state.next_msg_id,
      "messages" => messages
    }

    Logger.debug(
      "#{state.node_id}: Sending gossip to #{target_node} with messages=#{inspect(messages)}"
    )

    send(state.node_id, target_node, body)
  end
end
