defmodule GossipGlomers.Challenges.UniqueIds do
  @moduledoc """
  Links:
  - https://fly.io/dist-sys/2/
    - Challenge description
    - To test: `./maelstrom test -w unique-ids --bin ./gossip_glomers unique-ids --node-count 3 --time-limit 30 --rate 1000 --availability total --nemesis partition`
      - Everything looks good! ヽ(‘ー`)ノ
  """
  use GossipGlomers.Challenge

  @impl GenServer
  def init(_args) do
    {:ok, NodeState.new()}
  end

  @impl GossipGlomers.Challenge
  def process_maelstrom_message(%{"body" => %{"type" => "generate"}} = message, state) do
    # Globally unique ID:
    # - node_id (ensures uniqueness across nodes)
    # - next_msg_id (ensures uniqueness within the node)
    # - current timestamp (adds additional uniqueness and ordering)
    unique_id = "#{state.node_id}-#{state.next_msg_id}-#{System.system_time(:nanosecond)}"

    extra_body = %{
      "msg_id" => state.next_msg_id,
      "id" => unique_id
    }

    reply(message, "generate_ok", extra_body)

    NodeState.increment_msg_id(state)
  end

  def process_maelstrom_message(message, state) do
    Logger.warning("#{__MODULE__} received unknown message",
      message: inspect(message),
      state: inspect(state)
    )

    state
  end
end
