defmodule GossipGlomers.Challenges.Echo do
  @moduledoc """
  Links:
  - https://fly.io/dist-sys/1/
    - Challenge description
    - To test: `./maelstrom test -w echo --bin ./gossip_glomers echo --node-count 1 --time-limit 10`
      - Everything looks good! ヽ(‘ー`)ノ
  """
  use GossipGlomers.Challenge

  @impl GenServer
  def init(_args) do
    {:ok, NodeState.new()}
  end

  @impl GossipGlomers.Challenge
  def process_maelstrom_message(%{"body" => %{"echo" => echo} = body} = message, state) do
    extra_body = %{
      "msg_id" => body["msg_id"],
      "echo" => echo
    }

    reply(message, "echo_ok", extra_body)

    state
  end

  def process_maelstrom_message(message, state) do
    Logger.warning("#{__MODULE__} received unknown message",
      message: inspect(message),
      state: inspect(state)
    )

    state
  end
end
