defmodule GossipGlomers.Challenge do
  @moduledoc """
  A behaviour for a Maelstrom challenge.

  Docs:
  - https://github.com/jepsen-io/maelstrom/blob/main/resources/protocol-intro.md
  - https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md
  """

  @callback process_maelstrom_message(message :: map(), state :: GossipGlomers.NodeState.t()) ::
              GossipGlomers.NodeState.t()

  defmacro __using__(_opts) do
    quote do
      use GenServer
      require Logger

      alias GossipGlomers.ChallengeRunner
      alias GossipGlomers.NodeState

      @behaviour GossipGlomers.Challenge

      @doc "The main entry point for running a challenge."
      def run(args \\ []) do
        ChallengeRunner.run(__MODULE__, args)
      end

      @doc "Called by ChallengeRunner on startup"
      def start_link(args) do
        GenServer.start_link(__MODULE__, args, name: __MODULE__)
      end

      @doc "Called by ChallengeRunner for every message"
      def handle_message(message) do
        GenServer.cast(__MODULE__, {:handle_message, message})
      end

      @impl GenServer
      def handle_cast({:handle_message, %{"body" => %{"type" => "init"}} = message}, state) do
        Logger.info("Initializing node", message: inspect(message))

        reply(message, "init_ok")

        state
        |> Map.put(:node_id, message["body"]["node_id"])
        |> Map.put(:node_ids, message["body"]["node_ids"])
        |> then(&{:noreply, &1})
      end

      def handle_cast({:handle_message, message}, state) do
        message
        |> process_maelstrom_message(state)
        |> then(&{:noreply, &1})
      end

      @impl GossipGlomers.Challenge
      def process_maelstrom_message(message, state) do
        Logger.warning(
          "#{__MODULE__} received an unhandled message, please implement process_maelstrom_message/2: #{inspect(message)}"
        )

        state
      end

      @doc """
      Sends a new message to a specified destination.

      This is a general-purpose sending function, useful for when a node needs to
      initiate communication rather than just replying to a received message.
      Useful for when a node to be a client to another service and must read or
      write a value by creating/sending a new request message.

      Simpler challenges are purely reactive; they only ever sent messages in
      direct response to other messages. This function provides the ability to be
      proactive, which is essential for the asynchronous, multi-step operations in
      the some challenges.
      """
      def send(src, dest, body) do
        message = %{"src" => src, "dest" => dest, "body" => body}
        send_response(message)
      end

      @doc """
      Builds and sends a reply to an original message.

      Useful for asynchronous operations where a client request may not be answered immediately.
      Instead, its fate is stored in the node's state while other messages are sent. When the final
      result is ready, the node needs to find the original message in its state and send a reply.
      """
      def reply(original_message, type, extra_body \\ %{}) do
        original_message
        |> build_reply(type, extra_body)
        |> send_response()
      end

      # Constructs a reply message map based on an original message.
      # This utility swaps the `src` and `dest` fields and sets the `in_reply_to`
      # body field to the `msg_id` of the original message, which is required by the
      # Maelstrom protocol.
      defp build_reply(original_message, type, extra_body) do
        body = %{
          "type" => type,
          "in_reply_to" => original_message["body"]["msg_id"]
        }

        %{
          "src" => original_message["dest"],
          "dest" => original_message["src"],
          "body" => Map.merge(body, extra_body)
        }
      end

      defp send_response(message) do
        Logger.info("Sending response", message: inspect(message))
        IO.puts(Jason.encode!(message))
      end

      defoverridable process_maelstrom_message: 2
    end
  end
end
