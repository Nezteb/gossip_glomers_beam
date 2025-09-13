defmodule GossipGlomers.ChallengeRunner do
  @moduledoc """
  Handles the core message-processing loop for a Maelstrom challenge.

  This module is the engine that drives a challenge. It performs the following tasks:

  1. Starts the Challenge
    - Kicks off the specific challenge module's GenServer.
  2. Manages IO
    - Reads messages from standard input (`stdin`), line by line.
  3. Parses Messages
    - Decodes each line of input from JSON into an Elixir map.
  4. Dispatches to Challenge
    - Forwards the decoded message to the appropriate `handle_message/1` callback on the challenge module.
  5.Provides Helpers
    - Offers a set of helper functions (`send/3`, `reply/3`, etc.) to standardize and simplify
    the process of sending messages and replies, which is especially useful for challenges involving
    asynchronous communication with other nodes or services.

  The goal is to abstract away the boilerplate of Maelstrom communication, allowing each
  challenge module to focus purely on its own logic and state management.

  Docs:
  - https://github.com/jepsen-io/maelstrom/blob/main/resources/protocol-intro.md
  - https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md
  """
  require Logger

  @doc """
  The main entry point for running a challenge.

  Starts the given `challenge_module` and then blocks, creating a stream that reads
  from standard input. Each line is passed to `process_line/2` for parsing and
  dispatching.
  """
  def run(challenge_module, args \\ []) do
    Logger.info("Starting challenge", challenge: challenge_module, args: inspect(args))

    {:ok, _pid} = challenge_module.start_link(args)

    # Block and wait for messages from stdin
    IO.stream(:stdio, :line)
    |> Stream.each(&process_line(&1, challenge_module))
    |> Stream.run()
  end

  defp process_line(line, challenge_module) do
    Logger.info("Received line", line: line)

    case Jason.decode(line) do
      {:ok, message} ->
        challenge_module.handle_message(message)

      {:error, reason} ->
        Logger.error("Error decoding JSON", line: line, reason: reason)
    end
  end
end
