defmodule GossipGlomers.CLI do
  @moduledoc """
  CLI interface meant to be used by Maelstrom.
  """

  require Logger

  @challenges %{
    "echo" => GossipGlomers.Challenges.Echo,
    "unique-ids" => GossipGlomers.Challenges.UniqueIds,
    "broadcast" => GossipGlomers.Challenges.Broadcast,
    "g-counter" => GossipGlomers.Challenges.GrowOnlyCounter,
    "kafka" => GossipGlomers.Challenges.Kafka,
    "txn-rw-register" => GossipGlomers.Challenges.TransactionRegister,
    "txn-list-append" => GossipGlomers.Challenges.TransactionListAppend
  }

  def main(args) do
    Logger.info("Starting #{__MODULE__}", args: inspect(args))

    challenge = List.first(args) || "echo"

    case Map.get(@challenges, challenge) do
      nil ->
        Logger.error("Unknown challenge", challenge: challenge, available: Map.keys(@challenges))

      challenge_module ->
        challenge_module.run(args)
    end
  end
end
