defmodule GossipGlomers.Challenges.TransactionRegister do
  @moduledoc """
  Links:
  - https://fly.io/dist-sys/6a/
    - Challenge description: Single-Node, Totally-Available Transactions
    - To test: `./maelstrom test -w txn-rw-register --bin ./gossip_glomers txn-rw-register --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total`
      - Everything looks good! ヽ(‘ー`)ノ
  - https://fly.io/dist-sys/6b/
    - Challenge description: Totally-Available, Read Uncommitted Transactions
    - To test: `./maelstrom test -w txn-rw-register --bin ./gossip_glomers txn-rw-register --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted`
      - Everything looks good! ヽ(‘ー`)ノ
    - To test: `./maelstrom test -w txn-rw-register --bin ./gossip_glomers txn-rw-register --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition`
      - Everything looks good! ヽ(‘ー`)ノ
  - https://fly.io/dist-sys/6c/
    - Challenge description: Totally-Available, Read Committed Transactions
    - To test: `./maelstrom test -w txn-rw-register --bin ./gossip_glomers txn-rw-register --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition`
      - Everything looks good! ヽ(‘ー`)ノ
  """
  use GossipGlomers.Challenge

  @impl true
  def init(_args) do
    initial_challenge_state = %{
      kv_store: %{}
    }

    {:ok, NodeState.new(initial_challenge_state)}
  end

  @impl GossipGlomers.Challenge
  def process_maelstrom_message(%{"body" => %{"type" => "txn"}} = message, state) do
    handle_txn(message, state)
  end

  def process_maelstrom_message(%{"body" => %{"in_reply_to" => _}}, state), do: state

  def process_maelstrom_message(%{"body" => %{"type" => type}} = message, state) do
    Logger.warning("[#{state.node_id}] Unhandled message type: #{type}")
    reply(message, "error", %{"code" => 10, "text" => "Not supported"})
    state
  end

  def process_maelstrom_message(message, state) do
    Logger.warning("#{__MODULE__} received unknown message",
      message: inspect(message),
      state: inspect(state)
    )

    state
  end

  defp handle_txn(message, state) do
    with %{"txn" => ops} <- message["body"],
         :ok <- validate_transaction_ops(ops),
         {:ok, new_kv, processed_ops} <-
           process_txn_ops(ops, state.challenge_state.kv_store) do
      Logger.info(
        "[#{state.node_id}] Transaction successful. Processed ops: #{inspect(processed_ops)}"
      )

      reply(message, "txn_ok", %{"txn" => processed_ops})
      updated_challenge_state = %{state.challenge_state | kv_store: new_kv}
      %{state | challenge_state: updated_challenge_state}
    else
      error -> handle_txn_error(error, message, state)
    end
  end

  defp handle_txn_error(error, message, state) do
    text = inspect(error)
    Logger.error("[#{state.node_id}] Transaction failed: #{text}")
    reply_with_error(message, text)
    state
  end

  defp reply_with_error(message, text) do
    reply(message, "error", %{"code" => 11, "text" => text})
  end

  defp validate_transaction_ops(ops) when is_list(ops) do
    if Enum.all?(ops, &valid_op?/1), do: :ok, else: {:error, :invalid_operations}
  end

  defp validate_transaction_ops(_), do: {:error, :invalid_operations}

  defp valid_op?(["r", key, _]) when is_binary(key) or is_integer(key), do: true
  defp valid_op?(["w", key, _value]) when is_binary(key) or is_integer(key), do: true
  defp valid_op?(_), do: false

  defp process_txn_ops(ops, kv) do
    initial_acc = {:ok, [], kv}

    result = Enum.reduce(ops, initial_acc, &process_single_op/2)

    case result do
      {:ok, processed_ops_reversed, final_kv} ->
        {:ok, final_kv, Enum.reverse(processed_ops_reversed)}

      {:error, _reason} = error ->
        error
    end
  end

  defp process_single_op(op, {:ok, processed, current_kv}) do
    case apply_operation(op, current_kv) do
      {:ok, processed_op, new_kv} ->
        {:ok, [processed_op | processed], new_kv}

      error ->
        error
    end
  end

  defp process_single_op(_op, error_acc), do: error_acc

  defp apply_operation(["r", key, _], current_kv) do
    read_value = Map.get(current_kv, key)
    {:ok, ["r", key, read_value], current_kv}
  end

  defp apply_operation(["w", key, value], current_kv) do
    new_kv = Map.put(current_kv, key, value)
    {:ok, ["w", key, value], new_kv}
  end

  defp apply_operation(invalid_op, _current_kv) do
    {:error, {:invalid_operation, invalid_op}}
  end
end
