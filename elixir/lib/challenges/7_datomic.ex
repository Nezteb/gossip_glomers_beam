defmodule GossipGlomers.Challenges.TransactionListAppend do
  @moduledoc """
  TODO: Fetch docs below and implement.

  Links:
  - Datomic (`txn-list-append`)
    - Non-Elixir guide: https://github.com/jepsen-io/maelstrom/blob/main/doc/05-datomic/01-single-node.md
    - To test:
      ```
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 10 --node-count 1
      # Everything looks good! ヽ(‘ー`)ノ
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 30 --node-count 1 --concurrency 10n --rate 100
      # Everything looks good! ヽ(‘ー`)ノ
      ```
    - Non-Elixir guide: https://github.com/jepsen-io/maelstrom/blob/main/doc/05-datomic/02-shared-state.md
    - To test:
      ```
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 10 --node-count 2
      # Everything looks good! ヽ(‘ー`)ノ
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 10 --node-count 2 --rate 100
      # Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
      ```
    - Non-Elixir guide: https://github.com/jepsen-io/maelstrom/blob/main/doc/05-datomic/03-persistent-trees.md
    - To test:
      ```
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 10 --node-count 2 --rate 100
      # Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
      ```
    - Non-Elixir guide: https://github.com/jepsen-io/maelstrom/blob/main/doc/05-datomic/04-optimization.md
    - To test:
      ```
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 10 --node-count 2 --rate 1
      # Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 10 --node-count 2 --rate 100
      # Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 10 --node-count 2 --rate 10
      # Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 10 --node-count 2 --rate 100
      # Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
      ./maelstrom test -w txn-list-append --bin ./gossip_glomers txn-list-append --time-limit 10 --node-count 2 --rate 100 --consistency-models serializable
      # Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
      ```
  """
  use GossipGlomers.Challenge

  @impl GenServer
  def init(_args) do
    initial_challenge_state = %{
      pending_requests: %{},
      txn_queue: [],
      processing_txn: false,
      current_txn_request: nil
    }

    {:ok, NodeState.new(initial_challenge_state)}
  end

  @impl GossipGlomers.Challenge
  def process_maelstrom_message(message, state) do
    case message["body"]["type"] do
      "txn" ->
        handle_txn(message, state)

      "read_ok" ->
        handle_read_ok(message, state)

      "cas_ok" ->
        handle_cas_ok(message, state)

      "error" ->
        handle_error(message, state)

      type ->
        Logger.warning("Received unknown message type: #{type}")
        state
    end
  end

  defp handle_txn(message, state) do
    new_queue = state.challenge_state.txn_queue ++ [message]
    new_challenge_state = %{state.challenge_state | txn_queue: new_queue}
    new_state = %{state | challenge_state: new_challenge_state}
    maybe_process_next_from_queue(new_state)
  end

  defp maybe_process_next_from_queue(state) do
    if state.challenge_state.processing_txn or state.challenge_state.txn_queue == [] do
      state
    else
      [message | new_queue] = state.challenge_state.txn_queue
      txn = message["body"]["txn"]

      new_challenge_state = %{
        state.challenge_state
        | txn_queue: new_queue,
          processing_txn: true,
          current_txn_request: message
      }

      new_state = %{state | challenge_state: new_challenge_state}
      context = {message, txn, []}
      process_next_op(context, new_state)
    end
  end

  defp process_next_op({original_message, [], processed_txn}, state) do
    reply(original_message, "txn_ok", %{"txn" => Enum.reverse(processed_txn)})

    new_challenge_state = %{
      state.challenge_state
      | processing_txn: false,
        current_txn_request: nil
    }

    new_state = %{state | challenge_state: new_challenge_state}
    maybe_process_next_from_queue(new_state)
  end

  defp process_next_op({original_message, [op | rest_txn], processed_txn}, state) do
    context = {original_message, rest_txn, processed_txn}
    read_kv(op, context, state)
  end

  defp handle_read_ok(message, state) do
    {pending_request, next_state} = pop_pending_request(message, state)
    {op, context} = pending_request
    [op_type, key, value] = op
    {original_message, rest_txn, processed_txn} = context
    current_value = Map.get(message["body"], "value")

    case op_type do
      "r" ->
        processed_op = ["r", key, current_value]
        new_context = {original_message, rest_txn, [processed_op | processed_txn]}
        process_next_op(new_context, next_state)

      "append" ->
        new_value = (current_value || []) ++ [value]
        cas_kv(op, current_value, new_value, context, next_state)
    end
  end

  defp handle_cas_ok(message, state) do
    {pending_request, next_state} = pop_pending_request(message, state)
    {op, context} = pending_request
    [op_type, key, value] = op
    {original_message, rest_txn, processed_txn} = context

    processed_op = [op_type, key, value]
    new_context = {original_message, rest_txn, [processed_op | processed_txn]}
    process_next_op(new_context, next_state)
  end

  defp handle_error(message, state) do
    {pending_request, next_state} = pop_pending_request(message, state)
    {op, context} = pending_request
    [op_type, key, value] = op
    {original_message, _rest_txn, _processed_txn} = context
    error_code = message["body"]["code"]

    case error_code do
      20 ->
        case op_type do
          "r" ->
            {_original_message, rest_txn, processed_txn} = context
            processed_op = ["r", key, nil]
            new_context = {original_message, rest_txn, [processed_op | processed_txn]}
            process_next_op(new_context, next_state)

          "append" ->
            new_value = [value]
            cas_kv(op, nil, new_value, context, next_state, create_if_not_exists: true)
        end

      22 ->
        current_txn_request = next_state.challenge_state.current_txn_request
        new_queue = [current_txn_request | next_state.challenge_state.txn_queue]

        new_challenge_state = %{
          next_state.challenge_state
          | processing_txn: false,
            current_txn_request: nil,
            txn_queue: new_queue
        }

        new_state = %{next_state | challenge_state: new_challenge_state}
        maybe_process_next_from_queue(new_state)

      code ->
        Logger.error("Unhandled lin-kv error code: #{code}, aborting transaction.")
        reply(original_message, "error", %{"code" => 11, "text" => "Internal kv error #{code}"})

        new_challenge_state = %{
          next_state.challenge_state
          | processing_txn: false,
            current_txn_request: nil
        }

        new_state = %{next_state | challenge_state: new_challenge_state}
        maybe_process_next_from_queue(new_state)
    end
  end

  defp read_kv([op_type, key, _value] = op, context, state) when op_type in ["r", "append"] do
    body = %{"type" => "read", "key" => to_string(key)}
    {next_state, msg_id} = rpc("lin-kv", body, state)
    store_pending_request(msg_id, {op, context}, next_state)
  end

  defp cas_kv(op, from, to, context, state, opts \\ []) do
    [_, key, _] = op
    create_if_not_exists = Keyword.get(opts, :create_if_not_exists, false)

    body = %{
      "type" => "cas",
      "key" => to_string(key),
      "from" => from,
      "to" => to,
      "create_if_not_exists" => create_if_not_exists
    }

    {next_state, msg_id} = rpc("lin-kv", body, state)
    store_pending_request(msg_id, {op, context}, next_state)
  end

  defp store_pending_request(msg_id, pending_request, state) do
    updated_pending_requests =
      Map.put(state.challenge_state.pending_requests, msg_id, pending_request)

    updated_challenge_state = %{
      state.challenge_state
      | pending_requests: updated_pending_requests
    }

    %{state | challenge_state: updated_challenge_state}
  end

  defp pop_pending_request(message, state) do
    in_reply_to = message["body"]["in_reply_to"]

    {pending_request, updated_pending} =
      Map.pop!(state.challenge_state.pending_requests, in_reply_to)

    updated_challenge_state = %{state.challenge_state | pending_requests: updated_pending}
    next_state = %{state | challenge_state: updated_challenge_state}
    {pending_request, next_state}
  end

  defp rpc(dest, body, state) do
    msg_id = state.next_msg_id
    next_state = NodeState.increment_msg_id(state)
    body_with_msg_id = Map.put(body, "msg_id", msg_id)
    send(state.node_id, dest, body_with_msg_id)
    {next_state, msg_id}
  end
end
