defmodule GossipGlomers.Challenges.Kafka do
  @moduledoc """
  Links:
  - https://fly.io/dist-sys/5a/
    - Challenge description: Single-Node Kafka-Style Log
    - To test: `./maelstrom test -w kafka --bin ./gossip_glomers kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000`
      - Everything looks good! ヽ(‘ー`)ノ
  - https://fly.io/dist-sys/5b/
    - Challenge description: Multi-Node Kafka-Style Log
    - To test: `./maelstrom test -w kafka --bin ./gossip_glomers kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000`
      - Everything looks good! ヽ(‘ー`)ノ
  - https://fly.io/dist-sys/5c/
    - Challenge description: Efficient Kafka-Style Log
    - To test: `./maelstrom test -w kafka --bin ./gossip_glomers kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000`
      - Everything looks good! ヽ(‘ー`)ノ
  """
  use GossipGlomers.Challenge

  @impl true
  def init(_args) do
    initial_challenge_state = %{
      client_requests: %{},
      kv_requests: %{}
    }

    {:ok, NodeState.new(initial_challenge_state)}
  end

  @impl GossipGlomers.Challenge
  def process_maelstrom_message(%{"body" => body} = message, state) do
    type = body["type"]
    handler = handler_for_type(type)
    handler.(message, state)
  end

  def process_maelstrom_message(message, state) do
    Logger.warning("#{__MODULE__} received unknown message",
      message: inspect(message),
      state: inspect(state)
    )

    state
  end

  defp handler_for_type("send"), do: &handle_send/2
  defp handler_for_type("poll"), do: &handle_poll/2
  defp handler_for_type("commit_offsets"), do: &handle_commit_offsets/2
  defp handler_for_type("list_committed_offsets"), do: &handle_list_committed_offsets/2
  defp handler_for_type("read_ok"), do: &handle_read_ok/2
  defp handler_for_type("cas_ok"), do: &handle_cas_ok/2
  defp handler_for_type("write_ok"), do: fn _, state -> state end
  defp handler_for_type("error"), do: &handle_error/2

  defp handler_for_type(type) do
    fn message, state ->
      Logger.warning("Received unknown message type: #{type}", message: message)
      state
    end
  end

  defp handle_send(message, state) do
    key = message["body"]["key"]
    msg_id = message["body"]["msg_id"]
    msg = message["body"]["msg"]

    updated_client_requests =
      Map.put(state.challenge_state.client_requests, msg_id, {message, msg})

    updated_challenge_state = %{
      state.challenge_state
      | client_requests: updated_client_requests
    }

    state = %{state | challenge_state: updated_challenge_state}
    send_log_read(key, msg_id, state)
  end

  defp handle_poll(message, state) do
    keys = Map.keys(message["body"]["offsets"])
    start_multi_key_read(message, keys, :poll, &log_key/1, :poll_read, state)
  end

  defp handle_commit_offsets(message, state) do
    reply(message, "commit_offsets_ok")

    Enum.reduce(message["body"]["offsets"], state, fn {key, offset}, acc_state ->
      kv_msg_id = acc_state.next_msg_id
      next_state = NodeState.increment_msg_id(acc_state)

      body = %{
        "type" => "write",
        "key" => commit_key(key),
        "value" => offset,
        "msg_id" => kv_msg_id
      }

      send(next_state.node_id, "lin-kv", body)
      next_state
    end)
  end

  defp handle_list_committed_offsets(message, state) do
    keys = message["body"]["keys"]

    start_multi_key_read(
      message,
      keys,
      :list_committed_offsets,
      &commit_key/1,
      :list_committed_offsets_read,
      state
    )
  end

  defp handle_read_ok(message, state) do
    kv_msg_id = message["body"]["in_reply_to"]

    case Map.get(state.challenge_state.kv_requests, kv_msg_id) do
      {:send_read, client_msg_id} ->
        log = Map.get(message["body"], "value", [])
        updated_kv_requests = Map.delete(state.challenge_state.kv_requests, kv_msg_id)

        updated_challenge_state = %{
          state.challenge_state
          | kv_requests: updated_kv_requests
        }

        state = %{state | challenge_state: updated_challenge_state}
        send_log_cas(client_msg_id, log, state)

      {:poll_read, client_msg_id, key} ->
        handle_multi_read_reply(message, client_msg_id, key, state,
          update_results: &update_poll_results/3,
          reply: &reply_poll/1
        )

      {:list_committed_offsets_read, client_msg_id, key} ->
        handle_multi_read_reply(message, client_msg_id, key, state,
          update_results: &update_list_committed_offsets_results/3,
          reply: &reply_list_committed_offsets/1
        )

      _ ->
        state
    end
  end

  defp handle_cas_ok(message, state) do
    kv_msg_id = message["body"]["in_reply_to"]

    case Map.get(state.challenge_state.kv_requests, kv_msg_id) do
      {:send_cas, client_msg_id, offset} ->
        case Map.get(state.challenge_state.client_requests, client_msg_id) do
          {original_message, _msg} ->
            extra_body = %{"offset" => offset}
            reply(original_message, "send_ok", extra_body)

            challenge_state = state.challenge_state

            updated_client_requests =
              Map.delete(challenge_state.client_requests, client_msg_id)

            challenge_state = %{challenge_state | client_requests: updated_client_requests}

            updated_kv_requests = Map.delete(challenge_state.kv_requests, kv_msg_id)
            challenge_state = %{challenge_state | kv_requests: updated_kv_requests}

            %{state | challenge_state: challenge_state}

          _ ->
            state
        end

      _ ->
        state
    end
  end

  defp handle_error(message, state) do
    kv_msg_id = message["body"]["in_reply_to"]

    case Map.get(state.challenge_state.kv_requests, kv_msg_id) do
      {:send_cas, client_msg_id, _offset} ->
        # Precondition failed, retry from read
        retry_send(state, client_msg_id, kv_msg_id)

      {:send_read, client_msg_id} ->
        # Key does not exist, treat as empty log
        if message["body"]["code"] == 20 do
          updated_kv_requests = Map.delete(state.challenge_state.kv_requests, kv_msg_id)

          updated_challenge_state = %{
            state.challenge_state
            | kv_requests: updated_kv_requests
          }

          state = %{state | challenge_state: updated_challenge_state}
          send_log_cas(client_msg_id, [], state)
        else
          state
        end

      _ ->
        state
    end
  end

  defp retry_send(state, client_msg_id, kv_msg_id) do
    case Map.get(state.challenge_state.client_requests, client_msg_id) do
      {original_message, _msg} ->
        key = original_message["body"]["key"]
        updated_kv_requests = Map.delete(state.challenge_state.kv_requests, kv_msg_id)

        updated_challenge_state = %{
          state.challenge_state
          | kv_requests: updated_kv_requests
        }

        state = %{state | challenge_state: updated_challenge_state}
        send_log_read(key, client_msg_id, state)

      _ ->
        state
    end
  end

  defp start_multi_key_read(
         message,
         keys,
         context_type,
         kv_key_fun,
         kv_req_context_atom,
         state
       ) do
    client_msg_id = message["body"]["msg_id"]

    context = %{
      type: context_type,
      original_message: message,
      remaining_keys: Enum.into(keys, MapSet.new()),
      results: %{}
    }

    updated_client_requests =
      Map.put(state.challenge_state.client_requests, client_msg_id, context)

    updated_challenge_state = %{
      state.challenge_state
      | client_requests: updated_client_requests
    }

    state = %{state | challenge_state: updated_challenge_state}

    Enum.reduce(keys, state, fn key, acc_state ->
      kv_key = kv_key_fun.(key)
      kv_msg_id = acc_state.next_msg_id
      next_state = NodeState.increment_msg_id(acc_state)
      kv_req_context = {kv_req_context_atom, client_msg_id, key}

      updated_kv_requests =
        Map.put(next_state.challenge_state.kv_requests, kv_msg_id, kv_req_context)

      updated_challenge_state = %{
        next_state.challenge_state
        | kv_requests: updated_kv_requests
      }

      next_state = %{next_state | challenge_state: updated_challenge_state}

      body = %{"type" => "read", "key" => kv_key, "msg_id" => kv_msg_id}
      send(next_state.node_id, "lin-kv", body)
      next_state
    end)
  end

  defp handle_multi_read_reply(message, client_msg_id, key, state, opts) do
    kv_msg_id = message["body"]["in_reply_to"]
    update_results_fun = Keyword.fetch!(opts, :update_results)
    reply_fun = Keyword.fetch!(opts, :reply)

    case Map.get(state.challenge_state.client_requests, client_msg_id) do
      nil ->
        state

      context ->
        new_results = update_results_fun.(message, key, context)

        new_context =
          context
          |> Map.put(:results, new_results)
          |> Map.put(:remaining_keys, MapSet.delete(context.remaining_keys, key))

        challenge_state = state.challenge_state

        updated_client_requests =
          Map.put(challenge_state.client_requests, client_msg_id, new_context)

        challenge_state = %{challenge_state | client_requests: updated_client_requests}

        updated_kv_requests = Map.delete(challenge_state.kv_requests, kv_msg_id)
        challenge_state = %{challenge_state | kv_requests: updated_kv_requests}

        state = %{state | challenge_state: challenge_state}

        if MapSet.size(new_context.remaining_keys) == 0 do
          reply_fun.(new_context)

          updated_client_requests =
            Map.delete(state.challenge_state.client_requests, client_msg_id)

          updated_challenge_state = %{
            state.challenge_state
            | client_requests: updated_client_requests
          }

          %{state | challenge_state: updated_challenge_state}
        else
          state
        end
    end
  end

  defp update_poll_results(message, key, context) do
    log = Map.get(message["body"], "value", [])
    start_offset = context.original_message["body"]["offsets"][key]

    msgs =
      log
      |> Enum.with_index()
      |> Enum.filter(fn {_msg, offset} -> offset >= start_offset end)
      |> Enum.map(fn {msg, offset} -> [offset, msg] end)

    Map.put(context.results, key, msgs)
  end

  defp reply_poll(context) do
    extra_body = %{"msgs" => context.results}
    reply(context.original_message, "poll_ok", extra_body)
  end

  defp update_list_committed_offsets_results(message, key, context) do
    offset = Map.get(message["body"], "value")
    if offset, do: Map.put(context.results, key, offset), else: context.results
  end

  defp reply_list_committed_offsets(context) do
    extra_body = %{"offsets" => context.results}
    reply(context.original_message, "list_committed_offsets_ok", extra_body)
  end

  defp send_log_read(key, client_msg_id, state) do
    kv_msg_id = state.next_msg_id
    state = NodeState.increment_msg_id(state)
    context = {:send_read, client_msg_id}
    updated_kv_requests = Map.put(state.challenge_state.kv_requests, kv_msg_id, context)

    updated_challenge_state = %{
      state.challenge_state
      | kv_requests: updated_kv_requests
    }

    state = %{state | challenge_state: updated_challenge_state}

    body = %{"type" => "read", "key" => log_key(key), "msg_id" => kv_msg_id}
    send(state.node_id, "lin-kv", body)
    state
  end

  defp send_log_cas(client_msg_id, log, state) do
    case Map.get(state.challenge_state.client_requests, client_msg_id) do
      {original_message, msg} ->
        key = original_message["body"]["key"]
        offset = Enum.count(log)
        new_log = log ++ [msg]

        kv_msg_id = state.next_msg_id
        state = NodeState.increment_msg_id(state)
        context = {:send_cas, client_msg_id, offset}
        updated_kv_requests = Map.put(state.challenge_state.kv_requests, kv_msg_id, context)

        updated_challenge_state = %{
          state.challenge_state
          | kv_requests: updated_kv_requests
        }

        state = %{state | challenge_state: updated_challenge_state}

        body = %{
          "type" => "cas",
          "key" => log_key(key),
          "from" => log,
          "to" => new_log,
          "create_if_not_exists" => true,
          "msg_id" => kv_msg_id
        }

        send(state.node_id, "lin-kv", body)
        state

      _ ->
        state
    end
  end

  defp log_key(key), do: "log_#{key}"
  defp commit_key(key), do: "commit_#{key}"
end
