defmodule GossipGlomers.NodeState do
  @moduledoc """
  Represents the state of a Maelstrom node.
  """

  @enforce_keys [:node_id, :node_ids]
  defstruct node_id: nil,
            node_ids: nil,
            next_msg_id: 0,
            challenge_state: %{}

  @type t :: %__MODULE__{
          node_id: String.t() | nil,
          node_ids: list(String.t()) | nil,
          next_msg_id: integer(),
          challenge_state: map()
        }

  @spec new(map()) :: t()
  def new(challenge_state \\ %{}) do
    %__MODULE__{
      node_id: nil,
      node_ids: nil,
      challenge_state: challenge_state
    }
  end

  @spec increment_msg_id(t()) :: t()
  def increment_msg_id(%__MODULE__{next_msg_id: current_id} = state) do
    %{state | next_msg_id: current_id + 1}
  end
end
