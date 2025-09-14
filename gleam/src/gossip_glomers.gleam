// TODO: Implement first challenge:
// Links:
// - https://fly.io/dist-sys/1/
//   - Challenge description
//   - To test: `./maelstrom test -w echo --bin ./gossip_glomers echo --node-count 1 --time-limit 10`
//
// NOTE: `echo` and `type` are reserved keywords in Gleam, so do not use that for type/variable names.
//
// TODO: Figure out `gleam/json` and `gleam/dynamic` and `gleam/dynamic/decode`
//
import gleam/dynamic/decode.{type Decoder}
import gleam/io
import gleam/json
import gleam/option.{type Option}
import gleam/string

@external(erlang, "io", "get_line")
fn get_line(prompt: String) -> String

pub fn main() -> Nil {
  io.println("Prompt will echo input (empty line to exit)")
  read_loop()
}

// TODO: Actually use decoder to parse Maelstrom message from stdin
fn read_loop() -> Nil {
  case "> " |> get_line() |> string.trim {
    "" -> {
      io.println("Exiting")
    }
    line -> {
      io.println(line)
      read_loop()
    }
  }
}

pub type NodeId =
  String

pub type MaelstromMessage {
  MaelstromMessage(src: NodeId, dest: NodeId, body: Body)
}

// TODO: Figure out how to have separate types for `Common`, `EchoChallenge`,
// `UniqueIdChallenge`, and others instead of combining them all into one?
// A flattened representation of all possible Maelstrom message bodies
pub type Body {
  // Generic Error
  Error
  // Common
  Init(msg_id: Option(Int), node_id: NodeId, node_ids: List(NodeId))
  InitOk(in_reply_to: Int)
  MaelstromError(in_reply_to: Int, code: Int, text: String)
  // Echo Challenge
  Echo(msg_id: Option(Int), echo_text: String)
  EchoOk(msg_id: Option(Int), in_reply_to: Int, echo_text: String)
  // Unique ID Challenge
  Generate(msg_id: Option(Int))
  GenerateOk(msg_id: Option(Int), in_reply_to: Int, id: String)
}

// JSON Decoders

pub fn maelstrom_message_decoder() -> Decoder(MaelstromMessage) {
  use src <- decode.field("src", decode.string)
  use dest <- decode.field("dest", decode.string)
  use body <- decode.field("body", body_decoder())
  decode.success(MaelstromMessage(src, dest, body))
}

fn body_decoder() -> Decoder(Body) {
  use type_ <- decode.field("type", decode.string)
  case type_ {
    "init" -> {
      use msg_id <- decode.field("msg_id", decode.optional(decode.int))
      use node_id <- decode.field("node_id", decode.string)
      use node_ids <- decode.field("node_ids", decode.list(decode.string))
      decode.success(Init(msg_id, node_id, node_ids))
    }
    "init_ok" -> {
      use in_reply_to <- decode.field("in_reply_to", decode.int)
      decode.success(InitOk(in_reply_to))
    }
    "error" -> {
      use in_reply_to <- decode.field("in_reply_to", decode.int)
      use code <- decode.field("code", decode.int)
      use text <- decode.field("text", decode.string)
      decode.success(MaelstromError(in_reply_to, code, text))
    }
    "echo" -> {
      use msg_id <- decode.field("msg_id", decode.optional(decode.int))
      use echo_text <- decode.field("echo", decode.string)
      decode.success(Echo(msg_id, echo_text))
    }
    "echo_ok" -> {
      use msg_id <- decode.field("msg_id", decode.optional(decode.int))
      use in_reply_to <- decode.field("in_reply_to", decode.int)
      use echo_text <- decode.field("echo", decode.string)
      decode.success(EchoOk(msg_id, in_reply_to, echo_text))
    }
    "generate" -> {
      use msg_id <- decode.field("msg_id", decode.optional(decode.int))
      decode.success(Generate(msg_id))
    }
    "generate_ok" -> {
      use msg_id <- decode.field("msg_id", decode.optional(decode.int))
      use in_reply_to <- decode.field("in_reply_to", decode.int)
      use id <- decode.field("id", decode.string)
      decode.success(GenerateOk(msg_id, in_reply_to, id))
    }
    _ -> decode.failure(Error, "Unknown message type: " <> type_)
  }
}

// JSON Encoders

pub fn maelstrom_message_to_json(msg: MaelstromMessage) -> json.Json {
  json.object([
    #("src", json.string(msg.src)),
    #("dest", json.string(msg.dest)),
    #("body", body_to_json(msg.body)),
  ])
}

fn body_to_json(body: Body) -> json.Json {
  case body {
    Init(msg_id, node_id, node_ids) ->
      json.object([
        #("type", json.string("init")),
        #("msg_id", json.nullable(msg_id, of: json.int)),
        #("node_id", json.string(node_id)),
        #("node_ids", json.array(node_ids, of: json.string)),
      ])
    InitOk(in_reply_to) ->
      json.object([
        #("type", json.string("init_ok")),
        #("in_reply_to", json.int(in_reply_to)),
      ])
    MaelstromError(in_reply_to, code, text) ->
      json.object([
        #("type", json.string("error")),
        #("in_reply_to", json.int(in_reply_to)),
        #("code", json.int(code)),
        #("text", json.string(text)),
      ])
    Echo(msg_id, echo_text) ->
      json.object([
        #("type", json.string("echo")),
        #("msg_id", json.nullable(msg_id, of: json.int)),
        #("echo", json.string(echo_text)),
      ])
    EchoOk(msg_id, in_reply_to, echo_text) ->
      json.object([
        #("type", json.string("echo_ok")),
        #("msg_id", json.nullable(msg_id, of: json.int)),
        #("in_reply_to", json.int(in_reply_to)),
        #("echo", json.string(echo_text)),
      ])
    Generate(msg_id) ->
      json.object([
        #("type", json.string("generate")),
        #("msg_id", json.nullable(msg_id, of: json.int)),
      ])
    GenerateOk(msg_id, in_reply_to, id) ->
      json.object([
        #("type", json.string("generate_ok")),
        #("msg_id", json.nullable(msg_id, of: json.int)),
        #("in_reply_to", json.int(in_reply_to)),
        #("id", json.string(id)),
      ])
    Error -> json.null()
  }
}
