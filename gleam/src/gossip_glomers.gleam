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
import gleam/io
import gleam/string

@external(erlang, "io", "get_line")
fn get_line(prompt: String) -> String

pub fn main() -> Nil {
  io.println("Prompt will echo input (empty line to exit)")
  read_loop()
}

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
