import gleam/io

// Gleam docs:
// - https://tour.gleam.run/everything/
// - https://gleam.run/command-line-reference/
// - https://gleam.run/writing-gleam/gleam-toml/
// - https://gleam.run/cheatsheets/gleam-for-elixir-users/
// - https://hexdocs.pm/gleam_stdlib/
// - https://hexdocs.pm/gleam_otp/

// TODO: Implement first challenge:
// Links:
// - https://fly.io/dist-sys/1/
//   - Challenge description
//   - To test: `./maelstrom test -w echo --bin ./gossip_glomers echo --node-count 1 --time-limit 10`

pub fn main() -> Nil {
  io.println("Hello from gossip_glomers!")
}
