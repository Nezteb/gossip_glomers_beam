# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :logger, :default_formatter,
  colors: [enabled: false],
  format: "$time level=$level $message $metadata\n",
  metadata: :all

# TODO: If full path isn't used, Maelstrom somehow swallows the log file?
config :logger, :default_handler,
  config: [
    file: ~c"/Users/noah.betzen/Git/gossip_glomers_elixir/gossip_glomers.log",
    filesync_repeat_interval: 5000,
    file_check: 5000,
    max_no_bytes: 10_000_000,
    max_no_files: 5,
    compress_on_rotate: false
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
# import_config "#{config_env()}.exs"
