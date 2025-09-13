# just command runner
# Docs: https://just.systems/man/en/

all: clean build lint

clean:
    rm -rf ./store
    rm -f ./*.log

build: deps compile format

deps:
    mix deps.get

compile:
    mix compile # --warnings-as-errors
    mix escript.build

format:
    mix format

lint:
    mix credo
    mix dialyzer

iex:
    iex -S mix

test:
    mix test

# TODO: Figure out how to get the challenges working in Livebook and get this target working
node_name := "gossip"
node_host := "host.docker.internal"
livebook:
    iex --name {{node_name}}@{{node_host}} \
        --cookie {{node_name}} \
        -S mix
