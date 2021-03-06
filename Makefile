.PHONY: all test clean doc

all: deps compile

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps

test:
	./rebar skip_deps=true eunit


doc:
	@./rebar doc skip_deps=true
