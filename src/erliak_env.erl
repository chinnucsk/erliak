-module(erliak_env).
-export([get_env/1, get_env/2, set_env/2]).

-include("erliak.hrl").

-define(APPLICATION, erliak).

get_env(Key) ->
    case application:get_env(?APPLICATION, Key) of
        {ok, Value} -> Value;
        undefined -> ?DEFAULT_TRANSPORT
    end.

get_env(Key, Default) ->
    case application:get_env(?APPLICATION, Key) of
        {ok, Value} -> Value;
        undefined -> Default
    end.

set_env(Key, Value) ->
    application:set_env(?APPLICATION, Key, Value).
