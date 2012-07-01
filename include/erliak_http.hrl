%% RIAK ERLANG HTTP CLIENT INCLUDES
-include_lib("raw_http.hrl").
-include_lib("rhc.hrl").

-export_type([http_connection/0]).
-opaque http_connection() :: #rhc{}.