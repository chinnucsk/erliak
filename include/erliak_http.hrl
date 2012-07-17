%% RIAK ERLANG HTTP CLIENT INCLUDES
-include_lib("raw_http.hrl").
-include_lib("rhc.hrl").
-include("erliak.hrl").

-export_type([http_connection/0]).
-opaque http_connection() :: #rhc{}.

%% ====================================================================
%% Defines
%% ====================================================================
-define(DEFAULT_ADDRESS, "127.0.0.1").
-define(DEFAULT_PORT, 8098).