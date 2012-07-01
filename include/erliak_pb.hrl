%% RIAK ERLANG PB CLIENT INCLUDES


-export_type([pb_connection/0]).
-opaque pb_connection() :: pid().

%% ====================================================================
%% Defines
%% ====================================================================
-define(DEFAULT_ADDRESS, "127.0.0.1").
-define(DEFAULT_PORT, 8087).
