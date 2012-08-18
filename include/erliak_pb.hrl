%% RIAK ERLANG PB CLIENT INCLUDES
% -include_lib("kernel/include/inet.hrl").
% -include_lib("riak_pb/include/riak_pb.hrl").
% -include_lib("riak_pb/include/riak_kv_pb.hrl").
% -include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

% -include("erliak.hrl").

-include_lib("kernel/include/inet.hrl").
-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riak_pb/include/riak_search_pb.hrl").
%% -include("riakc.hrl"). 

-include("erliak.hrl").

-export_type([pb_connection/0]).
-opaque pb_connection() :: pid().



%% ====================================================================
%% Defines
%% ====================================================================
-define(DEFAULT_ADDRESS, "127.0.0.1").
-define(DEFAULT_PORT, 8087).

-define(PROTO_MAJOR, 1).
-define(PROTO_MINOR, 0).
-define(DEFAULT_TIMEOUT, 60000).
-define(FIRST_RECONNECT_INTERVAL, 100).
-define(MAX_RECONNECT_INTERVAL, 30000).
