%% RIAK ERLANG HTTP CLIENT INCLUDES
-include_lib("riakhttpc/include/raw_http.hrl").
-include_lib("riakhttpc/include/rhc.hrl").

%% ====================================================================
%% Defines
%% ====================================================================
-define(PB_DEFAULT_ADDRESS, "127.0.0.1").
-define(PB_DEFAULT_PORT, 8087).
-define(HTTP_DEFAULT_ADDRESS, "127.0.0.1").
-define(HTTP_DEFAULT_PORT, 8098).
-define(DEFAULT_PROTOCOL, pb).

%% ====================================================================
%% Internal types
%% ====================================================================
-type transport() :: http | pb. %% The allowed transport types
-type connection_ref() :: pid() | #rhc{}. %% Connection references

%% ====================================================================
%% Records
%% ====================================================================
-record(connection, {transport :: transport(), % transport used for this connection, 	                              
		     connection :: connection_ref() % Connection that is active
		%% TODO more?
	       }).
