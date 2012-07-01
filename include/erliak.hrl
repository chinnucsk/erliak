


%% ====================================================================
%% Defines
%% ====================================================================

%% TODO: in config file?
-define(DEFAULT_TRANSPORT, pb).

%% ====================================================================
%% Internal types
%% ====================================================================

%% Connection references
-type connection_ref() :: erliak_pb:pb_connection() | erliak_http:http_connection().

%% ====================================================================
%% Records
%% ====================================================================

-record(connection, {
			transport_module :: module(), % Transport module
		    connection :: connection_ref() % Connection that is active
			%% TODO more?
		}).
