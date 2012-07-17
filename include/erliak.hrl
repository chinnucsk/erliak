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
%% Defines
%% ====================================================================

-type address() :: string() | atom() | inet:ip_address(). %% The TCP/IP host name or address of the Riak node
-type portnum() :: non_neg_integer(). %% The TCP port number of the Riak node's HTTP/PB interface

-type client_option() :: {transport, atom()}. %% Allowed client options
-type client_options() :: [client_option()]. %% List of client options

-type bucket() :: binary(). %% A bucket name
-type key() :: binary(). %% A key name
-type riakc_obj() :: riakc_obj:riakc_obj(). %% An object (bucket, key, metadata, value) stored in Riak.
-type proplist() :: [tuple()]. %% Type for options

-type client_id() :: binary() | string().

%% ====================================================================
%% Records
%% ====================================================================

-record(connection, {
	  transport_module :: module(), % Transport module
	  connection :: connection_ref() % Connection that is active
			%% TODO more?
	 }).
