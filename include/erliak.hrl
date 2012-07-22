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
-type proplist() :: [proplists:property()]. %% Type for options

-type client_id() :: binary() | string().

-type req_id() :: non_neg_integer(). %% Request identifier for streaming requests.
%% TODO allow for more properties when the pb interface exposes more of them
-type bucket_prop() :: {n_val, pos_integer()} | {allow_mult, boolean()}. %% Bucket property definitions.
-type bucket_props() :: [bucket_prop()]. %% Bucket properties

-type mapred_queryterm() ::  {map, mapred_funterm(), Arg::term(), Accumulate :: boolean()} |
                             {reduce, mapred_funterm(), Arg::term(),Accumulate :: boolean()} |
                             {link, Bucket :: riakc_obj:bucket(), Tag :: term(), Accumulate :: boolean()}.
%% A MapReduce phase specification. `map' functions operate on single
%% K/V objects. `reduce' functions operate across collections of
%% inputs from other phases. `link' is a special type of map phase
%% that matches links in the fetched objects. The `Arg' parameter will
%% be passed as the last argument to the phase function. The
%% `Accumulate' param determines whether results from this phase will
%% be returned to the client.
-type mapred_funterm() :: {modfun, Module :: atom(), Function :: atom()} |
                          {qfun, function()} |
                          {strfun, list() | binary()}.
%% A MapReduce phase function specification. `modfun' requires that
%% the compiled module be available on all Riak nodes. `qfun' will
%% only work from the shell (compiled fun() terms refer to compiled
%% code only). `strfun' contains the textual source of an Erlang
%% function but the functionality must be enabled on the Riak cluster.
-type mapred_result() :: [term()].
%% The results of a MapReduce job.
-type mapred_inputs() :: [{bucket(), key()} | {bucket(), key(), term()}] |
                         {modfun, Module::atom(), Function::atom(), [term()]} |
                         bucket() |
                         {index, bucket(), Index::binary(), key()} |
                         {index, bucket(), Index::binary(), StartKey::key(), EndKey::key()}.

%% ====================================================================
%% Records
%% ====================================================================

-record(connection, {
	  transport_module :: module(), % Transport module
	  connection :: connection_ref(), % Connection that is active
    caller :: pid() % PID of the caller, for streaming calls
			%% TODO more?
	 }).
