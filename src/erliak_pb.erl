-module(erliak_pb).

%% ====================================================================
%% Includes
%% ====================================================================
-include("erliak_pb.hrl"). % Erliak_pb specific header file
-include_lib("kernel/include/inet.hrl").
-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-behaviour(erliak_transport).
-behaviour(gen_server).

-export([connect/3,
     ping/2,
     get/5,
     put/4,
     delete/5,
     disconnect/1,
     get_server_info/2,
     get_client_id/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ====================================================================
%% Public API
%% ====================================================================

connect(default_address, default_port, Options) ->
    connect(?DEFAULT_ADDRESS, ?DEFAULT_PORT, Options);
connect(Address, Port, Options) ->
    start_link(Address, Port, Options).

ping(Connection, default_timeout) ->
    e_ping(Connection);
ping(Connection, Timeout) ->
    e_ping(Connection, Timeout).

get(Connection, Bucket, Key, Options, default_timeout) ->
    e_get(Connection, Bucket, Key, Options);
get(Connection, Bucket, Key, Options, Timeout) ->
    e_get(Connection, Bucket, Key, Options, Timeout).

put(Connection, Object, Options, default_timeout) ->
    e_put(Connection, Object, Options);
put(Connection, Object, Options, Timeout) ->
    e_put(Connection, Object, Options, Timeout).

delete(Connection, Bucket, Key, Options, default_timeout) ->
    e_delete(Connection, Bucket, Key, Options);
delete(Connection, Bucket, Key, Options, Timeout) ->
    e_delete(Connection, Bucket, Key, Options, Timeout).

disconnect(Connection) ->
    stop(Connection).

get_server_info(Connection, default_timeout) ->
    e_get_server_info(Connection);
get_server_info(Connection, Timeout) ->
    e_get_server_info(Connection, Timeout).

get_client_id(Connection, default_timeout) ->
    e_get_client_id(Connection);
get_client_id(Connection, Timeout) ->    
    e_get_client_id(Connection, Timeout).

%% ====================================================================
%% Private (from riak-erlang-client)
%% ====================================================================


-define(PROTO_MAJOR, 1).
-define(PROTO_MINOR, 0).
-define(DEFAULT_TIMEOUT, 60000).
-define(FIRST_RECONNECT_INTERVAL, 100).
-define(MAX_RECONNECT_INTERVAL, 30000).

-type address() :: string() | atom() | inet:ip_address(). %% The TCP/IP host name or address of the Riak node
-type portnum() :: non_neg_integer(). %% The TCP port number of the Riak node's Protocol Buffers interface
-type client_option()  :: queue_if_disconnected | {queue_if_disconnected, boolean()} |
                   auto_reconnect | {auto_reconnect, boolean()}.
%% Options for starting or modifying the connection:
%% `queue_if_disconnected' when present or true will cause requests to
%% be queued while the connection is down. `auto_reconnect' when
%% present or true will automatically attempt to reconnect to the
%% server if the connection fails or is lost.
-type client_options() :: [client_option()]. %% A list of client options.
-type client_id() :: binary(). %% A client identifier, used for differentiating client processes
-type bucket() :: binary(). %% A bucket name.
-type key() :: binary(). %% A key name.
-type riakc_obj() :: riakc_obj:riakc_obj(). %% An object (bucket, key, metadata, value) stored in Riak.
-type req_id() :: non_neg_integer(). %% Request identifier for streaming requests.
-type rpb_req() :: atom() | tuple().
-type ctx() :: any().
-type rpb_resp() :: atom() | tuple().
-type server_prop() :: {node, binary()} | {server_version, binary()}. %% Server properties, as returned by the `get_server_info/1' call.
-type server_info() :: [server_prop()]. %% A response from the `get_server_info/1' call.
-type bucket_prop() :: {n_val, pos_integer()} | {allow_mult, boolean()}. %% Bucket property definitions.
-type bucket_props() :: [bucket_prop()]. %% Bucket properties
-type quorum() :: non_neg_integer() | one | all | quorum | default.  %% A quorum setting for get/put/delete requests.
-type read_quorum() :: {r, ReadQuorum::quorum()} |
                       {pr, PrimaryReadQuorum::quorum()}. %% Valid quorum options for get requests.
-type write_quorum() :: {w, WriteQuorum::quorum()} |
                        {dw, DurableWriteQuorum::quorum()} |
                        {pw, PrimaryWriteQuorum::quorum()}. %% Valid quorum options for write requests.
-type delete_quorum() :: read_quorum() |
                         write_quorum() |
                         {rw, ReadWriteQuorum::quorum()}. %% Valid quorum options for delete requests. Note that `rw' is deprecated in Riak 1.0 and later.
-type get_option() :: read_quorum() |
                      {if_modified, riakc_obj:vclock()} |
                      {notfound_ok, boolean()} |
                      {basic_quorum, boolean()} |
                      head | deletedvclock.
%% Valid request options for get requests. When `if_modified' is
%% specified with a vclock, the request will fail if the object has
%% not changed. When `head' is specified, only the metadata will be
%% returned. When `deletedvclock' is specified, the vector clock of
%% the tombstone will be returned if the object has been recently
%% deleted.
-type put_option() :: write_quorum() | return_body | return_head | if_not_modified | if_none_match.
%% Valid request options for put requests. `return_body' returns the
%% entire result of storing the object. `return_head' returns the
%% metadata from the result of storing the object. `if_not_modified'
%% will cause the request to fail if the local and remote vclocks do
%% not match. `if_none_match' will cause the request to fail if the
%% object already exists in Riak.
-type get_options() :: [get_option()]. %% A list of options for a get request.
-type put_options() :: [put_option()]. %% A list of options for a put request.
-type delete_options() :: [delete_quorum()]. %% A list of options for a delete request.
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
%% Inputs for a MapReduce job.
-type connection_failure() :: {Reason::term(), FailureCount::integer()}.
%% The reason for connection failure and how many times that type of
%% failure has occurred since startup.
-type timeout_name() :: ping_timeout | get_client_id_timeout |
                           set_client_id_timeout | get_server_info_timeout |
                           get_timeout | put_timeout | delete_timeout |
                           list_buckets_timeout | list_buckets_call_timeout |
                           list_keys_timeout | stream_list_keys_timeout |
                           stream_list_keys_call_timeout | get_bucket_timeout |
                           get_bucket_call_timeout | set_bucket_timeout |
                           set_bucket_call_timeout | mapred_timeout |
                           mapred_call_timeout | mapred_stream_timeout |
                           mapred_stream_call_timeout | mapred_bucket_timeout |
                           mapred_bucket_call_timeout | mapred_bucket_stream_call_timeout |
                           search_timeout | search_call_timeout | timeout.
%% Which client operation the default timeout is being requested
%% for. `timeout' is the global default timeout. Any of these defaults
%% can be overridden by setting the application environment variable
%% of the same name on the `riakc' application, for example:
%% `application:set_env(riakc, ping_timeout, 5000).'
-record(request, {ref :: reference(), msg :: rpb_req(), from, ctx :: ctx(), timeout :: timeout(),
                  tref :: reference() | undefined }).
-record(state, {address :: address(),    % address to connect to
                port :: portnum(),       % port to connect to
                auto_reconnect = false :: boolean(), % if true, automatically reconnects to server
                                        % if false, exits on connection failure/request timeout
                queue_if_disconnected = false :: boolean(), % if true, add requests to queue if disconnected
                sock :: port(),       % gen_tcp socket
                active :: #request{} | undefined,     % active request
                queue :: queue() | undefined,      % queue of pending requests
                connects=0 :: non_neg_integer(), % number of successful connects
                failed=[] :: [connection_failure()],  % breakdown of failed connects
                connect_timeout=infinity :: timeout(), % timeout of TCP connection
                reconnect_interval=?FIRST_RECONNECT_INTERVAL :: non_neg_integer()}).


%% @doc Create a linked process to talk with the riak server on Address:Port
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    start_link(Address, Port, []).

%% @doc Create a linked process to talk with the riak server on Address:Port with Options.
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum(), client_options()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port, Options) when is_list(Options) ->
    gen_server:start_link(?MODULE, [Address, Port, Options], []).

%% @doc Create a process to talk with the riak server on Address:Port.
%%      Client id will be assigned by the server.
-spec start(address(), portnum()) -> {ok, pid()} | {error, term()}.
start(Address, Port) ->
    start(Address, Port, []).

%% @doc Create a process to talk with the riak server on Address:Port with Options.
-spec start(address(), portnum(), client_options()) -> {ok, pid()} | {error, term()}.
start(Address, Port, Options) when is_list(Options) ->
    gen_server:start(?MODULE, [Address, Port, Options], []).

%% @doc Disconnect the socket and stop the process.
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop).

%% @doc Change the options for this socket.  Allows you to connect with one
%%      set of options then run with another (e.g. connect with no options to
%%      make sure the server is there, then enable queue_if_disconnected).
%% @equiv set_options(Pid, Options, infinity)
%% @see start_link/3
-spec set_options(pid(), client_options()) -> ok.
set_options(Pid, Options) ->
    set_options(Pid, Options, infinity).

%% @doc Like set_options/2, but with a gen_server timeout.
%% @see start_link/3
-spec set_options(pid(), client_options(), timeout()) -> ok.
set_options(Pid, Options, Timeout) ->
    gen_server:call(Pid, {set_options, Options}, Timeout).

%% @doc Determines whether the client is connected. Returns true if
%% connected, or false and a list of connection failures and frequencies if
%% disconnected.
%% @equiv is_connected(Pid, infinity)
-spec is_connected(pid()) -> true | {false, [connection_failure()]}.
is_connected(Pid) ->
    is_connected(Pid, infinity).

%% @doc Determines whether the client is connected, with the specified
%% timeout to the client process. Returns true if connected, or false
%% and a list of connection failures and frequencies if disconnected.
%% @see is_connected/1
-spec is_connected(pid(), timeout()) -> true | {false, [connection_failure()]}.
is_connected(Pid, Timeout) ->
    gen_server:call(Pid, is_connected, Timeout).

%% @doc Ping the server
%% @equiv e_ping(Pid, default_timeout(ping_timeout))
-spec e_ping(pid()) -> ok | {error, term()}.
e_ping(Pid) ->
    e_ping(Pid, default_timeout(ping_timeout)).

%% @doc Ping the server specifying timeout
-spec e_ping(pid(), timeout()) -> ok | {error, term()}.
e_ping(Pid, Timeout) ->
    gen_server:call(Pid, {req, rpbpingreq, Timeout}, infinity).

%% @doc Get the client id for this connection
%% @equiv e_get_client_id(Pid, default_timeout(get_client_id_timeout))
-spec e_get_client_id(pid()) -> {ok, client_id()} | {error, term()}.
e_get_client_id(Pid) ->
    e_get_client_id(Pid, default_timeout(get_client_id_timeout)).

%% @doc Get the client id for this connection specifying timeout
-spec e_get_client_id(pid(), timeout()) -> {ok, client_id()} | {error, term()}.
e_get_client_id(Pid, Timeout) ->
    gen_server:call(Pid, {req, rpbgetclientidreq, Timeout}, infinity).

%% @doc Set the client id for this connection
%% @equiv set_client_id(Pid, ClientId, default_timeout(set_client_id_timeout))
-spec set_client_id(pid(), client_id()) -> {ok, client_id()} | {error, term()}.
set_client_id(Pid, ClientId) ->
    set_client_id(Pid, ClientId, default_timeout(set_client_id_timeout)).

%% @doc Set the client id for this connection specifying timeout
-spec set_client_id(pid(), client_id(), timeout()) -> {ok, client_id()} | {error, term()}.
set_client_id(Pid, ClientId, Timeout) ->
    gen_server:call(Pid, {req, #rpbsetclientidreq{client_id = ClientId}, Timeout}, infinity).

%% @doc Get the server information for this connection
%% @equiv e_get_server_info(Pid, default_timeout(get_server_info_timeout))
-spec e_get_server_info(pid()) -> {ok, server_info()} | {error, term()}.
e_get_server_info(Pid) ->
    e_get_server_info(Pid, default_timeout(get_server_info_timeout)).

%% @doc Get the server information for this connection specifying timeout
-spec e_get_server_info(pid(), timeout()) -> {ok, server_info()} | {error, term()}.
e_get_server_info(Pid, Timeout) ->
    gen_server:call(Pid, {req, rpbgetserverinforeq, Timeout}, infinity).

%% @doc Get bucket/key from the server.
%%      Will return {error, notfound} if the key is not on the serverk.
%% @equiv e_get(Pid, Bucket, Key, [], default_timeout(get_timeout))
-spec e_get(pid(), bucket(), key()) -> {ok, riakc_obj()} | {error, term()}.
e_get(Pid, Bucket, Key) ->
    e_get(Pid, Bucket, Key, [], default_timeout(get_timeout)).

%% @doc Get bucket/key from the server specifying timeout.
%%      Will return {error, notfound} if the key is not on the server.
%% @equiv e_get(Pid, Bucket, Key, Options, Timeout)
-spec e_get(pid(), bucket(), key(), TimeoutOrOptions::timeout() |  get_options()) ->
                 {ok, riakc_obj()} | {error, term() | unchanged}.
e_get(Pid, Bucket, Key, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    e_get(Pid, Bucket, Key, [], Timeout);
e_get(Pid, Bucket, Key, Options) ->
    e_get(Pid, Bucket, Key, Options, default_timeout(get_timeout)).

%% @doc Get bucket/key from the server supplying options and timeout.
%%      <code>{error, unchanged}</code> will be returned when the
%%      <code>{if_modified, Vclock}</code> option is specified and the
%%      object is unchanged.
-spec e_get(pid(), bucket(), key(), get_options(), timeout()) ->
                 {ok, riakc_obj()} | {error, term() | unchanged}.
e_get(Pid, Bucket, Key, Options, Timeout) ->
    Req = get_options(Options, #rpbgetreq{bucket = Bucket, key = Key}),
    gen_server:call(Pid, {req, Req, Timeout}, infinity).

%% @doc Put the metadata/value in the object under bucket/key
%% @equiv e_put(Pid, Obj, [])
%% @see e_put/4
-spec e_put(pid(), riakc_obj()) ->
                 ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
e_put(Pid, Obj) ->
    e_put(Pid, Obj, []).

%% @doc Put the metadata/value in the object under bucket/key with options or timeout.
%% @equiv e_put(Pid, Obj, Options, Timeout)
%% @see e_put/4
-spec e_put(pid(), riakc_obj(), TimeoutOrOptions::timeout() | put_options()) ->
                 ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
e_put(Pid, Obj, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    e_put(Pid, Obj, [], Timeout);
e_put(Pid, Obj, Options) ->
    e_put(Pid, Obj, Options, default_timeout(put_timeout)).

%% @doc Put the metadata/value in the object under bucket/key with
%%      options and timeout. Put throws `siblings' if the
%%      riakc_obj contains siblings that have not been resolved by
%%      calling {@link riakc_obj:select_sibling/2.} or {@link
%%      riakc_obj:update_value/2} and {@link
%%      riakc_obj:update_metadata/2}.  If the object has no key and
%%      the Riak node supports it, `{ok, Key::key()}' will be returned
%%      when the object is created, or `{ok, Obj::riakc_obj()}' if
%%      `return_body' was specified.
%% @throws siblings
%% @end
-spec e_put(pid(), riakc_obj(), put_options(), timeout()) ->
                 ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
e_put(Pid, Obj, Options, Timeout) ->
    Content = riak_pb_kv_codec:encode_content({riakc_obj:get_update_metadata(Obj),
                                               riakc_obj:get_update_value(Obj)}),
    Req = put_options(Options,
                      #rpbputreq{bucket = riakc_obj:bucket(Obj),
                                 key = riakc_obj:key(Obj),
                                 vclock = riakc_obj:vclock(Obj),
                                 content = Content}),
    gen_server:call(Pid, {req, Req, Timeout}, infinity).

%% @doc Delete the key/value
%% @equiv e_delete(Pid, Bucket, Key, [])
-spec e_delete(pid(), bucket(), key()) -> ok | {error, term()}.
e_delete(Pid, Bucket, Key) ->
    e_delete(Pid, Bucket, Key, []).

%% @doc Delete the key/value specifying timeout or options. <em>Note that the rw quorum is deprecated, use r and w.</em>
%% @equiv e_delete(Pid, Bucket, Key, Options, Timeout)
-spec e_delete(pid(), bucket(), key(), TimeoutOrOptions::timeout() | delete_options()) ->
                    ok | {error, term()}.
e_delete(Pid, Bucket, Key, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    e_delete(Pid, Bucket, Key, [], Timeout);
e_delete(Pid, Bucket, Key, Options) ->
    e_delete(Pid, Bucket, Key, Options, default_timeout(delete_timeout)).

%% @doc Delete the key/value with options and timeout. <em>Note that the rw quorum is deprecated, use r and w.</em>
-spec e_delete(pid(), bucket(), key(), delete_options(), timeout()) -> ok | {error, term()}.
e_delete(Pid, Bucket, Key, Options, Timeout) ->
    Req = delete_options(Options, #rpbdelreq{bucket = Bucket, key = Key}),
    gen_server:call(Pid, {req, Req, Timeout}, infinity).

%% @doc Delete the object at Bucket/Key, giving the vector clock.
%% @equiv delete_vclock(Pid, Bucket, Key, VClock, [])
-spec delete_vclock(pid(), bucket(), key(), riakc_obj:vclock()) -> ok | {error, term()}.
delete_vclock(Pid, Bucket, Key, VClock) ->
    delete_vclock(Pid, Bucket, Key, VClock, []).

%% @doc Delete the object at Bucket/Key, specifying timeout or options and giving the vector clock.
%% @equiv delete_vclock(Pid, Bucket, Key, VClock, Options, Timeout)
-spec delete_vclock(pid(), bucket(), key(), riakc_obj:vclock(), TimeoutOrOptions::timeout() | delete_options()) ->
                           ok | {error, term()}.
delete_vclock(Pid, Bucket, Key, VClock, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    delete_vclock(Pid, Bucket, Key, VClock, [], Timeout);
delete_vclock(Pid, Bucket, Key, VClock, Options) ->
    delete_vclock(Pid, Bucket, Key, VClock, Options, default_timeout(delete_timeout)).

%% @doc Delete the key/value with options and timeout and giving the
%% vector clock. This form of delete ensures that subsequent get and
%% put requests will be correctly ordered with the delete.
%% @see delete_obj/4
-spec delete_vclock(pid(), bucket(), key(), riakc_obj:vclock(), delete_options(), timeout()) ->
                           ok | {error, term()}.
delete_vclock(Pid, Bucket, Key, VClock, Options, Timeout) ->
    Req = delete_options(Options, #rpbdelreq{bucket = Bucket, key = Key,
            vclock=VClock}),
    gen_server:call(Pid, {req, Req, Timeout}, infinity).


%% @doc Delete the riak object.
%% @equiv delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj), riakc_obj:vclock(Obj))
%% @see delete_vclock/6
-spec delete_obj(pid(), riakc_obj()) -> ok | {error, term()}.
delete_obj(Pid, Obj) ->
    delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj),
        riakc_obj:vclock(Obj), [], default_timeout(delete_timeout)).

%% @doc Delete the riak object with options.
%% @equiv delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj), riakc_obj:vclock(Obj), Options)
%% @see delete_vclock/6
-spec delete_obj(pid(), riakc_obj(), delete_options()) -> ok | {error, term()}.
delete_obj(Pid, Obj, Options) ->
    delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj),
        riakc_obj:vclock(Obj), Options, default_timeout(delete_timeout)).

%% @doc Delete the riak object with options and timeout.
%% @equiv delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj), riakc_obj:vclock(Obj), Options, Timeout)
%% @see delete_vclock/6
-spec delete_obj(pid(), riakc_obj(), delete_options(), timeout()) -> ok | {error, term()}.
delete_obj(Pid, Obj, Options, Timeout) ->
    delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj),
        riakc_obj:vclock(Obj), Options, Timeout).

%% @doc List all buckets on the server.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv list_buckets(Pid, default_timeout(list_buckets_timeout))
-spec list_buckets(pid()) -> {ok, [bucket()]} | {error, term()}.
list_buckets(Pid) ->
    list_buckets(Pid, default_timeout(list_buckets_timeout)).

%% @doc List all buckets on the server specifying server-side timeout.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv list_buckets(Pid, Timeout, default_timeout(list_buckets_call_timeout))
-spec list_buckets(pid(), timeout()) -> {ok, [bucket()]} | {error, term()}.
list_buckets(Pid, Timeout) ->
    list_buckets(Pid, Timeout, default_timeout(list_buckets_call_timeout)).

%% @doc List all buckets on the server specifying server-side and local
%%      call timeout.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
-spec list_buckets(pid(), timeout(), timeout()) -> {ok, [bucket()]} |
                                                   {error, term()}.
list_buckets(Pid, Timeout, CallTimeout) ->
    gen_server:call(Pid, {req, rpblistbucketsreq, Timeout}, CallTimeout).

%% @doc List all keys in a bucket
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv list_keys(Pid, Bucket, default_timeout(list_keys_timeout))
-spec list_keys(pid(), bucket()) -> {ok, [key()]} | {error, term()}.
list_keys(Pid, Bucket) ->
    list_keys(Pid, Bucket, default_timeout(list_keys_timeout)).

%% @doc List all keys in a bucket specifying timeout. This is
%% implemented using {@link stream_list_keys/3} and then waiting for
%% the results to complete streaming.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
-spec list_keys(pid(), bucket(), timeout()) -> {ok, [key()]} | {error, term()}.
list_keys(Pid, Bucket, Timeout) ->
    case stream_list_keys(Pid, Bucket, Timeout) of
        {ok, ReqId} ->
            wait_for_listkeys(ReqId, Timeout);
        Error ->
            Error
    end.

%% @doc Stream list of keys in the bucket to the calling process.  The
%%      process receives these messages.
%% ```    {ReqId::req_id(), {keys, [key()]}}
%%        {ReqId::req_id(), done}'''
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv stream_list_keys(Pid, Bucket, default_timeout(stream_list_keys_timeout))
-spec stream_list_keys(pid(), bucket()) -> {ok, req_id()} | {error, term()}.
stream_list_keys(Pid, Bucket) ->
    stream_list_keys(Pid, Bucket, default_timeout(stream_list_keys_timeout)).

%% @doc Stream list of keys in the bucket to the calling process specifying server side
%%      timeout.
%%      The process receives these messages.
%% ```    {ReqId::req_id(), {keys, [key()]}}
%%        {ReqId::req_id(), done}'''
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv stream_list_keys(Pid, Bucket, Timeout, default_timeout(stream_list_keys_call_timeout))
-spec stream_list_keys(pid(), bucket(), timeout()) -> {ok, req_id()} | {error, term()}.
stream_list_keys(Pid, Bucket, Timeout) ->
    stream_list_keys(Pid, Bucket, Timeout, default_timeout(stream_list_keys_call_timeout)).

%% @doc Stream list of keys in the bucket to the calling process specifying server side
%%      timeout and local call timeout.
%%      The process receives these messages.
%% ```    {ReqId::req_id(), {keys, [key()]}}
%%        {ReqId::req_id(), done}'''
%% <em>This is a potentially expensive operation and should not be used in production.</em>
-spec stream_list_keys(pid(), bucket(), timeout(), timeout()) -> {ok, req_id()} |
                                                                 {error, term()}.
stream_list_keys(Pid, Bucket, Timeout, CallTimeout) ->
    ReqMsg = #rpblistkeysreq{bucket = Bucket},
    ReqId = mk_reqid(),
    gen_server:call(Pid, {req, ReqMsg, Timeout, {ReqId, self()}}, CallTimeout).

%% @doc Get bucket properties.
%% @equiv get_bucket(Pid, Bucket, default_timeout(get_bucket_timeout))
-spec get_bucket(pid(), bucket()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(Pid, Bucket) ->
    get_bucket(Pid, Bucket, default_timeout(get_bucket_timeout)).

%% @doc Get bucket properties specifying a server side timeout.
%% @equiv get_bucket(Pid, Bucket, Timeout, default_timeout(get_bucket_call_timeout))
-spec get_bucket(pid(), bucket(), timeout()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(Pid, Bucket, Timeout) ->
    get_bucket(Pid, Bucket, Timeout, default_timeout(get_bucket_call_timeout)).

%% @doc Get bucket properties specifying a server side and local call timeout.
-spec get_bucket(pid(), bucket(), timeout(), timeout()) -> {ok, bucket_props()} |
                                                           {error, term()}.
get_bucket(Pid, Bucket, Timeout, CallTimeout) ->
    Req = #rpbgetbucketreq{bucket = Bucket},
    gen_server:call(Pid, {req, Req, Timeout}, CallTimeout).

%% @doc Set bucket properties.
%% @equiv set_bucket(Pid, Bucket, BucketProps, default_timeout(set_bucket_timeout))
-spec set_bucket(pid(), bucket(), bucket_props()) -> ok | {error, term()}.
set_bucket(Pid, Bucket, BucketProps) ->
    set_bucket(Pid, Bucket, BucketProps, default_timeout(set_bucket_timeout)).

%% @doc Set bucket properties specifying a server side timeout.
%% @equiv set_bucket(Pid, Bucket, BucketProps, Timeout, default_timeout(set_bucket_call_timeout))
-spec set_bucket(pid(), bucket(), bucket_props(), timeout()) -> ok | {error, term()}.
set_bucket(Pid, Bucket, BucketProps, Timeout) ->
    set_bucket(Pid, Bucket, BucketProps, Timeout,
               default_timeout(set_bucket_call_timeout)).

%% @doc Set bucket properties specifying a server side and local call timeout.
-spec set_bucket(pid(), bucket(), bucket_props(), timeout(), timeout()) -> ok | {error, term()}.
set_bucket(Pid, Bucket, BucketProps, Timeout, CallTimeout) ->
    PbProps = riak_pb_kv_codec:encode_bucket_props(BucketProps),
    Req = #rpbsetbucketreq{bucket = Bucket, props = PbProps},
    gen_server:call(Pid, {req, Req, Timeout}, CallTimeout).

%% @doc Perform a MapReduce job across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% @equiv mapred(Inputs, Query, default_timeout(mapred))
-spec mapred(pid(), mapred_inputs(), [mapred_queryterm()]) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Pid, Inputs, Query) ->
    mapred(Pid, Inputs, Query, default_timeout(mapred_timeout)).

%% @doc Perform a MapReduce job across the cluster with a job timeout.
%%      See the MapReduce documentation for explanation of behavior.
%% @equiv mapred(Pid, Inputs, Query, Timeout, default_timeout(mapred_call_timeout))
-spec mapred(pid(), mapred_inputs(), [mapred_queryterm()], timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Pid, Inputs, Query, Timeout) ->
    mapred(Pid, Inputs, Query, Timeout, default_timeout(mapred_call_timeout)).

%% @doc Perform a MapReduce job across the cluster with a job and
%%      local call timeout.  See the MapReduce documentation for
%%      explanation of behavior. This is implemented by using
%%      <code>mapred_stream/6</code> and then waiting for all results.
%% @see mapred_stream/6
-spec mapred(pid(), mapred_inputs(), [mapred_queryterm()], timeout(), timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Pid, Inputs, Query, Timeout, CallTimeout) ->
    case mapred_stream(Pid, Inputs, Query, self(), Timeout, CallTimeout) of
        {ok, ReqId} ->
            wait_for_mapred(ReqId, Timeout);
        Error ->
            Error
    end.

%% @doc Perform a streaming MapReduce job across the cluster sending results
%%      to ClientPid.
%%      See the MapReduce documentation for explanation of behavior.
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @equiv mapred_stream(ConnectionPid, Inputs, Query, ClientPid, default_timeout(mapred_stream_timeout))
-spec mapred_stream(ConnectionPid::pid(),Inputs::mapred_inputs(),Query::[mapred_queryterm()], ClientPid::pid()) ->
                           {ok, req_id()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_stream(Pid, Inputs, Query, ClientPid) ->
    mapred_stream(Pid, Inputs, Query, ClientPid, default_timeout(mapred_stream_timeout)).

%% @doc Perform a streaming MapReduce job with a timeout across the cluster.
%%      sending results to ClientPid.
%%      See the MapReduce documentation for explanation of behavior.
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @equiv mapred_stream(ConnectionPid, Inputs, Query, ClientPid, Timeout, default_timeout(mapred_stream_call_timeout))
-spec mapred_stream(ConnectionPid::pid(),Inputs::mapred_inputs(),Query::[mapred_queryterm()], ClientPid::pid(), Timeout::timeout()) ->
                           {ok, req_id()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_stream(Pid, Inputs, Query, ClientPid, Timeout) ->
    mapred_stream(Pid, Inputs, Query, ClientPid, Timeout,
                  default_timeout(mapred_stream_call_timeout)).

%% @doc Perform a streaming MapReduce job with a map/red timeout across the cluster,
%%      a local call timeout and sending results to ClientPid.
%%      See the MapReduce documentation for explanation of behavior.
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
-spec mapred_stream(ConnectionPid::pid(),Inputs::mapred_inputs(),
                    Query::[mapred_queryterm()], ClientPid::pid(),
                    Timeout::timeout(), CallTimeout::timeout()) ->
                           {ok, req_id()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_stream(Pid, Inputs, Query, ClientPid, Timeout, CallTimeout) ->
    MapRed = [{'inputs', Inputs},
              {'query', Query},
              {'timeout', Timeout}],
    send_mapred_req(Pid, MapRed, ClientPid, CallTimeout).

%% @doc Perform a MapReduce job against a bucket across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%% @equiv mapred_bucket(Pid, Bucket, Query, default_timeout(mapred_bucket_timeout))
-spec mapred_bucket(Pid::pid(), Bucket::bucket(), Query::[mapred_queryterm()]) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_bucket(Pid, Bucket, Query) ->
    mapred_bucket(Pid, Bucket, Query, default_timeout(mapred_bucket_timeout)).

%% @doc Perform a MapReduce job against a bucket with a timeout
%%      across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%% @equiv mapred_bucket(Pid, Bucket, Query, Timeout, default_timeout(mapred_bucket_call_timeout))
-spec mapred_bucket(Pid::pid(), Bucket::bucket(), Query::[mapred_queryterm()], Timeout::timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_bucket(Pid, Bucket, Query, Timeout) ->
    mapred_bucket(Pid, Bucket, Query, Timeout, default_timeout(mapred_bucket_call_timeout)).

%% @doc Perform a MapReduce job against a bucket with a timeout
%%      across the cluster and local call timeout.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
-spec mapred_bucket(Pid::pid(), Bucket::bucket(), Query::[mapred_queryterm()],
                    Timeout::timeout(), CallTimeout::timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_bucket(Pid, Bucket, Query, Timeout, CallTimeout) ->
    case mapred_bucket_stream(Pid, Bucket, Query, self(), Timeout, CallTimeout) of
        {ok, ReqId} ->
            wait_for_mapred(ReqId, Timeout);
        Error ->
            Error
    end.

%% @doc Perform a streaming MapReduce job against a bucket with a timeout
%%      across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @equiv     mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout, default_timeout(mapred_bucket_stream_call_timeout))
-spec mapred_bucket_stream(ConnectionPid::pid(), bucket(), [mapred_queryterm()], ClientPid::pid(), timeout()) ->
                                  {ok, req_id()} |
                                  {error, term()}.
mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout) ->
    mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout,
                         default_timeout(mapred_bucket_stream_call_timeout)).

%% @doc Perform a streaming MapReduce job against a bucket with a server timeout
%%      across the cluster and a call timeout.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
-spec mapred_bucket_stream(ConnectionPid::pid(), bucket(), [mapred_queryterm()], ClientPid::pid(), timeout(), timeout()) ->
                                  {ok, req_id()} | {error, term()}.
mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout, CallTimeout) ->
    MapRed = [{'inputs', Bucket},
              {'query', Query},
              {'timeout', Timeout}],
    send_mapred_req(Pid, MapRed, ClientPid, CallTimeout).


%% @doc Execute a search query. This command will return an error
%%      unless executed against a Riak Search cluster.  Because
%%      Protocol Buffers has no native Search interface, this uses the
%%      search inputs to MapReduce.
-spec search(pid(), bucket(), string()) ->  {ok, mapred_result()} | {error, term()}.
search(Pid, Bucket, SearchQuery) ->
    %% Run a MapReduce operation using reduce_identity to get a list
    %% of BKeys.
    IdentityQuery = [{reduce,
                      {modfun, riak_kv_mapreduce, reduce_identity},
                      [{reduce_phase_only_1, true}],
                      true}],
    case search(Pid, Bucket, SearchQuery, IdentityQuery,
                default_timeout(search_timeout)) of
        {ok, [{_, Results}]} ->
            %% Unwrap the results.
            {ok, Results};
        Other -> Other
    end.

%% @doc Execute a search query and feed the results into a MapReduce
%%      query.  This command will return an error
%%      unless executed against a Riak Search cluster.
%% @equiv search(Pid, Bucket, SearchQuery, MRQuery, Timeout, default_timeout(search_call_timeout))
-spec search(pid(), bucket(), string(), [mapred_queryterm()], timeout()) ->
                    {ok, mapred_result()} | {error, term()}.
search(Pid, Bucket, SearchQuery, MRQuery, Timeout) ->
    search(Pid, Bucket, SearchQuery, MRQuery, Timeout,
           default_timeout(search_call_timeout)).


%% @doc Execute a search query and feed the results into a MapReduce
%%      query with a timeout on the call. This command will return
%%      an error unless executed against a Riak Search cluster.
-spec search(pid(), bucket(), string(), [mapred_queryterm()], timeout(), timeout()) ->
                    {ok, mapred_result()} | {error, term()}.
search(Pid, Bucket, SearchQuery, MRQuery, Timeout, CallTimeout) ->
    Inputs = {modfun, riak_search, mapred_search, [Bucket, SearchQuery]},
    mapred(Pid, Inputs, MRQuery, Timeout, CallTimeout).

%% @doc Execute a secondary index equality query. This functionality
%% is implemented via executing a MapReduce job with an index as the
%% input.
-spec get_index(pid(), bucket(), binary(), key() | integer()) ->
                       {ok, mapred_result()} | {error, term()}.
get_index(Pid, Bucket, Index, Key) ->
    %% Run a MapReduce operation using reduce_identity to get a list
    %% of BKeys.
    Input = {index, Bucket, Index, Key},
    IdentityQuery = [{reduce,
                      {modfun, riak_kv_mapreduce, reduce_identity},
                      [{reduce_phase_only_1, true}],
                      true}],
    case mapred(Pid, Input, IdentityQuery) of
        {ok, [{_, Results}]} ->
            %% Unwrap the results.
            {ok, Results};
        Other -> Other
    end.

%% @doc Execute a secondary index equality query with specified
%% timeouts. This behavior is implemented via executing a MapReduce
%% job with an index as the input.
-spec get_index(pid(), bucket(), binary(), key() | integer(), timeout(), timeout()) ->
                       {ok, mapred_result()} | {error, term()}.
get_index(Pid, Bucket, Index, Key, Timeout, CallTimeout) ->
    %% Run a MapReduce operation using reduce_identity to get a list
    %% of BKeys.
    Input = {index, Bucket, Index, Key},
    IdentityQuery = [{reduce,
                      {modfun, riak_kv_mapreduce, reduce_identity},
                      [{reduce_phase_only_1, true}],
                      true}],
    case mapred(Pid, Input, IdentityQuery, Timeout, CallTimeout) of
        {ok, [{_, Results}]} ->
            %% Unwrap the results.
            {ok, Results};
        Other -> Other
    end.


%% @doc Execute a secondary index range query. This behavior is
%% implemented via executing a MapReduce job with an index as the
%% input.
-spec get_index(pid(), bucket(), binary(), key() | integer(), key() | integer()) ->
                       {ok, mapred_result()} | {error, term()}.
get_index(Pid, Bucket, Index, StartKey, EndKey) ->
    %% Run a MapReduce operation using reduce_identity to get a list
    %% of BKeys.
    Input = {index, Bucket, Index, StartKey, EndKey},
    IdentityQuery = [{reduce,
                      {modfun, riak_kv_mapreduce, reduce_identity},
                      [{reduce_phase_only_1, true}],
                      true}],
    case mapred(Pid, Input, IdentityQuery) of
        {ok, [{_, Results}]} ->
            %% Unwrap the results.
            {ok, Results};
        Other -> Other
    end.

%% @doc Execute a secondary index range query with specified
%% timeouts. This behavior is implemented via executing a MapReduce
%% job with an index as the input.
-spec get_index(pid(), bucket(), binary(), key() | integer(), key() | integer(), timeout(), timeout()) ->
                       {ok, mapred_result()} | {error, term()}.
get_index(Pid, Bucket, Index, StartKey, EndKey, Timeout, CallTimeout) ->
    %% Run a MapReduce operation using reduce_identity to get a list
    %% of BKeys.
    Input = {index, Bucket, Index, StartKey, EndKey},
    IdentityQuery = [{reduce,
                      {modfun, riak_kv_mapreduce, reduce_identity},
                      [{reduce_phase_only_1, true}],
                      true}],
    case mapred(Pid, Input, IdentityQuery, Timeout, CallTimeout) of
        {ok, [{_, Results}]} ->
            %% Unwrap the results.
            {ok, Results};
        Other -> Other
    end.


%% @doc Return the default timeout for an operation if none is provided.
%%      Falls back to the default timeout.
-spec default_timeout(timeout_name()) -> timeout().
default_timeout(OpTimeout) ->
    case application:get_env(riakc, OpTimeout) of
        {ok, EnvTimeout} ->
            EnvTimeout;
        undefined ->
            case application:get_env(riakc, timeout) of
                {ok, Timeout} ->
                    Timeout;
                undefined ->
                    ?DEFAULT_TIMEOUT
            end
    end.

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

%% @private
init([Address, Port, Options]) ->
    %% Schedule a reconnect as the first action.  If the server is up then
    %% the handle_info(reconnect) will run before any requests can be sent.
    State = parse_options(Options, #state{address = Address,
                                          port = Port,
                                          queue = queue:new()}),
    case State#state.auto_reconnect of
        true ->
            self() ! reconnect,
            {ok, State};
        false ->
            case e_connect(State) of
                {error, Reason} ->
                    {stop, {tcp, Reason}};
                Ok ->
                    Ok
            end
    end.

%% @private
handle_call({req, Msg, Timeout}, From, State) when State#state.sock =:= undefined ->
    case State#state.queue_if_disconnected of
        true ->
            {noreply, queue_request(new_request(Msg, From, Timeout), State)};
        false ->
            {reply, {error, disconnected}, State}
    end;
handle_call({req, Msg, Timeout, Ctx}, From, State) when State#state.sock =:= undefined ->
    case State#state.queue_if_disconnected of
        true ->
            {noreply, queue_request(new_request(Msg, From, Timeout, Ctx), State)};
        false ->
            {reply, {error, disconnected}, State}
    end;
handle_call({req, Msg, Timeout}, From, State) when State#state.active =/= undefined ->
    {noreply, queue_request(new_request(Msg, From, Timeout), State)};
handle_call({req, Msg, Timeout, Ctx}, From, State) when State#state.active =/= undefined ->
    {noreply, queue_request(new_request(Msg, From, Timeout, Ctx), State)};
handle_call({req, Msg, Timeout}, From, State) ->
    {noreply, send_request(new_request(Msg, From, Timeout), State)};
handle_call({req, Msg, Timeout, Ctx}, From, State) ->
    {noreply, send_request(new_request(Msg, From, Timeout, Ctx), State)};
handle_call(is_connected, _From, State) ->
    case State#state.sock of
        undefined ->
            {reply, {false, State#state.failed}, State};
        _ ->
            {reply, true, State}
    end;
handle_call({set_options, Options}, _From, State) ->
    {reply, ok, parse_options(Options, State)};
handle_call(stop, _From, State) ->
    _ = e_disconnect(State),
    {stop, normal, ok, State}.

%% @private
handle_info({tcp_error, _Socket, Reason}, State) ->
    error_logger:error_msg("PBC client TCP error for ~p:~p - ~p\n",
                           [State#state.address, State#state.port, Reason]),
    e_disconnect(State);

handle_info({tcp_closed, _Socket}, State) ->
    e_disconnect(State);

%% Make sure the two Sock's match.  If a request timed out, but there was
%% a response queued up behind it we do not want to process it.  Instead
%% it should drop through and be ignored.
handle_info({tcp, Sock, Data}, State=#state{sock = Sock, active = Active}) ->
    [MsgCode|MsgData] = Data,
    Resp = riak_pb_codec:decode(MsgCode, MsgData),
    case Resp of
        #rpberrorresp{} ->
            NewState1 = maybe_reply(on_error(Active, Resp, State)),
            NewState = dequeue_request(NewState1#state{active = undefined});
        _ ->
            case process_response(Active, Resp, State) of
                {reply, Response, NewState0} ->
                    %% Send reply and get ready for the next request - send the next request
                    %% if one is queued up
                    cancel_req_timer(Active#request.tref),
                    _ = send_caller(Response, NewState0#state.active),
                    NewState = dequeue_request(NewState0#state{active = undefined});
                {pending, NewState0} -> %% Request is still pending - do not queue up a new one
                    NewActive = restart_req_timer(Active),
                    NewState = NewState0#state{active = NewActive}
            end
    end,
    ok = inet:setopts(Sock, [{active, once}]),
    {noreply, NewState};
handle_info({req_timeout, Ref}, State) ->
    case State#state.active of %%
        undefined ->
            {noreply, remove_queued_request(Ref, State)};
        Active ->
            case Ref == Active#request.ref of
                true ->  %% Matches the current operation
                    NewState = maybe_reply(on_timeout(State#state.active, State)),
                    e_disconnect(NewState#state{active = undefined});
                false ->
                    {noreply, remove_queued_request(Ref, State)}
            end
    end;
handle_info(reconnect, State) ->
    case e_connect(State) of
        {ok, NewState} ->
            {noreply, dequeue_request(NewState)};
        {error, Reason} ->
            %% Update the failed count and reschedule a reconnection
            NewState = State#state{failed = orddict:update_counter(Reason, 1, State#state.failed)},
            e_disconnect(NewState)
    end;
handle_info(_, State) ->
    {noreply, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ====================================================================
%% internal functions
%% ====================================================================

%% @private
%% Parse options
parse_options([], State) ->
    %% Once all options are parsed, make sure auto_reconnect is enabled
    %% if queue_if_disconnected is enabled.
    case State#state.queue_if_disconnected of
        true ->
            State#state{auto_reconnect = true};
        _ ->
            State
    end;
parse_options([{connect_timeout, T}|Options], State) when is_integer(T) ->
    parse_options(Options, State#state{connect_timeout = T});
parse_options([{queue_if_disconnected,Bool}|Options], State) when
      Bool =:= true; Bool =:= false ->
    parse_options(Options, State#state{queue_if_disconnected = Bool});
parse_options([queue_if_disconnected|Options], State) ->
    parse_options([{queue_if_disconnected, true}|Options], State);
parse_options([{auto_reconnect,Bool}|Options], State) when
      Bool =:= true; Bool =:= false ->
    parse_options(Options, State#state{auto_reconnect = Bool});
parse_options([auto_reconnect|Options], State) ->
    parse_options([{auto_reconnect, true}|Options], State).

maybe_reply({reply, Reply, State}) ->
    Request = State#state.active,
    NewRequest = send_caller(Reply, Request),
    State#state{active = NewRequest};
maybe_reply({noreply, State}) ->
    State.

%% @private
%% Reply to caller - form clause first in case a ReqId/Client was passed
%% in as the context and gen_server:reply hasn't been called yet.
send_caller(Msg, #request{ctx = {ReqId, Client},
                          from = undefined}=Request) ->
    Client ! {ReqId, Msg},
    Request;
send_caller(Msg, #request{from = From}=Request) when From /= undefined ->
    gen_server:reply(From, Msg),
    Request#request{from = undefined}.

get_options([], Req) ->
    Req;
get_options([{basic_quorum, BQ} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{basic_quorum = BQ});
get_options([{notfound_ok, NFOk} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{notfound_ok = NFOk});
get_options([{r, R} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{r = riak_pb_kv_codec:encode_quorum(R)});
get_options([{pr, PR} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{pr = riak_pb_kv_codec:encode_quorum(PR)});
get_options([{if_modified, VClock} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{if_modified = VClock});
get_options([head | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{head = true});
get_options([deletedvclock | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{deletedvclock = true}).


put_options([], Req) ->
    Req;
put_options([{w, W} | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{w = riak_pb_kv_codec:encode_quorum(W)});
put_options([{dw, DW} | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{dw = riak_pb_kv_codec:encode_quorum(DW)});
put_options([{pw, PW} | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{pw = riak_pb_kv_codec:encode_quorum(PW)});
put_options([return_body | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{return_body = 1});
put_options([return_head | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{return_head = true});
put_options([if_not_modified | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{if_not_modified = true});
put_options([if_none_match | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{if_none_match = true}).


delete_options([], Req) ->
    Req;
delete_options([{rw, RW} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{rw = riak_pb_kv_codec:encode_quorum(RW)});
delete_options([{r, R} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{r = riak_pb_kv_codec:encode_quorum(R)});
delete_options([{w, W} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{w = riak_pb_kv_codec:encode_quorum(W)});
delete_options([{pr, PR} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{pr = riak_pb_kv_codec:encode_quorum(PR)});
delete_options([{pw, PW} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{pw = riak_pb_kv_codec:encode_quorum(PW)});
delete_options([{dw, DW} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{dw = riak_pb_kv_codec:encode_quorum(DW)}).

%% Process response from the server - passes back in the request and
%% context the request was issued with.
%% Return noreply if the request is completed, but no reply needed
%%        reply if the request is completed with a reply to the caller
%%        pending if the request has not completed yet (streaming op)
%% @private
-spec process_response(#request{}, rpb_resp(), #state{}) ->
                              {reply, term(), #state{}} |
                              {pending, #state{}}.
process_response(#request{msg = rpbpingreq}, rpbpingresp, State) ->
    {reply, pong, State};
process_response(#request{msg = rpbgetclientidreq},
                 #rpbgetclientidresp{client_id = ClientId}, State) ->
    {reply, {ok, ClientId}, State};
process_response(#request{msg = #rpbsetclientidreq{}},
                 rpbsetclientidresp, State) ->
    {reply, ok, State};
process_response(#request{msg = rpbgetserverinforeq},
                 #rpbgetserverinforesp{node = Node, server_version = ServerVersion}, State) ->
    case Node of
        undefined ->
            NodeInfo = [];
        Node ->
            NodeInfo = [{node, Node}]
    end,
    case ServerVersion of
        undefined ->
            VersionInfo = [];
        ServerVersion ->
            VersionInfo = [{server_version, ServerVersion}]
    end,
    {reply, {ok, NodeInfo++VersionInfo}, State};
process_response(#request{msg = #rpbgetreq{}}, rpbgetresp, State) ->
    %% server just returned the rpbgetresp code - no message was encoded
    {reply, {error, notfound}, State};
process_response(#request{msg = #rpbgetreq{deletedvclock=true}},
                 #rpbgetresp{vclock=VC, content=undefined}, State) ->
    %% server returned a notfound with a vector clock, meaning a tombstone
    {reply, {error, notfound, VC}, State};
process_response(#request{msg = #rpbgetreq{}}, #rpbgetresp{unchanged=true}, State) ->
    %% object was unchanged
    {reply, unchanged, State};
process_response(#request{msg = #rpbgetreq{bucket = Bucket, key = Key}},
                 #rpbgetresp{content = RpbContents, vclock = Vclock}, State) ->
    Contents = riak_pb_kv_codec:decode_contents(RpbContents),
    {reply, {ok, riakc_obj:new_obj(Bucket, Key, Vclock, Contents)}, State};

process_response(#request{msg = #rpbputreq{}},
                 rpbputresp, State) ->
    %% server just returned the rpbputresp code - no message was encoded
    {reply, ok, State};
process_response(#request{ msg = #rpbputreq{}},
                 #rpbputresp{key = Key, content=undefined, vclock=undefined},
                 State) when is_binary(Key) ->
    %% server generated a key and the client didn't request return_body, but
    %% the created key is returned
    {reply, {ok, Key}, State};
process_response(#request{msg = #rpbputreq{bucket = Bucket, key = Key}},
                 #rpbputresp{content = RpbContents, vclock = Vclock,
                     key = NewKey}, State) ->
    Contents = riak_pb_kv_codec:decode_contents(RpbContents),
    ReturnKey = case NewKey of
                    undefined -> Key;
                    _ -> NewKey
                end,
    {reply, {ok, riakc_obj:new_obj(Bucket, ReturnKey, Vclock, Contents)}, State};

process_response(#request{msg = #rpbdelreq{}},
                 rpbdelresp, State) ->
    %% server just returned the rpbdelresp code - no message was encoded
    {reply, ok, State};

process_response(#request{msg = rpblistbucketsreq},
                 #rpblistbucketsresp{buckets = Buckets}, State) ->
    {reply, {ok, Buckets}, State};

process_response(#request{msg = rpblistbucketsreq},
                 rpblistbucketsresp, State) ->
    %% empty buckets generate an empty message
    {reply, {ok, []}, State};

process_response(#request{msg = #rpblistkeysreq{}}=Request,
                 #rpblistkeysresp{done = Done, keys = Keys}, State) ->
    _ = case Keys of
            undefined ->
                ok;
            _ ->
                %% Have to directly use send_caller as may want to reply with done below.
                send_caller({keys, Keys}, Request)
        end,
    case Done of
        true ->
            {reply, done, State};
        1 ->
            {reply, done, State};
        _ ->
            {pending, State}
    end;

process_response(#request{msg = #rpbgetbucketreq{}},
                 #rpbgetbucketresp{props = PbProps}, State) ->
    Props = riak_pb_kv_codec:decode_bucket_props(PbProps),
    {reply, {ok, Props}, State};

process_response(#request{msg = #rpbsetbucketreq{}},
                 rpbsetbucketresp, State) ->
    {reply, ok, State};

process_response(#request{msg = #rpbmapredreq{content_type = ContentType}}=Request,
                 #rpbmapredresp{done = Done, phase=PhaseId, response=Data}, State) ->
    _ = case Data of
            undefined ->
                ok;
            _ ->
                Response = decode_mapred_resp(Data, ContentType),
                send_caller({mapred, PhaseId, Response}, Request)
        end,
    case Done of
        true ->
            {reply, done, State};
        1 ->
            {reply, done, State};
        _ ->
            {pending, State}
    end.

%%
%% Called after sending a message - supports returning a
%% request id for streaming calls
%% @private
after_send(#request{msg = #rpblistkeysreq{}, ctx = {ReqId, _Client}}, State) ->
    {reply, {ok, ReqId}, State};
after_send(#request{msg = #rpbmapredreq{}, ctx = {ReqId, _Client}}, State) ->
    {reply, {ok, ReqId}, State};
after_send(_Request, State) ->
    {noreply, State}.

%% Called on timeout for an operation
%% @private
on_timeout(_Request, State) ->
    {reply, {error, timeout}, State}.
%%
%% Called after receiving an error message - supports reruning
%% an error for streaming calls
%% @private
on_error(_Request, ErrMsg, State) ->
    {reply, fmt_err_msg(ErrMsg), State}.

%% Format the PB encoded error message
fmt_err_msg(ErrMsg) ->
    case ErrMsg#rpberrorresp.errcode of
        Code when Code =:= 1; Code =:= undefined ->
            {error, ErrMsg#rpberrorresp.errmsg};
        Code ->
            {error, {Code, ErrMsg#rpberrorresp.errmsg}}
    end.

%% deliberately crash if the handling an error response after
%% the client has been replied to

%% Common code for sending a single bucket or multiple inputs map/request
%% @private
send_mapred_req(Pid, MapRed, ClientPid, CallTimeout) ->
    ReqMsg = #rpbmapredreq{request = encode_mapred_req(MapRed),
                           content_type = <<"application/x-erlang-binary">>},
    ReqId = mk_reqid(),
    Timeout = proplists:get_value(timeout, MapRed, default_timeout(mapred_timeout)),
    Timeout1 = if
           is_integer(Timeout) ->
               %% Add an extra 100ms to the mapred timeout and use that
               %% for the socket timeout. This should give the
               %% map/reduce a chance to fail and let us know.
               Timeout + 100;
           true ->
               Timeout
           end,
    gen_server:call(Pid, {req, ReqMsg, Timeout1, {ReqId, ClientPid}}, CallTimeout).

%% @private
%% Make a new request that can be sent or queued
new_request(Msg, From, Timeout) ->
    Ref = make_ref(),
    #request{ref = Ref, msg = Msg, from = From, timeout = Timeout,
             tref = create_req_timer(Timeout, Ref)}.
new_request(Msg, From, Timeout, Context) ->
    Ref = make_ref(),
    #request{ref =Ref, msg = Msg, from = From, ctx = Context, timeout = Timeout,
             tref = create_req_timer(Timeout, Ref)}.

%% @private
%% Create a request timer if desired, otherwise return undefined.
create_req_timer(infinity, _Ref) ->
    undefined;
create_req_timer(undefined, _Ref) ->
    undefined;
create_req_timer(Msecs, Ref) ->
    erlang:send_after(Msecs, self(), {req_timeout, Ref}).

%% @private
%% Cancel a request timer made by create_timer/2
cancel_req_timer(undefined) ->
    ok;
cancel_req_timer(Tref) ->
    _ = erlang:cancel_timer(Tref),
    ok.

%% @private
%% Restart a request timer
-spec restart_req_timer(#request{}) -> #request{}.
restart_req_timer(Request) ->
    case Request#request.tref of
        undefined ->
            Request;
        Tref ->
            cancel_req_timer(Tref),
            NewTref = create_req_timer(Request#request.timeout,
                                       Request#request.ref),
            Request#request{tref = NewTref}
    end.

%% @private
%% Connect the socket if disconnected
e_connect(State) when State#state.sock =:= undefined ->
    #state{address = Address, port = Port, connects = Connects} = State,
    case gen_tcp:connect(Address, Port,
                         [binary, {active, once}, {packet, 4}, {header, 1}],
                         State#state.connect_timeout) of
        {ok, Sock} ->
            {ok, State#state{sock = Sock, connects = Connects+1,
                             reconnect_interval = ?FIRST_RECONNECT_INTERVAL}};
        Error ->
            Error
    end.

%% @private
%% Disconnect socket if connected
e_disconnect(State) ->
    %% Tell any pending requests we've disconnected
    _ = case State#state.active of
            undefined ->
                ok;
            Request ->
                send_caller({error, disconnected}, Request)
        end,

    %% Make sure the connection is really closed
    case State#state.sock of
        undefined ->
            ok;
        Sock ->
            gen_tcp:close(Sock)
    end,

    %% Decide whether to reconnect or exit
    NewState = State#state{sock = undefined, active = undefined},
    case State#state.auto_reconnect of
        true ->
            %% Schedule the reconnect message and return state
            erlang:send_after(State#state.reconnect_interval, self(), reconnect),
            {noreply, increase_reconnect_interval(NewState)};
        false ->
            {stop, disconnected, NewState}
    end.

%% Double the reconnect interval up to the maximum
increase_reconnect_interval(State) ->
    case State#state.reconnect_interval of
        Interval when Interval < ?MAX_RECONNECT_INTERVAL ->
            NewInterval = lists:min([Interval+Interval,?MAX_RECONNECT_INTERVAL]),
            State#state{reconnect_interval = NewInterval};
        _ ->
            State
    end.

%% Send a request to the server and prepare the state for the response
%% @private
send_request(Request, State) when State#state.active =:= undefined ->
    Pkt = riak_pb_codec:encode(Request#request.msg),
    ok = gen_tcp:send(State#state.sock, Pkt),
    maybe_reply(after_send(Request, State#state{active = Request})).

%% Queue up a request if one is pending
%% @private
queue_request(Request, State) ->
    State#state{queue = queue:in(Request, State#state.queue)}.

%% Try and dequeue request and send onto the server if one is waiting
%% @private
dequeue_request(State) ->
    case queue:out(State#state.queue) of
        {empty, _} ->
            State;
        {{value, Request}, Q2} ->
            send_request(Request, State#state{queue = Q2})
    end.

%% Remove a queued request by reference - returns same queue if ref not present
%% @private
remove_queued_request(Ref, State) ->
    L = queue:to_list(State#state.queue),
    case lists:keytake(Ref, #request.ref, L) of
        false -> % Ref not queued up
            State;
        {value, Req, L2} ->
            {reply, Reply, NewState} = on_timeout(Req, State),
            _ = send_caller(Reply, Req),
            NewState#state{queue = queue:from_list(L2)}
    end.

%% @private
mk_reqid() -> erlang:phash2(erlang:now()). % only has to be unique per-pid

%% @private
wait_for_listkeys(ReqId, Timeout) ->
    wait_for_listkeys(ReqId,Timeout,[]).
%% @private
wait_for_listkeys(ReqId,Timeout,Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId, {keys,Res}} -> wait_for_listkeys(ReqId,Timeout,[Res|Acc]);
        {ReqId, {error, Reason}} -> {error, Reason}
    after Timeout ->
            {error, {timeout, Acc}}
    end.


%% @private
wait_for_mapred(ReqId, Timeout) ->
    wait_for_mapred_first(ReqId, Timeout).

%% Wait for the first mapred result, so we know at least one phase
%% that will be delivering results.
wait_for_mapred_first(ReqId, Timeout) ->
    case receive_mapred(ReqId, Timeout) of
        done ->
            {ok, []};
        {mapred, Phase, Res} ->
            wait_for_mapred_one(ReqId, Timeout, Phase,
                                acc_mapred_one(Res, []));
        {error, _}=Error ->
            Error;
        timeout ->
            {error, {timeout, []}}
    end.

%% So far we have only received results from one phase.  This method
%% of accumulating a single phases's outputs will be more efficient
%% than the repeated orddict:append_list/3 used when accumulating
%% outputs from multiple phases.
wait_for_mapred_one(ReqId, Timeout, Phase, Acc) ->
    case receive_mapred(ReqId, Timeout) of
        done ->
            {ok, finish_mapred_one(Phase, Acc)};
        {mapred, Phase, Res} ->
            %% still receiving for just one phase
            wait_for_mapred_one(ReqId, Timeout, Phase,
                                acc_mapred_one(Res, Acc));
        {mapred, NewPhase, Res} ->
            %% results from a new phase have arrived - track them all
            Dict = [{NewPhase, Res},{Phase, Acc}],
            wait_for_mapred_many(ReqId, Timeout, Dict);
        {error, _}=Error ->
            Error;
        timeout ->
            {error, {timeout, finish_mapred_one(Phase, Acc)}}
    end.

%% Single-phase outputs are kept as a reverse list of results.
acc_mapred_one([R|Rest], Acc) ->
    acc_mapred_one(Rest, [R|Acc]);
acc_mapred_one([], Acc) ->
    Acc.

finish_mapred_one(Phase, Acc) ->
    [{Phase, lists:reverse(Acc)}].

%% Tracking outputs from multiple phases.
wait_for_mapred_many(ReqId, Timeout, Acc) ->
    case receive_mapred(ReqId, Timeout) of
        done ->
            {ok, finish_mapred_many(Acc)};
        {mapred, Phase, Res} ->
            wait_for_mapred_many(
              ReqId, Timeout, acc_mapred_many(Phase, Res, Acc));
        {error, _}=Error ->
            Error;
        timeout ->
            {error, {timeout, finish_mapred_many(Acc)}}
    end.

%% Many-phase outputs are kepts as a proplist of reversed lists of
%% results.
acc_mapred_many(Phase, Res, Acc) ->
    case lists:keytake(Phase, 1, Acc) of
        {value, {Phase, PAcc}, RAcc} ->
            [{Phase,acc_mapred_one(Res,PAcc)}|RAcc];
        false ->
            [{Phase,acc_mapred_one(Res,[])}|Acc]
    end.

finish_mapred_many(Acc) ->
    [ {P, lists:reverse(A)} || {P, A} <- lists:keysort(1, Acc) ].

%% Receive one mapred message.
-spec receive_mapred(reference(), timeout()) ->
         done | {mapred, integer(), [term()]} | {error, term()} | timeout.
receive_mapred(ReqId, Timeout) ->
    receive {ReqId, Msg} ->
            %% Msg should be `done', `{mapred, Phase, Results}', or
            %% `{error, Reason}'
            Msg
    after Timeout ->
            timeout
    end.


%% Encode the MapReduce request using term to binary
%% @private
-spec encode_mapred_req(term()) -> binary().
encode_mapred_req(Req) ->
    term_to_binary(Req).

%% Decode a partial phase response
%% @private
-spec decode_mapred_resp(binary(), binary()) -> term().
decode_mapred_resp(Data, <<"application/x-erlang-binary">>) ->
    try
        binary_to_term(Data)
    catch
        _:Error -> % On error, merge in with the other results
            [{error, Error}]
    end.
