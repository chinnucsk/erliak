-module(erliak).
-behaviour(gen_server).

-export([start_link/0, start_link/1, start_link/2, start_link/3,
         start/2, start/3,
         stop/1,
         ping/0, ping/1,
         get/2, get/3, get/4,
         put/1, put/2, put/3,
	     delete/2, delete/3, delete/4,
         get_server_info/0, get_server_info/1,
         get_client_id/0, get_client_id/1,
         list_buckets/0, list_buckets/1, list_buckets/2,
         list_keys/1, list_keys/2
         % stream_list_keys/1, stream_list_keys/2, stream_list_keys/3,
         % get_bucket/1, get_bucket/2, get_bucket/3,
         % set_bucket/2, set_bucket/3, set_bucket/4,
         % mapred/2, mapred/3, mapred/4,
         % mapred_stream/3, mapred_stream/4, mapred_stream/5,
         % mapred_bucket/2, mapred_bucket/3, mapred_bucket/4,
         % mapred_bucket_stream/4, mapred_bucket_stream/5,
         % search/2, search/4, search/5
         ]).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
      
%% ====================================================================
%% Internal types
%% ==================================================================== 

-include("erliak.hrl").

%% ====================================================================
%% Exports
%% ====================================================================

%% @doc Create a linked process to talk with the riak server on the default
%%      address and port using the default transport protocol
%%      Client id will be assigned by the server.
%% @equiv start_link(default_address, default_port, [])
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(default_address, default_port, []).

%% @doc Create a linked process to talk with the riak server on the default
%%      address and port using the transport protocol specified
%%      Client id will be assigned by the server.
%% @equiv start_link(default_address, default_port, Options)
-spec start_link(client_options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    start_link(default_address, default_port, Options).

%% @doc Create a linked process to talk with the riak server on Address:Port
%%      Client id will be assigned by the server.
%% @equiv start_link(Address, Port, [])
-spec start_link(address(), portnum()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    start_link(Address, Port, []).

%% @doc Create a linked process to talk with the riak server on Address:Port
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum(), client_options()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Address, Port, Options], []).


%% @TODO similar behaviour for start as for start_link
%% @doc Create a process to talk with the riak server on Address:Port
-spec start(address(), portnum()) -> {ok, pid()} | {error, term()}.
start(Address, Port) ->
    start(Address, Port, []).

%% @doc Create a process to talk with the riak server on Address:Port
-spec start(address(), portnum(), client_options()) -> {ok, pid()} | {error, term()}.
start(Address, Port, Options) ->
    gen_server:start({local, ?MODULE}, ?MODULE, [Address, Port, Options], []).

%% @TODO implement this
%% doc Disconnect all connections and stop the process.
%%

%% @doc Disconnect the socket and stop the process.
-spec stop(pid()) -> ok.
stop(Client) ->
    gen_server:call(Client, stop).

%% ====================================================================
%% client API
%% ====================================================================

%% @doc Ping the server
%% @equiv ping(default_timeout)
-spec ping() -> ok | {error, term()}.
ping() ->
    ping(default_timeout).

%% @doc Ping the server specifying timeout
-spec ping(timeout()) -> ok | {error, term()}.
ping(Timeout) ->
    gen_server:call(?MODULE, {client, ping, [Timeout]}, infinity).


%% @doc Get bucket/key from the server.
%%      Will return {error, notfound} if the key is not on the serverk.
%% @equiv get(Bucket, Key, [], default_timeout)
-spec get(bucket(), key()) -> {ok, riakc_obj()} | {error, term()}.
get(Bucket, Key) ->
    get(Bucket, Key, [], default_timeout).

%% @doc Get bucket/key from the server specifying timeout.
%%      Will return {error, notfound} if the key is not on the server.
%% @equiv get(Pid, Bucket, Key, Options, Timeout)
-spec get(bucket(), key(), TimeoutOrOptions::timeout() | proplist()) ->
                 {ok, riakc_obj()} | {error, term() | unchanged}.
get(Bucket, Key, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    get(Bucket, Key, [], Timeout);
get(Bucket, Key, Options) ->
    get(Bucket, Key, Options, default_timeout).

%% @doc Get bucket/key from the server supplying options and timeout.
%%      <code>{error, unchanged}</code> will be returned when the
%%      <code>{if_modified, Vclock}</code> option is specified and the
%%      object is unchanged.
get(Bucket, Key, Options, Timeout) ->
    gen_server:call(?MODULE, {client, get, [Bucket, Key, Options, Timeout]}, infinity).


%% @doc Put the metadata/value in the object under bucket/key
%% @equiv put(Obj, [])
%% @see put/3
-spec put(riakc_obj()) ->
                 ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(Object) ->
    %% -compile({no_auto_import,[put/2]}).
    ?MODULE:put(Object, []).

%% @doc Put the metadata/value in the object under bucket/key with options or timeout.
%% @equiv put(Obj, Options, Timeout)
%% @see put/3
put(Object, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    put(Object, [], Timeout);
put(Object, Options) ->
    put(Object, Options, default_timeout).

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
-spec put(riakc_obj(), proplist(), timeout()) ->
                 ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(Object, Options, Timeout) ->
    gen_server:call(?MODULE, {client, put, [Object, Options, Timeout]}, infinity).

%% @doc Delete the key/value
%% @equiv delete(Bucket, Key, [])
-spec delete(bucket(), key()) -> ok | {error, term()}.
delete(Bucket, Key) ->
    delete(Bucket, Key, []).

%% @doc Delete the key/value specifying timeout or options. <em>Note that the rw quorum is deprecated, use r and w.</em>
%% @equiv delete(Bucket, Key, Options, Timeout)
-spec delete(bucket(), key(), TimeoutOrOptions::timeout() | proplist()) ->
                    ok | {error, term()}.
delete(Bucket, Key, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    delete(Bucket, Key, [], Timeout);
delete(Bucket, Key, Options) ->
    delete(Bucket, Key, Options, default_timeout).

%% @doc Delete the key/value with options and timeout. <em>Note that the rw quorum is deprecated, use r and w.</em>
-spec delete(bucket(), key(), proplist(), timeout()) -> ok | {error, term()}.
delete(Bucket, Key, Options, Timeout) ->
    gen_server:call(?MODULE, {client, delete, [Bucket, Key, Options, Timeout]}, infinity).

%% @doc Get the server information for this connection
%% @equiv get_server_info(default_timeout)
-spec get_server_info() -> {ok, proplist()} | {error, term()}.
get_server_info() ->
    get_server_info(default_timeout).

%% @doc Get the server information for this connection specifying timeout
-spec get_server_info(timeout()) -> {ok, proplist()} | {error, term()}.
get_server_info(Timeout) ->
    gen_server:call(?MODULE, {client, get_server_info, [Timeout]}, infinity).

%% @doc Get the client id for this connection
%% @equiv get_client_id(default_timeout)
-spec get_client_id() -> {ok, client_id()} | {error, term()}.
get_client_id() ->
    get_client_id(default_timeout).

%% @doc Get the client id for this connection specifying timeout
-spec get_client_id(timeout()) -> {ok, client_id()} | {error, term()}.
get_client_id(Timeout) ->
    gen_server:call(?MODULE, {client, get_client_id, [Timeout]}, infinity).

%          stream_list_keys/1, stream_list_keys/2, stream_list_keys/3,
%          get_bucket/1, get_bucket/2, get_bucket/3,
%          set_bucket/2, set_bucket/3, set_bucket/4,
%          mapred/2, mapred/3, mapred/4,
%          mapred_stream/3, mapred_stream/4, mapred_stream/5,
%          mapred_bucket/2, mapred_bucket/3, mapred_bucket/4,
%          mapred_bucket_stream/4, mapred_bucket_stream/5,
%          search/2, search/4, search/5

%% @doc List all buckets on the server.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv list_buckets(default_timeout)
-spec list_buckets() -> {ok, [bucket()]} | {error, term()}.
list_buckets() ->
    list_buckets(default_timeout).

%% @doc List all buckets on the server specifying server-side timeout.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv list_buckets(Timeout, default_call_timeout)
-spec list_buckets(timeout()) -> {ok, [bucket()]} | {error, term()}.
list_buckets(Timeout) ->
    list_buckets(Timeout, default_call_timeout).

%% @doc List all buckets on the server specifying server-side and local
%%      call timeout.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
-spec list_buckets(timeout(), timeout()) -> {ok, [bucket()]} |
                                                   {error, term()}.
list_buckets(Timeout, CallTimeout) ->
    gen_server:call(?MODULE, {client, list_buckets, [Timeout, CallTimeout]}, infinity).

%% @doc List all keys in a bucket
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv list_keys(Bucket, default_timeout)
-spec list_keys(bucket()) -> {ok, [key()]} | {error, term()}.
list_keys(Bucket) ->
    list_keys(Bucket, default_timeout).

%% @doc List all keys in a bucket specifying timeout. This is
%% implemented using {@link stream_list_keys/3} and then waiting for
%% the results to complete streaming.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
-spec list_keys(bucket(), timeout()) -> {ok, [key()]} | {error, term()}.
list_keys(Bucket, Timeout) ->
    gen_server:call(?MODULE, {client, list_keys, [Bucket, Timeout]}, infinity).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

%% @private
init([Address, Port, Options]) ->
    %% TODO REFACTOR?
    {ok, State} = erliak_transport:connect(Address, Port, Options),
    io:format("State = ~p~n", [State]),
    {ok, State}.

%% Handling client API callbacks
handle_call({client, Function, Arguments}, _From, State) ->
    Reply = erliak_transport:handle(State, Function, Arguments),
    {reply, Reply, State};

% handle_call({client, ping, Timeout}, _From, State) ->
%     Reply = erliak_transport:ping(State, Timeout),
%     {reply, Reply, State};

% handle_call({client, get, Bucket, Key, Options, Timeout}, _From, State) ->
%     Reply = erliak_transport:get(State, Bucket, Key, Options, Timeout),
%     {reply, Reply, State};

% handle_call({client, put, Object, Options, Timeout}, _From, State) ->
%     Reply = erliak_transport:put(State, Object, Options, Timeout),
%     {reply, Reply, State};

% handle_call({client, delete, Bucket, Key, Options, Timeout}, _From, State) ->
%     Reply = erliak_transport:delete(State, Bucket, Key, Options, Timeout),
%     {reply, Reply, State};

% handle_call({client, get_server_info, Timeout}, _From, State) ->
%     Reply = erliak_transport:get_server_info(State, Timeout),
%     {reply, Reply, State};

handle_call(stop, _From, State) ->
    erliak_transport:handle(State, disconnect, []),
    {stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




