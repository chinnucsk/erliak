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
         list_keys/1, list_keys/2,
         stream_list_keys/1, stream_list_keys/2, stream_list_keys/3,
         get_bucket/1, get_bucket/2, get_bucket/3,
         set_bucket/2, set_bucket/3, set_bucket/4,
         mapred/2, mapred/3, mapred/4,
         mapred_stream/3, mapred_stream/4, mapred_stream/5,
         mapred_bucket/2, mapred_bucket/3, mapred_bucket/4,
         mapred_bucket_stream/4, mapred_bucket_stream/5,
         search/2, search/4, search/5
         ]).

%% debug
-export([do/1]).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
      
%% ====================================================================
%% Internal types
%% ==================================================================== 

-include("erliak.hrl").

do(A) ->
    {ok, C} = erliak:start_link([{transport, http}]),
    
    Res = erliak:mapred(undefined,
                                [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                 {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]),

    % M = erliak:mapred([{<<"bucket">>, <<"foo">>},
    %                                        {<<"bucket">>, <<"bar">>},
    %                                        {<<"bucket">>, <<"baz">>}],
    %                                       [{map, {modfun, riak_kv_mapreduce, map_object_value}, <<"include_notfound">>, false},
    %                                        {reduce, {modfun, riak_kv_mapreduce, reduce_set_union}, undefined, true}]),
    % {ok, _Ref} = erliak:stream_list_keys(A),
    % % MB = erlang:process_info(self(),[message_queue_len,messages]),
    % % io:format("MB ~p~n", [MB]),
    % % io:format("My self() ~p~n", [self()]),
    % receive
    %     A ->
    %         io:format("*** ~p~n", [A])
    % after
    %     10000 ->
    %         io:format("*** timeout~n")
    % end,
    erliak:stop(C),
    Res.
    

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
    Caller = self(),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Address, Port, Options, Caller], []).


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

%% @doc Stream list of keys in the bucket to the calling process.  The
%%      process receives these messages.
%% ```    {ReqId::req_id(), {keys, [key()]}}
%%        {ReqId::req_id(), done}'''
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv stream_list_keys(Bucket, default_timeout)
-spec stream_list_keys(bucket()) -> {ok, req_id()} | {ok, reference()} | {error, term()}.
stream_list_keys(Bucket) ->
    stream_list_keys(Bucket, default_timeout).

%% @doc Stream list of keys in the bucket to the calling process specifying server side
%%      timeout.
%%      The process receives these messages.
%% ```    {ReqId::req_id(), {keys, [key()]}}
%%        {ReqId::req_id(), done}'''
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv stream_list_keys(Bucket, Timeout, default_call_timeout)
-spec stream_list_keys(bucket(), timeout()) -> {ok, req_id()} | {ok, reference()} | {error, term()}.
stream_list_keys(Bucket, Timeout) ->
    stream_list_keys(Bucket, Timeout, default_call_timeout).

%% @doc Stream list of keys in the bucket to the calling process specifying server side
%%      timeout and local call timeout.
%%      The process receives these messages.
%% ```    {ReqId::req_id(), {keys, [key()]}}
%%        {ReqId::req_id(), done}'''
%% <em>This is a potentially expensive operation and should not be used in production.</em>
-spec stream_list_keys(bucket(), timeout(), timeout()) -> {ok, req_id()} |
                                                                 {ok, reference()} |
                                                                 {error, term()}.
stream_list_keys(Bucket, Timeout, CallTimeout) ->
    gen_server:call(?MODULE, {client, stream_list_keys, [Bucket, Timeout, CallTimeout]}, infinity).

%% @doc Get bucket properties.
%% @equiv get_bucket(Bucket, default_timeout)
-spec get_bucket(bucket()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(Bucket) ->
    get_bucket(Bucket, default_timeout).

%% @doc Get bucket properties specifying a server side timeout.
%% @equiv get_bucket(Bucket, Timeout, default_call_timeout)
-spec get_bucket(bucket(), timeout()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(Bucket, Timeout) ->
    get_bucket(Bucket, Timeout, default_call_timeout).

%% @doc Get bucket properties specifying a server side and local call timeout.
-spec get_bucket(bucket(), timeout(), timeout()) -> {ok, bucket_props()} |
                                                           {error, term()}.
get_bucket(Bucket, Timeout, CallTimeout) ->
    gen_server:call(?MODULE, {client, get_bucket, [Bucket, Timeout, CallTimeout]}, infinity).

%% @doc Set bucket properties.
%% @equiv set_bucket(Bucket, BucketProps, default_timeout)
-spec set_bucket(bucket(), bucket_props()) -> ok | {error, term()}.
set_bucket(Bucket, BucketProps) ->
    set_bucket(Bucket, BucketProps, default_timeout).

%% @doc Set bucket properties specifying a server side timeout.
%% @equiv set_bucket(Bucket, BucketProps, Timeout, default_call_timeout)
-spec set_bucket(bucket(), bucket_props(), timeout()) -> ok | {error, term()}.
set_bucket(Bucket, BucketProps, Timeout) ->
    set_bucket(Bucket, BucketProps, Timeout, default_call_timeout).

%% @doc Set bucket properties specifying a server side and local call timeout.
-spec set_bucket(bucket(), bucket_props(), timeout(), timeout()) -> ok | {error, term()}.
set_bucket(Bucket, BucketProps, Timeout, CallTimeout) ->
    gen_server:call(?MODULE, {client, set_bucket, [Bucket, BucketProps, Timeout, CallTimeout]}, infinity).
    
%% @doc Perform a MapReduce job across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% @equiv mapred(Inputs, Query, default_timeout)
-spec mapred(mapred_inputs(), [mapred_queryterm()]) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Inputs, Query) ->
    mapred(Inputs, Query, default_timeout).

%% @doc Perform a MapReduce job across the cluster with a job timeout.
%%      See the MapReduce documentation for explanation of behavior.
%% @equiv mapred(Inputs, Query, Timeout, default_call_timeout)
-spec mapred(mapred_inputs(), [mapred_queryterm()], timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Inputs, Query, Timeout) ->
    mapred(Inputs, Query, Timeout, default_call_timeout).

%% @doc Perform a MapReduce job across the cluster with a job and
%%      local call timeout.  See the MapReduce documentation for
%%      explanation of behavior. This is implemented by using
%%      <code>mapred_stream/6</code> and then waiting for all results.
%% @see mapred_stream/6
-spec mapred(mapred_inputs(), [mapred_queryterm()], timeout(), timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Inputs, Query, Timeout, CallTimeout) ->
    gen_server:call(?MODULE, {client, mapred, [Inputs, Query, Timeout, CallTimeout]}, infinity).

%% @doc Perform a streaming MapReduce job across the cluster sending results
%%      to ClientPid.
%%      See the MapReduce documentation for explanation of behavior.
%%      The ClientPid will receive messages in this format:
%%      {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @equiv mapred_stream(Inputs, Query, ClientPid, default_timeout)
-spec mapred_stream(Inputs::mapred_inputs(),Query::[mapred_queryterm()], ClientPid::pid()) ->
                           {ok, req_id()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_stream(Inputs, Query, ClientPid) ->
    mapred_stream(Inputs, Query, ClientPid, default_timeout).

%% @doc Perform a streaming MapReduce job with a timeout across the cluster.
%%      sending results to ClientPid.
%%      See the MapReduce documentation for explanation of behavior.
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @equiv mapred_stream(Inputs, Query, ClientPid, Timeout, default_call_timeout)
-spec mapred_stream(Inputs::mapred_inputs(),Query::[mapred_queryterm()], ClientPid::pid(), Timeout::timeout()) ->
                           {ok, req_id()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_stream(Inputs, Query, ClientPid, Timeout) ->
    mapred_stream(Inputs, Query, ClientPid, Timeout, default_call_timeout).

%% @doc Perform a streaming MapReduce job with a map/red timeout across the cluster,
%%      a local call timeout and sending results to ClientPid.
%%      See the MapReduce documentation for explanation of behavior.
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
-spec mapred_stream(Inputs::mapred_inputs(),
                    Query::[mapred_queryterm()], ClientPid::pid(),
                    Timeout::timeout(), CallTimeout::timeout()) ->
                           {ok, req_id()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_stream(Inputs, Query, ClientPid, Timeout, CallTimeout) ->
    gen_server:call(?MODULE, mapred_stream, [Inputs, Query, ClientPid, Timeout, CallTimeout], infinity).


%% @doc Perform a MapReduce job against a bucket across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%% @equiv mapred_bucket(Bucket, Query, default_timeout)
-spec mapred_bucket(Bucket::bucket(), Query::[mapred_queryterm()]) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_bucket(Bucket, Query) ->
    mapred_bucket(Bucket, Query, default_timeout).

%% @doc Perform a MapReduce job against a bucket with a timeout
%%      across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%% @equiv mapred_bucket(Bucket, Query, Timeout, default_timeout(mapred_bucket_call_timeout))
-spec mapred_bucket(Bucket::bucket(), Query::[mapred_queryterm()], Timeout::timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_bucket(Bucket, Query, Timeout) ->
    mapred_bucket(Bucket, Query, Timeout, default_call_timeout).

%% @doc Perform a MapReduce job against a bucket with a timeout
%%      across the cluster and local call timeout.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
-spec mapred_bucket(Bucket::bucket(), Query::[mapred_queryterm()],
                    Timeout::timeout(), CallTimeout::timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_bucket(Bucket, Query, Timeout, CallTimeout) ->
    gen_server:call(?MODULE, {client, mapred_bucket, [Bucket, Query, Timeout, CallTimeout]}, infinity).

%% @doc Perform a streaming MapReduce job against a bucket with a timeout
%%      across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @equiv     mapred_bucket_stream(Bucket, Query, ClientPid, Timeout, default_call_timeout)
-spec mapred_bucket_stream(bucket(), [mapred_queryterm()], ClientPid::pid(), timeout()) ->
                                  {ok, req_id()} |
                                  {error, term()}.
mapred_bucket_stream(Bucket, Query, ClientPid, Timeout) ->
    mapred_bucket_stream(Bucket, Query, ClientPid, Timeout, default_call_timeout).

%% @doc Perform a streaming MapReduce job against a bucket with a server timeout
%%      across the cluster and a call timeout.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
-spec mapred_bucket_stream(bucket(), [mapred_queryterm()], ClientPid::pid(), timeout(), timeout()) ->
                                  {ok, req_id()} | {error, term()}.
mapred_bucket_stream(Bucket, Query, ClientPid, Timeout, CallTimeout) ->
    gen_server:call(?MODULE, {client, mapred_bucket_stream, [Bucket, Query, ClientPid, Timeout, CallTimeout]}, infinity).

%% @doc Execute a search query. This command will return an error
%%      unless executed against a Riak Search cluster.  Because
%%      Protocol Buffers has no native Search interface, this uses the
%%      search inputs to MapReduce.
-spec search(bucket(), string()) ->  {ok, mapred_result()} | {error, term()}.
search(Bucket, SearchQuery) ->
    gen_server:call(?MODULE, {client, search, [Bucket, SearchQuery]}, infinity).

%% @doc Execute a search query and feed the results into a MapReduce
%%      query.  This command will return an error
%%      unless executed against a Riak Search cluster.
%% @equiv search(Bucket, SearchQuery, MRQuery, Timeout, default_call_timeout)
-spec search(bucket(), string(), [mapred_queryterm()], timeout()) ->
                    {ok, mapred_result()} | {error, term()}.
search(Bucket, SearchQuery, MRQuery, Timeout) ->
    search(Bucket, SearchQuery, MRQuery, Timeout, default_call_timeout).


%% @doc Execute a search query and feed the results into a MapReduce
%%      query with a timeout on the call. This command will return
%%      an error unless executed against a Riak Search cluster.
-spec search(bucket(), string(), [mapred_queryterm()], timeout(), timeout()) ->
                    {ok, mapred_result()} | {error, term()}.
search(Bucket, SearchQuery, MRQuery, Timeout, CallTimeout) ->
    gen_server:call(?MODULE, {client, search, [Bucket, SearchQuery, MRQuery, Timeout, CallTimeout]}, infinity).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

%% @private
init([Address, Port, Options, Caller]) ->    
    erliak_transport:connect(Address, Port, Options, Caller).
    
%% Handling client API callbacks
handle_call({client, Function, Arguments}, From, State) ->    
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

%% Handles messages sent to the gen_server
handle_info(Info, State) ->        
    Caller = State#connection.caller,
    io:format("*** Forwarding ~p to ~p~n", [Info, Caller]),
    Caller ! Info,
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




