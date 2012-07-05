-module(erliak).
-behaviour(gen_server).

-export([start_link/0, start_link/1, start_link/2, start_link/3,
         start/2, start/3,
         stop/1,
         ping/0, ping/1,
         get/2, get/3,
         put/1, put/2, put/3]).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
      
%% Eunit testing
-ifdef(TEST).
-include("../test/erliak_tests.hrl").
-endif.

%% ====================================================================
%% Internal types
%% ====================================================================
-type address() :: string() | atom() | inet:ip_address(). %% The TCP/IP host name or address of the Riak node
-type portnum() :: non_neg_integer(). %% The TCP port number of the Riak node's HTTP/PB interface

-type client_option() :: {transport, atom()}. %% Allowed client options
-type client_options() :: [client_option()]. %% List of client options

-type bucket() :: binary(). %% A bucket name
-type key() :: binary(). %% A key name
-type riakc_obj() :: riakc_obj:riakc_obj(). %% An object (bucket, key, metadata, value) stored in Riak.
-type proplist() :: [tuple()]. %% Type for options

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
%% @equiv ping(default_timeout(ping_timeout))
%% @TODO use an internal function for timeouts
-spec ping() -> ok | {error, term()}.
ping() ->
    ping(default_timeout).

%% @doc Ping the server specifying timeout
-spec ping(timeout()) -> ok | {error, term()}.
ping(Timeout) ->
    gen_server:call(?MODULE, {client, ping, Timeout}).


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
    gen_server:call(?MODULE, {client, get, Bucket, Key, Options, Timeout}).


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
    gen_server:call(?MODULE, {client, put, Object, Options, Timeout}).

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
handle_call({client, ping, Timeout}, _From, State) ->
    Reply = erliak_transport:ping(State, Timeout),
    {reply, Reply, State};

handle_call({client, get, Bucket, Key, Options, Timeout}, _From, State) ->
    Reply = erliak_transport:get(State, Bucket, Key, Options, Timeout),
    {reply, Reply, State};

handle_call({client, put, Object, Options, Timeout}, _From, State) ->
    Reply = erliak_transport:put(State, Object, Options, Timeout),
    {reply, Reply, State};

handle_call(stop, _From, State) ->
    erliak_transport:disconnect(State),
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    io:format("DEFAULT HANDLE_CALL ~n"),
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




