-module(erliak).
-behaviour(gen_server).

-export([start_link/0, start_link/1, start_link/2, start_link/3,
	 start/2, start/3,
	 stop/1,
	 ping/0, ping/1
	]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Erliak include file, holds defines, types and records
-include("erliak.hrl").

%% Eunit testing

-define(TEST, 1).
-ifdef(TEST).
-include("../test/erliak_tests.hrl").
-endif.

%% ====================================================================
%% Internal types
%% ====================================================================
-type address() :: string() | atom() | inet:ip_address(). %% The TCP/IP host name or address of the Riak node
-type portnum() :: non_neg_integer(). %% The TCP port number of the Riak node's Protocol Buffers interface

-type client_option() :: {transport, transport()}. %% allowed client options
-type client_options() :: [client_option()]. %% list of client options

%% -type bucket() :: binary(). %% A bucket name
%% -type key() :: binary(). %% A key name

%% ====================================================================
%% Exports
%% ====================================================================

%% @doc Create a linked process to talk with the riak server on the default 
%%      address and port using the default transport protocol
%%      Client id will be assigned by the server.
%% @equiv start_link("127.0.0.1", 8087, [])
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    case ?DEFAULT_PROTOCOL of
	pb ->
	    start_link(?PB_DEFAULT_ADDRESS, ?PB_DEFAULT_PORT, [{transport, pb}]);
	http ->
	    start_link(?HTTP_DEFAULT_ADDRESS, ?HTTP_DEFAULT_PORT, [{transport, http}])
    end.

%% @doc Create a linked process to talk with the riak server on the default 
%%      address and port using the transport protocol specified
%%      Client id will be assigned by the server.
-spec start_link(client_options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    case proplists:get_value(transport, Options) of
	pb ->
	    start_link(?PB_DEFAULT_ADDRESS, ?PB_DEFAULT_PORT, [{transport, pb}]);
	http ->
	    start_link(?HTTP_DEFAULT_ADDRESS, ?HTTP_DEFAULT_PORT, [{transport, http}])
    end.


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

%% @doc Create a process to talk with the riak server on Address:Port
-spec start(address(), portnum()) -> {ok, pid()} | {error, term()}.
start(Address, Port) ->
    start(Address, Port, []).

%% @doc Create a process to talk with the riak server on Address:Port
-spec start(address(), portnum(), client_options()) -> {ok, pid()} | {error, term()}.
start(Address, Port, Options) ->
    gen_server:start({local, ?MODULE}, ?MODULE, [Address, Port, Options], []).

%% @doc Disconnect the socket and stop the process.
-spec stop(pid()) -> ok.
stop(Client) ->
    gen_server:call(Client, stop).

%% ====================================================================
%% client API
%% ====================================================================

%% @doc Ping the server
%% @equiv ping(default_timeout(ping_timeout))
-spec ping() -> ok | {error, term()}.
ping() ->
    ping(riakc_pb_socket:default_timeout(ping_timeout)).

%% @doc Ping the server specifying timeout
-spec ping(timeout()) -> ok | {error, term()}.
ping(Timeout) ->
    gen_server:call(?MODULE, {client, ping, Timeout}).    

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

%% @private
init([Address, Port, Options]) ->
    %% TODO REFACTOR?
    {ok, State} = erliak_transport:connect(Address, Port, Options),
    io:format("State = ~p~n", [State]),
    {ok, State}.


%% 
handle_call({client, ping, Timeout}, _From, State) ->
    Reply = erliak_transport:ping(State, Timeout),
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




