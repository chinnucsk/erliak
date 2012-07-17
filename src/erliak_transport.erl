-module(erliak_transport).

%% Public exports
-export([connect/3,
     % ping/2,
     % get/5,
     % put/4,
     % delete/5,
     % disconnect/1,
     % get_server_info/2,
     handle/3]).

%% Behaviour export
-export([behaviour_info/1]).

-include("erliak.hrl").

behaviour_info(callbacks) ->
    [{connect,3},
     {ping,2},
     {get,5},
     {put,4},
     {delete,5},
     {disconnect,1},
     {get_server_info,2},
     {get_client_id,2},
     {list_buckets,3},
     {list_keys,3}
    ];

behaviour_info(_Other) ->
    undefined.


%% @doc returns the corresponding erliang transport module for Transport
%% TODO provide a notice to the user that we're falling back to ?DEFAULT_TRANSPORT
-spec get_transport_module(atom()) -> atom().
get_transport_module(Transport) ->
    case Transport of
        pb ->
            erliak_pb;
        http ->
            erliak_http;
        undefined ->	    
            io:format("*** No transport protocol given.~n"),
            DefTransport = erliak_env:get_env(default_transport, ?DEFAULT_TRANSPORT),
            io:format("*** Falling back to default transport (~p).~n", [DefTransport]),            
            list_to_existing_atom("erliak_" ++ atom_to_list(DefTransport));
        Other ->
            io:format("*** Invalid transport protocol given (~p).~n", [Other]),
            DefTransport = erliak_env:get_env(default_transport, ?DEFAULT_TRANSPORT),
            io:format("*** Falling back to default transport (~p).~n", [DefTransport]),
            list_to_existing_atom("erliak_" ++ atom_to_list(DefTransport))
    end.

%% API functions
connect(Address, Port, Options) ->
    %% Extract (and remove) the transport from the options
    Transport = proplists:get_value(transport, Options),
    Opts = proplists:delete(transport, Options),
    TModule = get_transport_module(Transport),
    %% Perform connection based on the transport given
    {ok, Connection} = TModule:connect(Address, Port, Opts),
    %% Store this connection in state
    State = #connection{
        connection = Connection,
        transport_module = TModule
    },
    {ok, State}.

%% "Forwards" Function(Arguments) to the transport module set in State
handle(State, Function, Arguments) ->    
    TModule = State#connection.transport_module,
    Conn = State#connection.connection,
    CallArgs = [Conn|Arguments],
    erlang:apply(TModule, Function, CallArgs).

% Refactoring

% ping(State, Timeout) ->
%     TModule = State#connection.transport_module,
%     Conn = State#connection.connection,
%     TModule:ping(Conn, Timeout).

% get(State, Bucket, Key, Options, Timeout) ->
%     TModule = State#connection.transport_module,
%     Conn = State#connection.connection,
%     TModule:get(Conn, Bucket, Key, Options, Timeout).

% put(State, Object, Options, Timeout) ->
%     TModule = State#connection.transport_module,
%     Conn = State#connection.connection,
%     TModule:put(Conn, Object, Options, Timeout).

% delete(State, Bucket, Key, Options, Timeout) ->
%     TModule = State#connection.transport_module,
%     Conn = State#connection.connection,
%     TModule:delete(Conn, Bucket, Key, Options, Timeout).

% disconnect(State) ->
%     TModule = State#connection.transport_module,
%     Conn = State#connection.connection,
%     TModule:disconnect(Conn).

% get_server_info(State, Timeout) ->
%     TModule = State#connection.transport_module,
%     Conn = State#connection.connection,
%     TModule:get_server_info(Conn, Timeout).