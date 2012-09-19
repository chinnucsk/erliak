-module(erliak_transport).

%% Public exports
-export([connect/4, handle/3]).

%% Behaviour export
-export([behaviour_info/1]).

-include("erliak.hrl").
%% Behaviour functions
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
     {list_keys,3},
     {stream_list_keys,4},
     {get_bucket,4},
     {set_bucket,5},
     {mapred,5},
     {mapred_stream,6},
     {mapred_bucket,5},
     {mapred_bucket_stream,6},
     {search,3},
     {search,6}
    ];

behaviour_info(_Other) ->
    undefined.


%% @doc returns the corresponding erliak transport module for Transport or
%%      ?DEFAULT_TRANSPORT if no Transport is given
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

%% @doc Connects to the Riak server on Address:Port with Options using
%%      the transport protocol specified in Options
-spec connect(address(), portnum(), client_options(), pid()) -> {ok, connection_ref()}.
connect(Address, Port, Options, Caller) ->
    %% Extract (and remove) the transport from the options
    Transport = proplists:get_value(transport, Options),
    Opts = proplists:delete(transport, Options),
    TModule = get_transport_module(Transport),
    %% Perform connection based on the transport given
    {ok, Connection} = TModule:connect(Address, Port, Opts),
    % io:format("CONNECT self() ~p~n", [self()]),
    %% Store this connection in state
    State = #connection{
        connection = Connection,
        transport_module = TModule,
        caller = Caller
    },
    {ok, State}.

%% "Forwards" Function(Arguments) to the transport module set in State
handle(State, Function, Arguments) ->    
    TModule = State#connection.transport_module,
    % Conn = State#connection.connection,
    CallArgs = [State|Arguments],
    erlang:apply(TModule, Function, CallArgs).
