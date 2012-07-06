-module(erliak_transport).

%% Public exports
-export([connect/3,
     ping/2,
     get/5,
     put/4,
     delete/5,
     disconnect/1]).

%% Behaviour export
-export([behaviour_info/1]).

-include("erliak.hrl").

behaviour_info(callbacks) ->
    [{connect,3},
     {ping,2},
     {get,5},
     {put,4},
     {delete,5},
     {disconnect,1}
    ];

behaviour_info(_Other) ->
    undefined.


%% @doc returns the corresponding erliang transport module for Transport
%% TODO a more generalized solution? check for existing modules matching erliak_Transport?
-spec get_transport_module(atom()) -> atom().
get_transport_module(Transport) ->
    case Transport of
        pb ->
            erliak_pb;
        http ->
            erliak_http;
        undefined ->	    
	    case application:get_env(erliak, default_transport) of
		{ok, DefaultTransport} -> 
		    list_to_existing_atom("erliak_" ++ atom_to_list(DefaultTransport));
		undefined ->
		    list_to_existing_atom("erliak_" ++ atom_to_list(?DEFAULT_TRANSPORT))
	    end            
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

ping(State, Timeout) ->
    TModule = State#connection.transport_module,
    Conn = State#connection.connection,
    TModule:ping(Conn, Timeout).

get(State, Bucket, Key, Options, Timeout) ->
    TModule = State#connection.transport_module,
    Conn = State#connection.connection,
    TModule:get(Conn, Bucket, Key, Options, Timeout).

put(State, Object, Options, Timeout) ->
    TModule = State#connection.transport_module,
    Conn = State#connection.connection,
    TModule:put(Conn, Object, Options, Timeout).

delete(State, Bucket, Key, Options, Timeout) ->
    TModule = State#connection.transport_module,
    Conn = State#connection.connection,
    TModule:delete(Conn, Bucket, Key, Options, Timeout).

disconnect(State) ->
    TModule = State#connection.transport_module,
    Conn = State#connection.connection,
    TModule:disconnect(Conn).
