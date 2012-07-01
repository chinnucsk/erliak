-module(erliak_transport).

%% Public exports
-export([e_connect/3,
	 e_ping/2,
	 e_get/5,
	 e_put/4,
	 e_disconnect/1]).

%% Behaviour export
-export([behaviour_info/1]).

-include("erliak.hrl").

behaviour_info(callbacks) ->
    [{e_connect,3},
     {e_ping,2},
	 {e_get/5},
	 {e_put/4},
	 {e_disconnect,1}
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
			list_to_existing_atom("erliak_" ++ atom_to_list(?DEFAULT_TRANSPORT))
	end.

%% API functions
e_connect(Address, Port, Options) ->
    %% Extract (and remove) the transport from the options
    Transport = proplists:get_value(transport, Options),
    Opts = proplists:delete(transport, Options),
	TModule = get_transport_module(Transport),
    %% Perform connection based on the transport given
	{ok, Connection} = TModule:e_connect(Address, Port, Opts),
    %% Store this connection in state
    State = #connection{
		connection = Connection,
		transport_module = TModule
	},
    {ok, State}.

e_ping(State, Timeout) ->
	TModule = State#connection.transport_module,
	Conn = State#connection.connection,
	TModule:e_ping(Conn, Timeout).

e_get(State, Bucket, Key, Options, Timeout) ->
	TModule = State#connection.transport_module,
	Conn = State#connection.connection,
	TModule:e_get(Conn, Bucket, Key, Options, Timeout).

e_put(State, Object, Options, Timeout) ->
	TModule = State#connection.transport_module,
	Conn = State#connection.connection,
	TModule:e_put(Conn, Object, Options, Timeout).

e_disconnect(State) ->
	TModule = State#connection.transport_module,
	Conn = State#connection.connection,
    TModule:e_disconnect(Conn).
