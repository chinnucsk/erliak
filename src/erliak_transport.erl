-module(erliak_transport).

%% Public exports
-export([connect/3,
	 ping/2,
	 disconnect/1]).

%% Behaviour export
-export([behaviour_info/1]).

-include("erliak.hrl").

behaviour_info(callbacks) ->
    [{disconnect,1},
     {connect,3},
     {ping,2}];

behaviour_info(_Other) ->
    undefined.

%% API functions
connect(Address, Port, Options) ->
    %% Extract the transport from the options
    Transport = proplists:get_value(transport, Options, pb),
    Opts = proplists:delete(transport, Options),
    %% Perform connection based on the transport given
    case Transport of
    	pb ->
    	    {ok, Connection} = erliak_pb:connect(Address, Port, Opts);
    	http ->
    	    {ok, Connection} = erliak_http:connect(Address, Port, Opts)
    end,
    %% Store this connection in state
    State = #connection{transport = Transport, connection = Connection},
    {ok, State}.

ping(#connection{transport=pb, connection=Conn}, Timeout) ->
    erliak_pb:ping(Conn, Timeout); 
ping(#connection{transport=http, connection=Conn}, Timeout) ->
    erliak_http:ping(Conn, Timeout).

disconnect(#connection{transport=pb, connection=Conn}) ->
    erliak_pb:disconnect(Conn);
disconnect(#connection{transport=http, connection=Conn}) ->
    erliak_http:disconnect(Conn).
    

