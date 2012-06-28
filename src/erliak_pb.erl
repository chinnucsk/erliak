-module(erliak_pb).
-behaviour(erliak_transport).

-export([connect/3, 
	 ping/2,
	 disconnect/1]).

connect(Address, Port, Options) ->
    riakc_pb_socket:start_link(Address, Port, Options).

ping(Connection, Timeout) ->
    riakc_pb_socket:ping(Connection, Timeout).

disconnect(Connection) ->
    riakc_pb_socket:stop(Connection).
