-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% Unit Tests
%% ===================================================================

live_node_test_() ->
    [{"ping_pb",
      ?_test( begin
		  {ok, Pid} = erliak:start_link([{transport, pb}]),
		  ?assertEqual(pong, erliak:ping()),
		  ?assertEqual(pong, erliak:ping(1000)),
		  erliak:stop(Pid)
	      end)},
     {"ping_http",
      ?_test( begin
		  application:start(sasl),
		  application:start(ibrowse),
		  {ok, Pid} = erliak:start_link([{transport, http}]),
		  ?assertEqual(ok, erliak:ping()),
		  ?assertEqual(ok, erliak:ping(1000)),
		  erliak:stop(Pid)
	      end)}
    ].
