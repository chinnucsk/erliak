-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% Unit Tests
%% ===================================================================


%% Get the test host - check env RIAK_TEST_PB_HOST then env 'RIAK_TEST_HOST_1'
%% falling back to 127.0.0.1
test_ip() ->
    case os:getenv("RIAK_TEST_PB_HOST") of
        false ->
            case os:getenv("RIAK_TEST_HOST_1") of
                false ->
                    "127.0.0.1";
                Host ->
                    Host
            end;
        Host ->
            Host
    end.

%% Test port - check env RIAK_TEST_PBC_1
test_port() ->
    case os:getenv("RIAK_TEST_PBC_1") of
        false ->
            8087;
        PortStr ->
            list_to_integer(PortStr)
    end.

live_node_test_() ->
    [{"ping_pb",
      ?_test( begin
		  {ok, Pid} = start_link(test_ip(), test_port()),
		  ?assertEqual(pong, ?MODULE:ping()),
		  ?assertEqual(pong, ?MODULE:ping(1000)),
		  stop(Pid)
	      end)},
     {"ping_http",
      ?_test( begin
		  {ok, Pid} = start_link(test_ip(), test_port(), [{transport, http}]),
		  ?assertEqual(ok, ?MODULE:ping()),
		  ?assertEqual(ok, ?MODULE:ping(1000)),
		  stop(Pid)
	      end)}
    ].
