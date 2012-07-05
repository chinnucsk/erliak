-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% Unit Tests
%% ===================================================================
%reset_riak() ->

test_address() ->
    "127.0.0.1".

test_port(pb) ->
    8087;
test_port(http) ->
    8098.

test_port() ->
    8087.

live_node_test_() ->
    [{"pb - ping",
      ?_test( begin
		  {ok, C} = erliak:start_link([{transport, pb}]),
		  ?assertEqual(pong, erliak:ping()),
		  ?assertEqual(pong, erliak:ping(1000)),
		  erliak:stop(C)
	      end)},
     {"http - ping",
      ?_test( begin
		  application:start(sasl),
		  application:start(ibrowse),
		  {ok, C} = erliak:start_link([{transport, http}]),
		  ?assertEqual(ok, erliak:ping()),
		  ?assertEqual(ok, erliak:ping(1000)),
		  erliak:stop(C)
	      end)},
     {"pb - simple put then read value of get",
      ?_test( begin
		  {ok, C} = erliak:start_link(),
		  O0 = riakc_obj:new(<<"b">>, <<"k">>, <<"v">>),
		  ok = erliak:put(O0),
		  {ok, GO} = erliak:get(<<"b">>, <<"k">>),
		  ?assertEqual(<<"v">>, riakc_obj:get_value(GO)),
		  erliak:stop(C)
	      end)},
     {"pb - get should read put",
      ?_test( begin
		  {ok, C} = erliak:start_link(),
		  O0 = riakc_obj:new(<<"b">>, <<"k">>),
		  O = riakc_obj:update_value(O0, <<"v">>),
		  {ok, PO} = erliak:put(O, [return_body]),
		  {ok, GO} = erliak:get(<<"b">>, <<"k">>),
		  ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO)),
		  erliak:stop(C)
	      end)},		      
     {"pb - get (with timeout) should read put",
      ?_test( begin
		  {ok, C} = start_link(test_address(), test_port(pb)),
		  O0 = riakc_obj:new(<<"b">>, <<"k">>),
		  O = riakc_obj:update_value(O0, <<"v">>),
		  {ok, PO} = erliak:put(O, [{w, 1}, {dw, 1}, return_body]),
		  {ok, GO} = erliak:get(<<"b">>, <<"k">>, 500),
		  ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO)),
		  erliak:stop(C)
	      end)},
     {"pb - get should read put with options",
      ?_test( begin
		  {ok, C} = erliak:start_link(test_address(), test_port(pb)),
		  Obj = riakc_obj:new(<<"b">>, <<"k">>),
		  O = riakc_obj:update_value(Obj, <<"v">>),		  		  
		  {ok, PO} = erliak:put(O, [{w, 1}, {dw, 1}, return_body]),
		  {ok, GO} = erliak:get(<<"b">>, <<"k">>, [{r, 1}]),
		  ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO)),
		  erliak:stop(C)
	      end)}
    ].
