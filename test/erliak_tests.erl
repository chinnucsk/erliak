-module(erliak_tests).
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
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
test_bucket() ->
    <<"b">>.
test_key() ->
    <<"k">>.
test_value() ->
    <<"v">>.

setup_pb() ->
    {ok, C} = erliak:start_link([{transport, pb}]),
    C.
setup_http() ->
    application:start(sasl),
    application:start(ibrowse),
    {ok, C} = erliak:start_link([{transport, http}]),
    C.
setup_http2() ->
    application:start(sasl),
    application:start(ibrowse),
    {ok, C} = erliak:start_link(test_address(), test_port(http), [{transport, http}]),
    C.

setup() ->
    application:start(sasl),
    application:start(ibrowse),
    {ok, C} = erliak:start_link(),
    C.

cleanup(C) ->
    erliak:delete(test_bucket(), test_key()),
    erliak:stop(C),
    application:stop(sasl),
    application:stop(ibrowse).

pb_test_() ->
    [{"pb ping",
      { setup,
	fun setup_pb/0,
	fun cleanup/1,
	?_test( begin		  
		    ?assertEqual(pong, erliak:ping()),
		    ?assertEqual(pong, erliak:ping(1000))
		end)}},
     {"pb - simple put then read value of get",
      { setup,
	fun setup_pb/0,
	fun cleanup/1,
	?_test( begin
		    O0 = riakc_obj:new(test_bucket(), test_key(), test_value()),
		    ok = erliak:put(O0),
		    {ok, GO} = erliak:get(test_bucket(), test_key()),
		    ?assertEqual(test_value(), riakc_obj:get_value(GO))
		end)}},
     {"pb - put return_body compare get",
      { setup,
	fun setup_pb/0,
	fun cleanup/1,
	?_test( begin
		    O0 = riakc_obj:new(test_bucket(), test_key()),
		    O = riakc_obj:update_value(O0, test_value()),
		    {ok, PO} = erliak:put(O, [return_body]),
		    {ok, GO} = erliak:get(test_bucket(), test_key()),
		    ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
		end)}},
     {"pb - put w. options compare to get w. timeout",
      { setup,
	fun setup_pb/0,
	fun cleanup/1,
	?_test( begin
		    O0 = riakc_obj:new(test_bucket(), test_key()),
		    O = riakc_obj:update_value(O0, test_value()),
		    {ok, PO} = erliak:put(O, [{w, 1}, {dw, 1}, return_body]),
		    {ok, GO} = erliak:get(test_bucket(), test_key(), 500),
		    ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
		end)}},
     {"pb - put w. options compare to get w. options",
      { setup,
	fun setup_pb/0,
	fun cleanup/1,
	?_test( begin
		    Obj = riakc_obj:new(test_bucket(), test_key()),
		    O = riakc_obj:update_value(Obj, test_value()),		  		  
		    {ok, PO} = erliak:put(O, [{w, 1}, {dw, 1}, return_body]),
		    {ok, GO} = erliak:get(test_bucket(), test_key(), [{r, 1}]),
		    ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
		end)}},
     {"pb - put w. options & timeout compare to get w. options & timeout",
      { setup,
	fun setup_pb/0,
	fun cleanup/1,
	?_test( begin
		    Obj = riakc_obj:new(test_bucket(), test_key()),
		    O = riakc_obj:update_value(Obj, test_value()),		  		  
		    {ok, PO} = erliak:put(O, [return_body], 500),
		    {ok, GO} = erliak:get(test_bucket(), test_key(), [{r, 1}], 500),
		    ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
		end)}},
     {"pb - put w. timeout and delete, ensure deleted",
      { setup,
	fun setup_pb/0,
	fun cleanup/1,
	?_test( begin
		    Obj = riakc_obj:new(test_bucket(), test_key(), test_value()),
		    ok = erliak:put(Obj, 500),
		    erliak:delete(test_bucket(), test_key()),
		    ?assertEqual({error, notfound}, erliak:get(test_bucket(), test_key()))
		end)}},
     {"pb - put and delete w. timeout, ensure deleted",
      { setup,
	fun setup_pb/0,
	fun cleanup/1,
	?_test( begin
		    Obj = riakc_obj:new(test_bucket(), test_key(), test_value()),
		    ok = erliak:put(Obj),
		    erliak:delete(test_bucket(), test_key(), 500),
		    ?assertEqual({error, notfound}, erliak:get(test_bucket(), test_key()))
		end)}}
    ].

http_test_() ->
    [{"http ping",
     { setup,
       fun setup/0,
       fun cleanup/1,
       ?_test( begin		  
		   ?assertEqual(pong, erliak:ping()),
		   ?assertEqual(pong, erliak:ping(1000))
	       end)}
     }].


