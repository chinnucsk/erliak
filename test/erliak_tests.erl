-module(erliak_tests).
-include_lib("eunit/include/eunit.hrl").
%%-include("erliak.hrl").
-compile(export_all).
%% ===================================================================
%% Unit Tests
%% ===================================================================

test_bucket() ->
    <<"b">>.
test_key() ->
    <<"k">>.
test_value() ->
    <<"v">>.

%% sasl and ibrowse started before and stopped after every test, eh?
setup() ->
    [ ok = application:start(A) || A <- [sasl, ibrowse] ],
    {ok, C} = erliak:start_link(),
    C.

cleanup(C) ->
    erliak:delete(test_bucket(), test_key()),
    erliak:stop(C),
    [ ok = application:stop(A) || A <- [sasl, ibrowse] ].

switch_protocol_to_pb_test_() ->
    [{"switch to pb as default",
      ?_test( begin
		  ok = erliak_env:set_env(default_transport, pb),
		  ?assertEqual(pb, erliak_env:get_env(default_transport))
	      end
	    )}
    ].

pb_test_() ->
    Transport = atom_to_list(erliak_env:get_env(default_transport)),
    [{Transport ++ " - ping",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin		  
		    ?assertEqual(pong, erliak:ping()),
		    ?assertEqual(pong, erliak:ping(1000))
		end)}},
     {Transport ++ " - simple put then read value of get",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
		    O0 = riakc_obj:new(test_bucket(), test_key(), test_value()),
		    ok = erliak:put(O0),
		    {ok, GO} = erliak:get(test_bucket(), test_key()),
		    ?assertEqual(test_value(), riakc_obj:get_value(GO))
		end)}},
     {Transport ++ " - put return_body compare get",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
		    O0 = riakc_obj:new(test_bucket(), test_key()),
		    O = riakc_obj:update_value(O0, test_value()),
		    {ok, PO} = erliak:put(O, [return_body]),
		    {ok, GO} = erliak:get(test_bucket(), test_key()),
		    ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
		end)}},
     {Transport ++ " - put w. options compare to get w. timeout",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
		    O0 = riakc_obj:new(test_bucket(), test_key()),
		    O = riakc_obj:update_value(O0, test_value()),
		    {ok, PO} = erliak:put(O, [{w, 1}, {dw, 1}, return_body]),
		    {ok, GO} = erliak:get(test_bucket(), test_key(), 500),
		    ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
		end)}},
     {Transport ++ " - put w. options compare to get w. options",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
		    Obj = riakc_obj:new(test_bucket(), test_key()),
		    O = riakc_obj:update_value(Obj, test_value()),		  		  
		    {ok, PO} = erliak:put(O, [{w, 1}, {dw, 1}, return_body]),
		    {ok, GO} = erliak:get(test_bucket(), test_key(), [{r, 1}]),
		    ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
		end)}},
     {Transport ++ " - put w. options & timeout compare to get w. options & timeout",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
		    Obj = riakc_obj:new(test_bucket(), test_key()),
		    O = riakc_obj:update_value(Obj, test_value()),		  		  
		    {ok, PO} = erliak:put(O, [return_body], 500),
		    {ok, GO} = erliak:get(test_bucket(), test_key(), [{r, 1}], 500),
		    ?assertEqual(riakc_obj:get_contents(PO), riakc_obj:get_contents(GO))
		end)}},
     {Transport ++ " - put w. timeout and delete, ensure deleted",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
		    Obj = riakc_obj:new(test_bucket(), test_key(), test_value()),
		    ok = erliak:put(Obj, 500),
		    erliak:delete(test_bucket(), test_key()),
		    ?assertEqual({error, notfound}, erliak:get(test_bucket(), test_key()))
		end)}},
     {Transport ++ " - put and delete w. timeout, ensure deleted",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
		    Obj = riakc_obj:new(test_bucket(), test_key(), test_value()),
		    ok = erliak:put(Obj),
		    erliak:delete(test_bucket(), test_key(), 500),
		    ?assertEqual({error, notfound}, erliak:get(test_bucket(), test_key()))
		end)}}
    ].


switch_protocol_to_http_test_() ->
    [{"switch to http as default",
      ?_test( begin
		  ok = erliak_env:set_env(default_transport, http),
		  ?assertEqual(http, erliak_env:get_env(default_transport))
	      end
	    )}
    ].

http_test_() ->
    Transport = atom_to_list(erliak_env:get_env(default_transport)),
    [{Transport ++ " - ping",
     { setup,
       fun setup/0,
       fun cleanup/1,
       ?_test( begin		  
		   ?assertEqual(pong, erliak:ping()),
		   ?assertEqual(pong, erliak:ping(1000))
	       end)}
     }].


