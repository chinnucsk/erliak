-module(erliak_tests).
-include_lib("eunit/include/eunit.hrl").
%%-include("erliak.hrl").
-compile(export_all).
%% ===================================================================
%% Unit Tests
%% ===================================================================

%% A set of tests is defined in test_suite/0. The application environment 
%% variable default_transport controls which protocol is being used 
%% as the default. This way we can ensure both clients run through the 
%% same tests and behave consistently.
%% Process
%% 1. Set pb as default
%% 2. Run test_suite()
%% 3. Set http as default
%% 4. Run test_suite()

%% Test values
test_buckets() ->
	[<<"b1">>, <<"b2">>, <<"b3">>].
test_bucket() ->
    <<"b">>.
test_keys() ->
	[ list_to_binary("k" ++ integer_to_list(N)) || N <- lists:seq(1,9) ].
test_key() ->
    <<"k">>.
test_value() ->
    <<"v">>.

%% sasl and ibrowse started before and stopped after every test, eh?
setup() ->	
    application:set_env(sasl, sasl_error_logger, {file, "erliak_test_sasl.log"}),
    error_logger:logfile({open, "erliak_test.log"}),
    error_logger:tty(false),

    [ ok = application:start(A) || A <- [sasl, ibrowse] ],

    %% application:start(ibrowse),
    
    {ok, C} = erliak:start_link(),
    C.

cleanup(C) ->	        
	reset_riak(),
    erliak:stop(C),
    [ ok = application:stop(A) || A <- [sasl, ibrowse] ].

%% The test suite that both clients should pass
test_suite() ->
    Transport = atom_to_list(erliak_env:get_env(default_transport)),
    reset_riak(),
    [{Transport ++ " - ping",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin		  
		    ?assertEqual(pong, erliak:ping()),
		    ?assertEqual(pong, erliak:ping(1000))
		end)}},	 
     {Transport ++ " - get server info",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin		  
		    {ok, ServerInfo} = erliak:get_server_info(),
		    [{node, _}, {server_version, _}] = lists:sort(ServerInfo)
		end)}},
	 {Transport ++ " - get server info with timeout",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin		  
		    {ok, ServerInfo} = erliak:get_server_info(500),
		    [{node, _}, {server_version, _}] = lists:sort(ServerInfo)
		end)}},		 
	 {Transport ++ " - get client id",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			{ok, ClientID} = erliak:get_client_id(),
			%% Make sure it's a binary (PB) or a list of characters (HTTP)
			case erliak_env:get_env(default_transport) of
				http ->
					?assertEqual(true, is_list(ClientID));
				pb ->
					?assertEqual(true, is_binary(ClientID))
			end		    
		end)}},
	 {Transport ++ " - get client id with timeout",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			{ok, ClientID} = erliak:get_client_id(500),
			%% Make sure it's a binary (PB) or a list of characters (HTTP)
			case erliak_env:get_env(default_transport) of
				http ->
					?assertEqual(true, is_list(ClientID));
				pb ->
					?assertEqual(true, is_binary(ClientID))
			end		    
		end)}},		 
	 {Transport ++ " - empty list buckets with and without timeout",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin			
			?assertEqual({ok, []}, erliak:list_buckets()),
			?assertEqual({ok, []}, erliak:list_buckets(500))
		end)}},	 	 
	%% CHECK HOW CURRENT TESTS DO THIS
	%% make sure to delete the bucket
	 {Transport ++ " - put then list buckets with timeout and call timeout",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			Buckets = test_buckets(),
			F = fun(B) -> 
				Obj = riakc_obj:new(B, test_key(), test_value()),
				erliak:put(Obj)
			end,
			[ F(B) || B <- Buckets ],
			{ok, BucketList} = erliak:list_buckets(500, 500),
			?assertEqual(Buckets, lists:sort(BucketList))
		end)}},	 	 
	 {Transport ++ " - list keys in empty bucket with and without timeout",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin			
			?assertEqual({ok, []}, erliak:list_keys(<<"very_empty_bucket">>)),
			?assertEqual({ok, []}, erliak:list_keys(<<"very_empty_bucket">>, 500))		
		end)}},
	 {Transport ++ " - put keys then list keys",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin			
			Keys = test_keys(),
			F = fun(K) ->
				Obj = riakc_obj:new(test_bucket(), K, test_value()),
				erliak:put(Obj)
			end,
			[ F(K) || K <- Keys ],
			{ok, KeyList} = erliak:list_keys(test_bucket()),
			?assertEqual(Keys, lists:sort(KeyList))			
		end)}},
	 {Transport ++ " - stream list keys in empty bucket",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin			
			%% TODO get rid of PB gen_server in favour of erliak's gen_server
			{ok, Ref} = erliak:stream_list_keys(<<"even_emptier_bucket">>)			
			% MB = erlang:process_info(self(),[message_queue_len,messages]),
   %  		io:format("*************** MB ~p~n", [MB]),
			% ?assertEqual(ok, self())
			% receive
			% 	Msg ->
			% 		Msg
			% end

			% receive
			% 	{GRef, done} ->
			% 		?assertEqual(GRef, Ref)
			% end			
		end)}},
	 {Transport ++ " - put keys then stream list keys",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin			
			%% TODO get rid of PB gen_server in favour of erliak's gen_server
			% Keys = test_keys(),
			% F = fun(K) ->
			% 	Obj = riakc_obj:new(test_bucket(), K, test_value()),
			% 	erliak:put(Obj)
			% end,
			% [ F(K) || K <- Keys ],
			{ok, Ref} = erliak:stream_list_keys(test_bucket())
		end)}},
	 {Transport ++ " - get bucket properties",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin			
			%% TODO http will fail this test as it'll return more properties
			%% TODO alternative test for now, as HTTP returns more properties
			%% TODO also test with timeout
			{ok, Props} = erliak:get_bucket(test_bucket()),
			?assertEqual({ok,Props}, erliak:get_bucket(test_bucket(), 500)),
			?assertEqual({ok,Props}, erliak:get_bucket(test_bucket(), 500, 700)),
            ?assertEqual(false, proplists:get_value(allow_mult, Props)),
            ?assertEqual(3, proplists:get_value(n_val, Props))
            % ?assertEqual([{allow_mult,false},
            %               {n_val,3}],
            %               lists:sort(Props))
		end)}},	 
	 {Transport ++ " - set bucket properties then get bucket properties",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin			
			%% TODO alternative test for now, as HTTP returns more properties
			ok = erliak:set_bucket(test_bucket(), [{n_val, 2}, {allow_mult, true}]),
            {ok, Props} = erliak:get_bucket(test_bucket()),
            ?assertEqual(true, proplists:get_value(allow_mult, Props)),
			?assertEqual(2, proplists:get_value(n_val, Props))
                          
            % ?assertEqual([{allow_mult,true},
            %               {n_val,2}],
            %               lists:sort(Props))
		end)}},
     {Transport ++ " - simple put then read value of get",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			?assertEqual({error, notfound}, erliak:get(test_bucket(), test_key())),
		    Obj = riakc_obj:new(test_bucket(), test_key(), test_value()),
		    ok = erliak:put(Obj),
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
     {Transport ++ " - put and delete w. timeout, ensure deleted",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
		    Obj = riakc_obj:new(test_bucket(), test_key(), test_value()),
		    ok = erliak:put(Obj),
		    erliak:delete(test_bucket(), test_key(), 500),
		    ?assertEqual({error, notfound}, erliak:get(test_bucket(), test_key()))
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
	 {Transport ++ " - javascript source map test",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			O = riakc_obj:new(test_bucket(), test_key()),
			erliak:put(riakc_obj:update_value(O, <<"2">>, "application/json")),
			?assertEqual({ok, [{0, [2]}]},
            	erliak:mapred([{test_bucket(), test_key()}],
                              [{map, {jsanon, <<"function (v) { return [JSON.parse(v.values[0].data)]; }">>},
							  undefined, true}]))
		end)}},
	 {Transport ++ " - javascript named map test",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			O = riakc_obj:new(test_bucket(), test_key()),
			erliak:put(riakc_obj:update_value(O, <<"99">>, "application/json")),
            ?assertEqual({ok, [{0, [99]}]},
				erliak:mapred([{test_bucket(), test_key()}],
                              [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, true}],
                              500))
		end)}},
	 {Transport ++ " - javascript_source_map_reduce_test()",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			Store = fun({K,V}) ->
				O=riakc_obj:new(<<"bucket">>, K),
				erliak:put(riakc_obj:update_value(O, V, "application/json"))
			end,
			[Store(KV) || KV <- [{<<"foo">>, <<"2">>},
								 {<<"bar">>, <<"3">>},
								 {<<"baz">>, <<"4">>}]],

			?assertEqual({ok, [{1, [3]}]},
				erliak:mapred([{<<"bucket">>, <<"foo">>},
							   {<<"bucket">>, <<"bar">>},
							   {<<"bucket">>, <<"baz">>}],
							  [{map, {jsanon, <<"function (v) { return [1]; }">>}, undefined, false},
							   {reduce, {jsanon,
								<<"function(v) {
									total = v.reduce(
										function(prev,curr,idx,array) {
										return prev+curr;
									}, 0);
									return [total];
								}">>},
								undefined, true}],
								500, 500))
		end)}},
	 {Transport ++ " - javascript_named_map_reduce_test()",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			Store = fun({K,V}) ->
            	O=riakc_obj:new(<<"bucket">>, K),
             	erliak:put(riakc_obj:update_value(O, V, "application/json"))
     		end,
			[Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                  				 {<<"bar">>, <<"3">>},
                  				 {<<"baz">>, <<"4">>}]],
			?assertEqual({ok, [{1, [9]}]},
	          	erliak:mapred([{<<"bucket">>, <<"foo">>},
	                           {<<"bucket">>, <<"bar">>},
	                           {<<"bucket">>, <<"baz">>}],
	                          [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
	                           {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]))
		end)}},
	 {Transport ++ " - javascript_bucket_map_reduce_test()",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			Store = fun({K,V}) ->
            	O=riakc_obj:new(<<"bucket">>, K),
             	erliak:put(riakc_obj:update_value(O, V, "application/json"))
     		end,
			[Store(KV) || KV <- [{<<"foo">>, <<"2">>},
            				     {<<"bar">>, <<"3">>},
                  				 {<<"baz">>, <<"4">>}]],
			?assertEqual({ok, [{1, [9]}]},
          		erliak:mapred_bucket(<<"bucket">>,
                                	 [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                 	  {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]))
		end)}},
	 {Transport ++ " - javascript_arg_map_reduce_test()",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
         	O=riakc_obj:new(<<"bucket">>, <<"foo">>),
         	erliak:put(riakc_obj:update_value(O, <<"2">>, "application/json")),
         	?assertEqual({ok, [{1, [10]}]},
          		erliak:mapred([{{<<"bucket">>, <<"foo">>}, 5},
                          	   {{<<"bucket">>, <<"foo">>}, 10},
                          	   {{<<"bucket">>, <<"foo">>}, 15},
                          	   {{<<"bucket">>, <<"foo">>}, -15},
                          	   {{<<"bucket">>, <<"foo">>}, -5}],
                         	  [{map, {jsanon, <<"function(v, arg) { return [arg]; }">>}, undefined, false},
                          	   {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]))

		end)}},
	 {Transport ++ " - erlang_map_reduce_test()",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
         	Store = fun({K,V}) ->
            	O=riakc_obj:new(<<"bucket">>, K),
                erliak:put(riakc_obj:update_value(O, V, "application/json"))
            end,
         	[Store(KV) || KV <- [{<<"foo">>, <<"2">>},
                              	 {<<"bar">>, <<"3">>},
                              	 {<<"baz">>, <<"4">>}]],
         	{ok, [{1, Results}]} = erliak:mapred([{<<"bucket">>, <<"foo">>},
                                                  {<<"bucket">>, <<"bar">>},
                                                  {<<"bucket">>, <<"baz">>}],
                                                 [{map, {modfun, riak_kv_mapreduce, map_object_value}, undefined, false},
                                                  {reduce, {modfun, riak_kv_mapreduce, reduce_set_union}, undefined, true}]),
         	?assertEqual([<<"2">>, <<"3">>, <<"4">>], lists:sort(Results))
		end)}},
	 {Transport ++ " - missing_key_erlang_map_reduce_test()",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
            {ok, Results} = erliak:mapred([{<<"bucket">>, <<"foo">>},
                                           {<<"bucket">>, <<"bar">>},
                                           {<<"bucket">>, <<"baz">>}],
                                          [{map, {modfun, riak_kv_mapreduce, map_object_value}, <<"include_notfound">>, false},
                                           {reduce, {modfun, riak_kv_mapreduce, reduce_set_union}, undefined, true}]),
            io:format("~n************** ~p~n", [Results]),
         	[{1, [{error, notfound}|_]}] = Results
		end)}},
	 {Transport ++ " - missing_key_javascript_map_reduce_test()",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
            {ok, Results} = erliak:mapred([{<<"bucket">>, <<"foo">>},
                                           {<<"bucket">>, <<"bar">>},
                                           {<<"bucket">>, <<"baz">>}],
                                          [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                           {reduce, {jsfun, <<"Riak.reduceSort">>}, undefined, true}]),                        
            [{1,
            	[{struct,[{<<"not_found">>,
                       {struct,[_, _, {<<"keydata">>,<<"undefined">>}]}}]}|_]}] = Results             	           
		end)}},
	 {Transport ++ " - map reduce bad inputs",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
            Res = erliak:mapred(undefined,
                                [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                 {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]),
            ?assertEqual({error, {0, <<"{inputs,{\"Inputs must be a binary bucket, a tuple of bucket and key-filters, a list of target tuples, or a search, index, or modfun tuple:\",\n         undefined}}">>}},
                         Res)
		end)}},
	 {Transport ++ " - map reduce bad input keys",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
            Res = erliak:mapred([<<"b">>], % no {B,K} tuple
                                [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                 {reduce, {jsfun, <<"Riak.reduceSum">>}, undefined, true}]),
            ?assertEqual({error,{0, <<"{inputs,{\"Inputs target tuples must be {B,K} or {{B,K},KeyData}:\",[<<\"b\">>]}}">>}},
                         Res)
		end)}},
	%% TODO OLD ASSERTION
	% ?assertEqual({error,<<"{'query',{\"Query takes a list of step tuples\",undefined}}">>}, Res)
	% ACTUALLY RETURNS {error, {0, <<"{'query',{\"Query takes a list of step tuples\",undefined}}">>}}
	 {Transport ++ " - map reduce bad query",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			Res = erliak:mapred([{<<"b">>,<<"k">>}], % no {B,K} tuple
        						 undefined),
            ?assertEqual({error,{0, <<"{'query',{\"Query takes a list of step tuples\",undefined}}">>}},
                         Res)
		end)}},
	 {Transport ++ " - javascript_source_map_reduce_test()",
      { setup,
	fun setup/0,
	fun cleanup/1,
	?_test( begin
			ok
		end)}}
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%














%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%








switch_protocol_to_pb_test_() ->
    [{"switch to pb as default",
      ?_test( begin
		  ok = erliak_env:set_env(default_transport, pb),
		  ?assertEqual(pb, erliak_env:get_env(default_transport))
	      end
	    )}
    ].
    
pb_test_() ->
    test_suite().

switch_protocol_to_http_test_() ->
    [{"switch to http as default",
      ?_test( begin
		  ok = erliak_env:set_env(default_transport, http),
		  ?assertEqual(http, erliak_env:get_env(default_transport))
	      end
	    )}
    ].

http_test_() ->
    test_suite().

%% ===================================================================
%% Riak Resetting code from the pb client
%% ===================================================================

%% Get the riak version from the init boot script, turn it into a list
%% of integers.
riak_version() ->
    StrVersion = element(2, rpc:call(test_riak_node(), init, script_id, [])),
    [ list_to_integer(V) || V <- string:tokens(StrVersion, ".") ].

%% Resets the riak node
reset_riak() ->
    %% sleep because otherwise we're going to kill the vnodes too fast
    %% for the supervisor's maximum restart frequency, which will bring
    %% down the entire node
    ?assertEqual(ok, maybe_start_network()),
    case riak_version() of
        [1,Two|_] when Two >= 2->
            reset_riak_12();
        _ ->
            reset_riak_legacy()
    end.

%% Resets a Riak 1.2+ node, which can run the memory backend in 'test'
%% mode.
reset_riak_12() ->
    set_test_backend(),
    ok = rpc:call(test_riak_node(), riak_kv_memory_backend, reset, []),
    reset_ring().

%% Sets up the memory/test backend, leaving it alone if already set properly.
set_test_backend() ->
    Env = rpc:call(test_riak_node(), application, get_all_env, [riak_kv]),
    Backend = proplists:get_value(storage_backend, Env),
    Test = proplists:get_value(test, Env),
    case {Backend, Test} of
        {riak_kv_memory_backend, true} ->
            ok;
        _ ->
            ok = rpc:call(test_riak_node(), application, set_env, [riak_kv, storage_backend, riak_kv_memory_backend]),
            ok = rpc:call(test_riak_node(), application, set_env, [riak_kv, test, true]),
            Vnodes = rpc:call(test_riak_node(), riak_core_vnode_manager, all_vnodes, [riak_kv_vnode]),
            [ ok = rpc:call(test_riak_node(), supervisor, terminate_child, [riak_core_vnode_sup, Pid]) ||
                {_, _, Pid} <- Vnodes ]
    end.

%% Resets a Riak 1.1 and earlier node.
reset_riak_legacy() ->
    timer:sleep(500),
    %% Until there is a good way to empty the vnodes, require the
    %% test to run with ETS and kill the vnode master/sup to empty all the ETS tables
    %% and the ring manager to remove any bucket properties
    ok = rpc:call(test_riak_node(), application, set_env, [riak_kv, storage_backend, riak_kv_memory_backend]),

    %% Restart the vnodes so they come up with ETS
    ok = supervisor:terminate_child({riak_kv_sup, test_riak_node()}, riak_kv_vnode_master),
    ok = supervisor:terminate_child({riak_core_sup, test_riak_node()}, riak_core_vnode_sup),
    {ok, _} = supervisor:restart_child({riak_core_sup, test_riak_node()}, riak_core_vnode_sup),
    {ok, _} = supervisor:restart_child({riak_kv_sup, test_riak_node()}, riak_kv_vnode_master),

    %% Clear the MapReduce cache
    ok = rpc:call(test_riak_node(), riak_kv_mapred_cache, clear, []),

    %% Now reset the ring so bucket properties are default
    reset_ring().

%% Resets the ring to a fresh one, effectively deleting any bucket properties.
reset_ring() ->
    Ring = rpc:call(test_riak_node(), riak_core_ring, fresh, []),
    ok = rpc:call(test_riak_node(), riak_core_ring_manager, set_my_ring, [Ring]).

maybe_start_network() ->
    %% Try to spin up net_kernel
    os:cmd("epmd -daemon"),
    case net_kernel:start([test_eunit_node(), longnames]) of
        {ok, _} ->
            erlang:set_cookie(test_riak_node(), test_cookie()),
            ok;
        {error, {already_started, _}} ->
            ok;
        X ->
            X
    end.

%% Cookie for distributed erlang
test_cookie() ->
    case os:getenv("RIAK_TEST_COOKIE") of
        false ->
            'riak';
        CookieStr ->
            list_to_atom(CookieStr)
    end.    

%% Riak node under test - used to setup/configure/tweak it for tests
test_riak_node() ->
    case os:getenv("RIAK_TEST_NODE_1") of
        false ->
            'riak@127.0.0.1';
        NodeStr ->
            list_to_atom(NodeStr)
    end.

%% Node for the eunit node for distributed erlang
test_eunit_node() ->
    case os:getenv("RIAK_EUNIT_NODE") of
        false ->
            'eunit@127.0.0.1';
        EunitNodeStr ->
            list_to_atom(EunitNodeStr)
    end.    