-module(erliak_http).
-behaviour(erliak_transport).

-export([connect/3,
         ping/2,
         get/5,
         put/4,
	     delete/5,
         disconnect/1,
         get_server_info/2,
         get_client_id/2,
         list_buckets/3,
         list_keys/3,
         stream_list_keys/4,
         get_bucket/4,
         set_bucket/5,
         mapred/5,
         mapred_stream/6,
         mapred_bucket/5,
         mapred_bucket_stream/6,
         search/3, search/6
         ]).

-include("erliak_http.hrl").

%% ====================================================================
%% Public API
%% ====================================================================

connect(default_address, default_port, _Options) ->
    Connection = create(),
    {ok, Connection};
connect(Address, Port, Options) ->
    Prefix = "riak",
    Connection = create(Address, Port, Prefix, Options),
    %% TODO build a lookup table from the links returned from getting http://Address:Port/
    {ok, Connection}.

ping(State, _Timeout) ->
    Connection = State#connection.connection,
    ok = e_ping(Connection),
    pong.

get(State, Bucket, Key, Options, _Timeout) ->
    Connection = State#connection.connection,
    e_get(Connection, Bucket, Key, Options).

put(State, Object, Options, _Timeout) ->
    Connection = State#connection.connection,
    e_put(Connection, Object, Options).

delete(State, Bucket, Key, Options, _Timeout) ->
    Connection = State#connection.connection,
    e_delete(Connection, Bucket, Key, Options).

disconnect(_State) ->
    ok.

get_server_info(State, _Timeout) ->
    Connection = State#connection.connection,
    e_get_server_info(Connection).

get_client_id(State, _Timeout) ->
    Connection = State#connection.connection,
    e_get_client_id(Connection).

%% @doc List all buckets on the server
%% <em>This is a potentially expensive operation and should not be used in production.</em>
-spec list_buckets(#rhc{}, timeout(), timeout()) -> {ok, [bucket()]} | {error, term()}.
list_buckets(State, _Timeout, _CallTimeout) ->
    Connection = State#connection.connection,
    Url = list_buckets_url(Connection),
    case request(get, Url, ["200","204"]) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            Buckets = proplists:get_value(<<"buckets">>, Response),
            {ok, Buckets};
        {error, Error} ->
            {error, Error}
    end.

list_keys(State, Bucket, _Timeout) ->
    Connection = State#connection.connection,
    e_list_keys(Connection, Bucket).

%% TODO needs to know the PID of the calling process to be able to forward the 
%%  messages received to it
stream_list_keys(State, Bucket, _Timeout, _CallTimeout) ->
    Connection = State#connection.connection,
    % io:format("************* erliak_http stream_list_keys My self() ~p~n", [self()]),
    e_stream_list_keys(Connection, Bucket).
    % A = e_stream_list_keys(Connection, Bucket),
    % receive 
    %     MSG ->
    %         io:format("MSG = ~p~n", [MSG])
    % end,
    %%io:format("erliak_http stream_list_keys My self() ~p~n", [Connection#]),
    % timer:sleep(500),
    % MB = erlang:process_info(self(),[message_queue_len,messages]),
    % io:format("MB ~p~n", [MB]),
    % A.

get_bucket(State, Bucket, _Timeout, _CallTimeout) ->    
    Connection = State#connection.connection,
    e_get_bucket(Connection, Bucket).

set_bucket(State, Bucket, BucketProps, _Timeout, _CallTimeout) ->
    Connection = State#connection.connection,
    e_set_bucket(Connection, Bucket, BucketProps).


%% Auxiliary formatting function for individual {struct, KeyValuesStruct} structures
format_aux({struct, KeyValuesStruct}) ->
    [{Msg, {struct, KeyValues}}] = KeyValuesStruct,
    AtomMsg = list_to_atom(binary_to_list(Msg)),
    Bucket = proplists:get_value(<<"bucket">>, KeyValues),
    Key = proplists:get_value(<<"key">>, KeyValues),
    KeyData = proplists:get_value(<<"keydata">>, KeyValues),
    {AtomMsg, {Bucket, Key}, KeyData};
%% In the case of a simple data structure, simple return the values
format_aux(Values) ->
    Values.

format_result({struct, [{<<"error">>,<<"notfound">>}]}) ->
    [{error,notfound}];
format_result(Structs) ->
    [ format_aux(T) || T <- Structs ].    

% e_mapred throws error:function_clause if given invalid Inputs
% changed to correspond with PB's behaviour
mapred(State, Inputs, Query, default_timeout, _CallTimeout) ->
    Connection = State#connection.connection,    
    try
        {ok, [{N, Data}]} = e_mapred(Connection, Inputs, Query),        
        Formatted = format_result(Data),         
        {ok, [{N, Formatted}]}        
    catch
        % Malformed queries are reported differently
        error:function_clause when Query == undefined ->
            Msg = "{'query',{\"Query takes a list of step tuples\",undefined}}",
            {error, erlang:iolist_to_binary(Msg)};
        % Malformed inputs are reported differently
        error:function_clause when is_list(Inputs) ->
            Msg = io_lib:format("{inputs,{\"Inputs target tuples must be {B,K} or {{B,K},KeyData}:\",~p}}", [Inputs]),
            {error, erlang:iolist_to_binary(Msg)};            
        error:function_clause ->
            Msg = io_lib:format("{inputs,{\"Inputs must be a binary bucket, a tuple of bucket and key-filters, a list of target tuples, or a search, index, or modfun tuple:\",\n         ~p}}", [Inputs]),
            {error, erlang:iolist_to_binary(Msg)}            
    end;
            
mapred(State, Inputs, Query, Timeout, _CallTimeout) ->
    Connection = State#connection.connection,
    try
        {ok, [{N, Data}]} = e_mapred(Connection, Inputs, Query, Timeout),
        Formatted = format_result(Data),         
        {ok, [{N, Formatted}]}        
    catch
        % Malformed queries are reported differently
        error:function_clause when Query == undefined ->
            Msg = "{'query',{\"Query takes a list of step tuples\",undefined}}",
            {error, erlang:iolist_to_binary(Msg)};
        % Malformed inputs are reported differently
        error:function_clause when is_list(Inputs) ->
            Msg = io_lib:format("{inputs,{\"Inputs target tuples must be {B,K} or {{B,K},KeyData}:\",~p}}", [Inputs]),
            {error, erlang:iolist_to_binary(Msg)};            
        error:function_clause ->
            Msg = io_lib:format("{inputs,{\"Inputs must be a binary bucket, a tuple of bucket and key-filters, a list of target tuples, or a search, index, or modfun tuple:\",\n         ~p}}", [Inputs]),
            {error, erlang:iolist_to_binary(Msg)}            
    end.

mapred_stream(State, Inputs, Query, ClientPid, default_timeout, _CallTimeout) ->
    Connection = State#connection.connection,
    e_mapred_stream(Connection, Inputs, Query, ClientPid);
mapred_stream(State, Inputs, Query, ClientPid, Timeout, _CallTimeout) ->    
    Connection = State#connection.connection,
    e_mapred_stream(Connection, Inputs, Query, ClientPid, Timeout).

mapred_bucket(State, Bucket, Query, default_timeout, _CallTimeout) ->
    Connection = State#connection.connection,
    e_mapred_bucket(Connection, Bucket, Query);
mapred_bucket(State, Bucket, Query, Timeout, _CallTimeout) ->    
    Connection = State#connection.connection,
    e_mapred_bucket(Connection, Bucket, Query, Timeout).

%% TODO consistent timeout behaviour?
% mapred_bucket_stream(Connection, Bucket, Query, ClientPid, default_timeout, _CallTimeout) ->
%     e_mapred_bucket_stream(Connection, Bucket, Query, ClientPid);
mapred_bucket_stream(State, Bucket, Query, ClientPid, Timeout, _CallTimeout) ->    
    Connection = State#connection.connection,
    e_mapred_bucket_stream(Connection, Bucket, Query, ClientPid, Timeout).

search(State, Bucket, SearchQuery) ->
    Connection = State#connection.connection,
    e_search(Connection, Bucket, SearchQuery).

%% TODO consistent timeout behaviour?
% search(Connection, Bucket, SearchQuery, MRQuery, default_timeout, _CallTimeout) ->    
%     e_search(Connection, Bucket, SearchQuery, MRQuery);
search(State, Bucket, SearchQuery, MRQuery, Timeout, _CallTimeout) ->            
    Connection = State#connection.connection,
    e_search(Connection, Bucket, SearchQuery, MRQuery, Timeout).


%% ====================================================================
%% Erliak utility
%% ====================================================================
list_buckets_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "/buckets?buckets=true"])).    

%% ====================================================================
%% Private (from riak-erlang-http-client)
%% ====================================================================

%% @doc Create a client for connecting to the default port on localhost.
%% @equiv create("127.0.0.1", 8098, "riak", [])
create() ->
    create(?DEFAULT_ADDRESS, ?DEFAULT_PORT, "riak", []).
    % create("127.0.0.1", 8098, "riak", []).

%% @doc Create a client for connecting to a Riak node.
%%
%%      Connections are made to:
%%      ```http://IP:Port/Prefix/(<bucket>/<key>)'''
%%
%%      Defaults for r, w, dw, rw, and return_body may be passed in
%%      the Options list.  The client id can also be specified by
%%      adding `{client_id, ID}' to the Options list.
%% @spec create(string(), integer(), string(), Options::list()) -> rhc()
create(IP, Port, Prefix, Opts0) when is_list(IP), is_integer(Port),
                                     is_list(Prefix), is_list(Opts0) ->
    Opts = case proplists:lookup(client_id, Opts0) of
               none -> [{client_id, random_client_id()}|Opts0];
               Bin when is_binary(Bin) ->
                   [{client_id, binary_to_list(Bin)}
                    | [ O || O={K,_} <- Opts0, K =/= client_id ]];
               _ ->
                   Opts0
           end,
    #rhc{ip=IP, port=Port, prefix=Prefix, options=Opts}.

%% @doc Get the IP this client will connect to.
%% @spec ip(rhc()) -> string()
ip(#rhc{ip=IP}) -> IP.

%% @doc Get the Port this client will connect to.
%% @spec port(rhc()) -> integer()
port(#rhc{port=Port}) -> Port.

%% @doc Get the prefix this client will use for object URLs
%% @spec prefix(rhc()) -> string()
prefix(#rhc{prefix=Prefix}) -> Prefix.

%% @doc Ping the server by requesting the "/ping" resource.
%% @spec e_ping(rhc()) -> ok|{error, term()}
e_ping(Rhc) ->
    Url = ping_url(Rhc),
    case request(get, Url, ["200","204"]) of
        {ok, _Status, _Headers, _Body} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

%% @doc Get the client ID that this client will use when storing objects.
%% @spec e_get_client_id(rhc()) -> {ok, string()}
e_get_client_id(Rhc) ->
    {ok, client_id(Rhc, [])}.

%% @doc Get some basic information about the server.  The proplist returned
%%      should include `node' and `server_version' entries.
%% @spec e_get_server_info(rhc()) -> {ok, proplist()}|{error, term()}
e_get_server_info(Rhc) ->
    Url = stats_url(Rhc),
    case request(get, Url, ["200"]) of
        {ok, _Status, _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {ok, erlify_server_info(Response)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Get the objects stored under the given bucket and key.
%%
%%      Allowed options are:
%%      <dl>
%%        <dt>`r'</dt>
%%          <dd>The 'R' value to use for the read</dd>
%%      </dl>
%%
%%      The term in the second position of the error tuple will be
%%      `notfound' if the key was not found.
%% @spec e_get(rhc(), bucket(), key(), proplist())
%%          -> {ok, riakc_obj()}|{error, term()}
e_get(Rhc, Bucket, Key, Options) ->
    Qs = get_q_params(Rhc, Options),
    Url = make_url(Rhc, Bucket, Key, Qs),
    case request(get, Url, ["200", "300"]) of
        {ok, _Status, Headers, Body} ->
            {ok, rhc_obj:make_riakc_obj(Bucket, Key, Headers, Body)};
        {error, {ok, "404", _, _}} ->
            {error, notfound};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Store the given object in Riak.
%%
%%      Allowed options are:
%%      <dl>
%%        <dt>`w'</dt>
%%          <dd>The 'W' value to use for the write</dd>
%%        <dt>`dw'</dt>
%%          <dd>The 'DW' value to use for the write</dd>
%%        <dt>return_body</dt>
%%          <dd>Whether or not to return the updated object in the
%%          response.  `ok' is returned if return_body is false.
%%          `{ok, Object}' is returned if return_body is true.</dd>
%%      </dl>
%% @spec e_put(rhc(), riakc_obj(), proplist())
%%         -> ok|{ok, riakc_obj()}|{error, term()}
e_put(Rhc, Object, Options) ->
    Qs = put_q_params(Rhc, Options),
    Bucket = riakc_obj:bucket(Object),
    Key = riakc_obj:key(Object),
    Url = make_url(Rhc, Bucket, Key, Qs),
    Method = if Key =:= undefined -> post;
                true              -> put
             end,
    {Headers0, Body} = rhc_obj:serialize_riakc_obj(Rhc, Object),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}
               |Headers0],
    case request(Method, Url, ["200", "204", "300"], Headers, Body) of
        {ok, Status, ReplyHeaders, ReplyBody} ->
            if Status =:= "204" ->
                    ok;
               true ->
                    {ok, rhc_obj:make_riakc_obj(Bucket, Key,
                                                ReplyHeaders, ReplyBody)}
            end;
        {error, Error} ->
            {error, Error}
    end.

%% @equiv delete(Rhc, Bucket, Key, [])
e_delete(Rhc, Bucket, Key) ->
    e_delete(Rhc, Bucket, Key, []).

%% @doc Delete the given key from the given bucket.
%%
%%      Allowed options are:
%%      <dl>
%%        <dt>`rw'</dt>
%%          <dd>The 'RW' value to use for the delete</dd>
%%      </dl>
%% @spec e_delete(rhc(), bucket(), key(), proplist()) -> ok|{error, term()}
e_delete(Rhc, Bucket, Key, Options) ->
    Qs = delete_q_params(Rhc, Options),
    Url = make_url(Rhc, Bucket, Key, Qs),
    Headers = [{?HEAD_CLIENT, client_id(Rhc, Options)}],
    case request(delete, Url, ["204"], Headers) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.

%% @doc Unsupported
%% @throws not_implemented
e_list_buckets(_Rhc) ->
    throw(not_implemented).

%% @doc List the keys in the given bucket.
%% @spec e_list_keys(rhc(), bucket()) -> {ok, [key()]}|{error, term()}
e_list_keys(Rhc, Bucket) ->
    {ok, ReqId} = e_stream_list_keys(Rhc, Bucket),
    rhc_listkeys:wait_for_listkeys(ReqId, ?DEFAULT_TIMEOUT).

%% @doc Stream key lists to a Pid.  Messages sent to the Pid will
%%      be of the form `{reference(), message()}'
%%      where `message()' is one of:
%%      <dl>
%%         <dt>`done'</dt>
%%            <dd>end of key list, no more messages will be sent</dd>
%%         <dt>`{keys, [key()]}'</dt>
%%            <dd>a portion of the key list</dd>
%%         <dt>`{error, term()}'</dt>
%%            <dd>an error occurred</dd>
%%      </dl>
%% @spec e_stream_list_keys(rhc(), bucket()) ->
%%          {ok, reference()}|{error, term()}
e_stream_list_keys(Rhc, Bucket) ->
    Url = make_url(Rhc, Bucket, undefined, [{?Q_KEYS, ?Q_STREAM},
                                            {?Q_PROPS, ?Q_FALSE}]),
    StartRef = make_ref(),    
    Pid = spawn(rhc_listkeys, list_keys_acceptor, [self(), StartRef]),
    case request_stream(Pid, get, Url) of
        {ok, ReqId}    ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} -> {error, Error}
    end.

%% @doc Get the properties of the given bucket.
%% @spec e_get_bucket(rhc(), bucket()) -> {ok, proplist()}|{error, term()}
e_get_bucket(Rhc, Bucket) ->
    Url = make_url(Rhc, Bucket, undefined, [{?Q_KEYS, ?Q_FALSE}]),
    case request(get, Url, ["200"]) of
        {ok, "200", _Headers, Body} ->
            {struct, Response} = mochijson2:decode(Body),
            {struct, Props} = proplists:get_value(?JSON_PROPS, Response),
            {ok, rhc_bucket:erlify_props(Props)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Set the properties of the given bucket.
%%
%%      Allowed properties are:
%%      <dl>
%%        <dt>`n_val'</dt>
%%          <dd>The 'N' value to use for storing data in this bucket</dd>
%%        <dt>`allow_mult'</dt>
%%          <dd>Whether or not this bucket should allow siblings to
%%          be created for its keys</dd>
%%      </dl>
%% @spec e_set_bucket(rhc(), bucket(), proplist()) -> ok|{error, term()}
e_set_bucket(Rhc, Bucket, Props0) ->
    Url = make_url(Rhc, Bucket, undefined, []),
    Headers =  [{"Content-Type", "application/json"}],
    Props = rhc_bucket:httpify_props(Props0),
    Body = mochijson2:encode({struct, [{?Q_PROPS, {struct, Props}}]}),
    case request(put, Url, ["204"], Headers, Body) of
        {ok, "204", _Headers, _Body} -> ok;
        {error, Error}               -> {error, Error}
    end.

%% @equiv e_mapred(Rhc, Inputs, Query, ?DEFAULT_TIMEOUT)
e_mapred(Rhc, Inputs, Query) ->
    e_mapred(Rhc, Inputs, Query, ?DEFAULT_TIMEOUT).

%% @doc Execute a map/reduce query. See {@link
%%      rhc_mapred:encode_mapred/2} for details of the allowed formats
%%      for `Inputs' and `Query'.
%% @spec e_mapred(rhc(), rhc_mapred:map_input(),
%%              [rhc_mapred:query_part()], integer())
%%         -> {ok, [rhc_mapred:phase_result()]}|{error, term()}
e_mapred(Rhc, Inputs, Query, Timeout) ->
    {ok, ReqId} = e_mapred_stream(Rhc, Inputs, Query, self(), Timeout),
    rhc_mapred:wait_for_mapred(ReqId, Timeout).

%% @equiv e_mapred_stream(Rhc, Inputs, Query, ClientPid, DEFAULT_TIMEOUT)
e_mapred_stream(Rhc, Inputs, Query, ClientPid) ->
    e_mapred_stream(Rhc, Inputs, Query, ClientPid, ?DEFAULT_TIMEOUT).

%% @doc Stream map/reduce results to a Pid.  Messages sent to the Pid
%%      will be of the form `{reference(), message()}',
%%      where `message()' is one of:
%%      <dl>
%%         <dt>`done'</dt>
%%            <dd>query has completed, no more messages will be sent</dd>
%%         <dt>`{mapred, integer(), mochijson()}'</dt>
%%            <dd>partial results of a query the second item in the tuple
%%             is the (zero-indexed) phase number, and the third is the
%%             JSON-decoded results</dd>
%%         <dt>`{error, term()}'</dt>
%%             <dd>an error occurred</dd>
%%      </dl>
%% @spec e_mapred_stream(rhc(), rhc_mapred:mapred_input(),
%%                     [rhc_mapred:query_phase()], pid(), integer())
%%          -> {ok, reference()}|{error, term()}
e_mapred_stream(Rhc, Inputs, Query, ClientPid, Timeout) ->
    Url = mapred_url(Rhc),
    StartRef = make_ref(),
    Pid = spawn(rhc_mapred, mapred_acceptor, [ClientPid, StartRef, Timeout]),
    Headers = [{?HEAD_CTYPE, "application/json"}],
    Body = rhc_mapred:encode_mapred(Inputs, Query),
    case request_stream(Pid, post, Url, Headers, Body) of
        {ok, ReqId} ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} -> {error, Error}
    end.

%% @doc Execute a search query. This command will return an error
%%      unless executed against a Riak Search cluster.
%% @spec e_search(rhc(), bucket(), string()) ->
%%       {ok, [rhc_mapred:phase_result()]}|{error, term()}
e_search(Rhc, Bucket, SearchQuery) ->
    %% Run a Map/Reduce operation using reduce_identity to get a list
    %% of BKeys.
    IdentityQuery = [{reduce, {modfun, riak_kv_mapreduce, reduce_identity}, none, true}],
    case e_search(Rhc, Bucket, SearchQuery, IdentityQuery, ?DEFAULT_TIMEOUT) of
        {ok, [{_, Results}]} ->
            %% Unwrap the results.
            {ok, Results};
        Other -> Other
    end.

%% @doc Execute a search query and feed the results into a map/reduce
%%      query. See {@link rhc_mapred:encode_mapred/2} for details of
%%      the allowed formats for `MRQuery'. This command will return an error
%%      unless executed against a Riak Search cluster.
%% @spec e_search(rhc(), bucket(), string(),
%%       [rhc_mapred:query_part()], integer()) ->
%%       {ok, [rhc_mapred:phase_result()]}|{error, term()}
e_search(Rhc, Bucket, SearchQuery, MRQuery, Timeout) ->
    Inputs = {modfun, riak_search, mapred_search, [Bucket, SearchQuery]},
    e_mapred(Rhc, Inputs, MRQuery, Timeout).

%% @equiv e_mapred_bucket(Rhc, Bucket, Query, DEFAULT_TIMEOUT)
e_mapred_bucket(Rhc, Bucket, Query) ->
    e_mapred_bucket(Rhc, Bucket, Query, ?DEFAULT_TIMEOUT).

%% @doc Execute a map/reduce query over all keys in the given bucket.
%% @spec e_mapred_bucket(rhc(), bucket(), [rhc_mapred:query_phase()],
%%                     integer())
%%          -> {ok, [rhc_mapred:phase_result()]}|{error, term()}
e_mapred_bucket(Rhc, Bucket, Query, Timeout) ->
    {ok, ReqId} = e_mapred_bucket_stream(Rhc, Bucket, Query, self(), Timeout),
    rhc_mapred:wait_for_mapred(ReqId, Timeout).

%% @doc Stream map/reduce results over all keys in a bucket to a Pid.
%%      Similar to {@link mapred_stream/5}
%% @spec e_mapred_bucket_stream(rhc(), bucket(),
%%                     [rhc_mapred:query_phase()], pid(), integer())
%%          -> {ok, reference()}|{error, term()}
e_mapred_bucket_stream(Rhc, Bucket, Query, ClientPid, Timeout) ->
    e_mapred_stream(Rhc, Bucket, Query, ClientPid, Timeout).

%% INTERNAL

%% @doc Get the client ID to use, given the passed options and client.
%%      Choose the client ID in Options before the one in the client.
%% @spec client_id(rhc(), proplist()) -> client_id()
client_id(#rhc{options=RhcOptions}, Options) ->
    case proplists:get_value(client_id, Options) of
        undefined ->
            proplists:get_value(client_id, RhcOptions);
        ClientId ->
            ClientId
    end.

%% @doc Generate a random client ID.
%% @spec random_client_id() -> client_id()
random_client_id() ->
    {{Y,Mo,D},{H,Mi,S}} = erlang:universaltime(),
    {_,_,NowPart} = now(),
    Id = erlang:phash2([Y,Mo,D,H,Mi,S,node(),NowPart]),
    base64:encode_to_string(<<Id:32>>).

%% @doc Assemble the root URL for the given client
%% @spec root_url(rhc()) -> iolist()
root_url(#rhc{ip=Ip, port=Port}) ->
    ["http://",Ip,":",integer_to_list(Port),"/"].

%% @doc Assemble the URL for the map/reduce resource
%% @spec mapred_url(rhc()) -> iolist()
mapred_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "mapred/?chunked=true"])).

%% @doc Assemble the URL for the ping resource
%% @spec ping_url(rhc()) -> iolist()
ping_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "ping/"])).

%% @doc Assemble the URL for the stats resource
%% @spec stats_url(rhc()) -> iolist()
stats_url(Rhc) ->
    binary_to_list(iolist_to_binary([root_url(Rhc), "stats/"])).

%% @doc Assemble the URL for the given bucket and key
%% @spec make_url(rhc(), bucket(), key(), proplist()) -> iolist()
make_url(Rhc=#rhc{prefix=Prefix}, Bucket, Key, Query) ->
    binary_to_list(
      iolist_to_binary(
        [root_url(Rhc),
         Prefix, "/",
         Bucket, "/",
         [ [Key,"/"] || Key =/= undefined ],
         [ ["?", mochiweb_util:urlencode(Query)] || Query =/= [] ]
        ])).

%% @doc send an ibrowse request
request(Method, Url, Expect) ->
    request(Method, Url, Expect, [], []).
request(Method, Url, Expect, Headers) ->
    request(Method, Url, Expect, Headers, []).
request(Method, Url, Expect, Headers, Body) ->
    Accept = {"Accept", "multipart/mixed, */*;q=0.9"},
    case ibrowse:send_req(Url, [Accept|Headers], Method, Body,
                          [{response_format, binary}]) of
        Resp={ok, Status, _, _} ->
            case lists:member(Status, Expect) of
                true -> Resp;
                false -> {error, Resp}
            end;
        Error ->
            Error
    end.

%% @doc stream an ibrowse request
request_stream(Pid, Method, Url) ->
    request_stream(Pid, Method, Url, []).
request_stream(Pid, Method, Url, Headers) ->
    request_stream(Pid, Method, Url, Headers, []).
request_stream(Pid, Method, Url, Headers, Body) ->
    case ibrowse:send_req(Url, Headers, Method, Body,
                          [{stream_to, Pid},
                           {response_format, binary}]) of
        {ibrowse_req_id, ReqId} ->
            {ok, ReqId};
        Error ->
            Error
    end.

%% @doc Get the default options for the given client
%% @spec options(rhc()) -> proplist()
options(#rhc{options=Options}) ->
    Options.

%% @doc Extract the list of query parameters to use for a GET
%% @spec get_q_params(rhc(), proplist()) -> proplist()
get_q_params(Rhc, Options) ->
    options_list([r], Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a PUT
%% @spec put_q_params(rhc(), proplist()) -> proplist()
put_q_params(Rhc, Options) ->
    options_list([r,w,dw,{return_body,"returnbody"}],
                 Options ++ options(Rhc)).

%% @doc Extract the list of query parameters to use for a DELETE
%% @spec delete_q_params(rhc(), proplist()) -> proplist()
delete_q_params(Rhc, Options) ->
    options_list([r,rw], Options ++ options(Rhc)).

%% @doc Extract the options for the given `Keys' from the possible
%%      list of `Options'.
%% @spec options_list([Key::atom()|{Key::atom(),Alias::string()}],
%%                    proplist()) -> proplist()
options_list(Keys, Options) ->
    options_list(Keys, Options, []).

options_list([K|Rest], Options, Acc) ->
    {Key,Alias} = case K of
                      {_, _} -> K;
                      _ -> {K, K}
                  end,
    NewAcc = case proplists:lookup(Key, Options) of
                 {Key,V} -> [{Alias,V}|Acc];
                 none  -> Acc
             end,
    options_list(Rest, Options, NewAcc);
options_list([], _, Acc) ->
    Acc.

%% @doc Convert a stats-resource response to an erlang-term server
%%      information proplist.
erlify_server_info(Props) ->
    lists:flatten([ erlify_server_info(K, V) || {K, V} <- Props ]).
erlify_server_info(<<"nodename">>, Name) -> {node, Name};
erlify_server_info(<<"riak_kv_version">>, Vsn) -> {server_version, Vsn};
erlify_server_info(_Ignore, _) -> [].


