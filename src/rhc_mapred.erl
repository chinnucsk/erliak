%% -------------------------------------------------------------------
%%
%% riakhttpc: Riak HTTP Client
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc This module contains utilities that the rhc module uses to
%%      encode and decode map/reduce queries.
-module(rhc_mapred).

-export([encode_mapred/2,
         wait_for_mapred/2]).
%% spawnable exports
-export([mapred_acceptor/3]).

%%% REQUEST ENCODING

%% @doc Translate erlang-term map/reduce query into JSON format.
%% @spec encode_mapred(map_input(), [query_part()]) -> iolist()
%% @type map_input() = bucket()|[key_spec()]|
%%                     {modfun, atom(), atom(), term()}
%% @type key_spec() = {bucket(), key()}|{{bucket(), key()},tag()}
%% @type bucket() = binary()
%% @type key() = binary()
%% @type tag() = binary()
%% @type query_part() = {map, funspec(), binary(), boolean()}|
%%                      {reduce, funspec(), binary(), boolean()}|
%%                      {link, linkspec(), linkspec(), boolean()}
%% @type funspec() = {modfun, atom(), atom()}|
%%                   {jsfun, binary()}|
%%                   {jsanon, {bucket(), key()}}|
%%                   {jsanon, binary()}
%% @type linkspec() = binary()|'_'
encode_mapred(Inputs, Query) ->
    mochijson2:encode(
      {struct, [{<<"inputs">>, encode_mapred_inputs(Inputs)},
                {<<"query">>, encode_mapred_query(Query)}]}).

encode_mapred_inputs(Bucket) when is_binary(Bucket) ->
    Bucket;
encode_mapred_inputs(Keylist) when is_list(Keylist) ->
    [ normalize_mapred_input(I) || I <- Keylist ];
encode_mapred_inputs({modfun, Module, Function, Options}) ->
    {struct, [{<<"module">>, atom_to_binary(Module, utf8)},
              {<<"function">>, atom_to_binary(Function, utf8)},
              {<<"arg">>, Options}]}.

%% @doc Normalize all bucket-key-data inputs to either
%%        [Bucket, Key]
%%      or
%%        [Bucket, Key, KeyData]
normalize_mapred_input({Bucket, Key})
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key];
normalize_mapred_input({{Bucket, Key}, KeyData})
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key, KeyData];
normalize_mapred_input([Bucket, Key])
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key];
normalize_mapred_input([Bucket, Key, KeyData])
  when is_binary(Bucket), is_binary(Key) ->
    [Bucket, Key, KeyData].

encode_mapred_query(Query) when is_list(Query) ->
    [ encode_mapred_phase(P) || P <- Query ].

encode_mapred_phase({MR, Fundef, Arg, Keep}) when MR =:= map;
                                                  MR =:= reduce ->
    Type = if MR =:= map -> <<"map">>;
              MR =:= reduce -> <<"reduce">>
           end,
    {Lang, Json} = case Fundef of
                       {modfun, Mod, Fun} ->
                           {<<"erlang">>,
                            [{<<"module">>,
                              list_to_binary(atom_to_list(Mod))},
                             {<<"function">>,
                              list_to_binary(atom_to_list(Fun))}]};
                       {jsfun, Name} ->
                           {<<"javascript">>,
                            [{<<"name">>, Name}]};
                       {jsanon, {Bucket, Key}} ->
                           {<<"javascript">>,
                            [{<<"bucket">>, Bucket},
                             {<<"key">>, Key}]};
                       {jsanon, Source} ->
                           {<<"javascript">>,
                            [{<<"source">>, Source}]}
                   end,
    {struct,
     [{Type,
       {struct, [{<<"language">>, Lang},
                 {<<"arg">>, Arg},
                 {<<"keep">>, Keep}
                 |Json
                ]}
      }]};
encode_mapred_phase({link, Bucket, Tag, Keep}) ->
    {struct,
     [{<<"link">>,
       {struct, [{<<"bucket">>, if Bucket =:= '_' -> <<"_">>;
                                   true           -> Bucket
                                end},
                 {<<"tag">>, if Tag =:= '_' -> <<"_">>;
                                true        -> Tag
                             end},
                 {<<"keep">>, Keep}
                 ]}
       }]}.

%%% RESPONSE DECODING


%% @doc Collect all mapreduce results, and provide them as one value
%%      instead of streaming to a Pid.
%% @spec wait_for_mapred(term(), integer()) ->
%%            {ok, [phase_result()]}|{error, term()}
%% @type phase_result() = {integer(), [term()]}
wait_for_mapred(ReqId, Timeout) ->
    wait_for_mapred(ReqId,Timeout,orddict:new()).
%% @private
wait_for_mapred(ReqId, Timeout, Acc) ->
    receive
        {ReqId, done} -> {ok, orddict:to_list(Acc)};
        {ReqId, {mapred,Phase,Res}} ->
            io:format("mapred, Phase, Res = ~p | ~p~n", [Phase, Res]),
            wait_for_mapred(ReqId,Timeout,orddict:append_list(Phase,Res,Acc));
        {ReqId, {error, Reason}} -> io:format("MATCH ERROR REASON~n"), {error, Reason}
    after Timeout ->
            {error, {timeout, orddict:to_list(Acc)}}
    end.

%% @doc first stage of ibrowse response handling - just waits to be
%%      told what ibrowse request ID to expect
mapred_acceptor(Pid, PidRef, Timeout) ->
    receive
        {ibrowse_req_id, PidRef, IbrowseRef} ->
            mapred_acceptor(Pid,PidRef,Timeout,IbrowseRef)
    after Timeout ->
            Pid ! {PidRef, {error, {timeout, []}}}
    end.

%% @doc second stage of ibrowse response handling - waits for headers
%%      and extracts the boundary of the multipart/mixed message
mapred_acceptor(Pid,PidRef,Timeout,IbrowseRef) ->
    receive
        {ibrowse_async_headers, IbrowseRef, Status, Headers} ->
            if Status =/= "200" ->
                    Pid ! {PidRef, {error, {Status, Headers}}};
               true ->
                    {"multipart/mixed", Args} =
                        rhc_obj:ctype_from_headers(Headers),
                    {"boundary", Boundary} =
                        proplists:lookup("boundary", Args),
                    stream_parts_acceptor(
                      Pid, PidRef,
                      webmachine_multipart:stream_parts(
                        {[],stream_parts_helper(Pid,PidRef,Timeout,
                                                IbrowseRef,true)},
                        Boundary))
            end
    after Timeout ->
            Pid ! {PidRef, {error, timeout}}
    end.

%% @doc driver of the webmachine_multipart streamer - handles results
%%      of the parsing process (sends them to the client) and polls for
%%      the next part
stream_parts_acceptor(Pid,PidRef,done_parts) ->
    Pid ! {PidRef, done};
stream_parts_acceptor(Pid,PidRef,{{_Name, _Param, Part},Next}) ->
    {struct, Response} = mochijson2:decode(Part),
    Phase = proplists:get_value(<<"phase">>, Response),
    io:format("RESPONSE: ˙~p~n", [Response]),
    Res = proplists:get_value(<<"data">>, Response),
    Pid ! {PidRef, {mapred, Phase, Res}},
    stream_parts_acceptor(Pid,PidRef,Next()).

%% @doc "next" fun for the webmachine_multipart streamer - waits for
%%      an ibrowse message, and then returns it to the streamer for processing
stream_parts_helper(Pid, PidRef, Timeout, IbrowseRef, First) ->              
    fun() ->
            receive
                {ibrowse_async_response_end, IbrowseRef} ->
                    {<<>>,done};
                {ibrowse_async_response, IbrowseRef, {error, Error}} ->
                    Pid ! {PidRef, {error, Error}},
                    throw({error, {ibrowse, Error}});
                {ibrowse_async_response, IbrowseRef, []} ->
                    Fun = stream_parts_helper(Pid, PidRef, Timeout,
                                              IbrowseRef, First),
                    Fun();
                {ibrowse_async_response, IbrowseRef, Data0} ->
                    %% the streamer doesn't like the body to start with
                    %% CRLF, so strip that off on the first chunk
                    Data = if First ->
                                   case Data0 of
                                       <<"\n",D/binary>> -> D;
                                       <<"\r\n",D/binary>> -> D;
                                       _ -> Data0
                                   end;
                              true ->
                                   Data0
                           end,
                    {Data,
                     stream_parts_helper(Pid, PidRef, Timeout,
                                         IbrowseRef, false)}
            after Timeout ->
                    Pid ! {PidRef, {error, timeout}},
                    throw({error, {ibrowse, timeout}})
            end
    end.
