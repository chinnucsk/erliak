-module(erliak_http).
-behaviour(erliak_transport).

-export([connect/3, 
	 ping/2,
	 disconnect/1]).

%% @doc Assemble the root URL for the given client
%% @spec root_url(rhc()) -> iolist()
%% root_url(Address, Port) ->
%%     binary_to_list(iolist_to_binary(["http://",Address,":",integer_to_list(Port),"/"])).

%% parse_links(Links) ->
%%     %% Get a list of "</PATH>; rel=\"key\"" strings
%%     TokenizedLinks = string:tokens(Links, ","),
%%     parse_links(TokenizedLinks, []).

%% parse_links([Link|Rest], []) ->
%%     [Path, Rel] = string:tokens(Link, ";"),
%%     io:format("Links ~p~n", [Link]),
%%     parse_links(Rest, []);

%% parse_links([], Ack) ->
%%     Ack.

%% @doc Performs a GET request on http://Address:Port/ and reads the 
%%      list of links to build a record containing a LUT for all 
%%      http resources
%% build_configuration(Address, Port) ->
%%     Url = root_url(Address, Port),
%%     Response = {ok, _Status, Headers, _Body} = request(get, Url, ["200","204"]),
%%     io:format("Response: ~p~n", [Response]),
%%     %% Extract the links
%%     Links = proplists:get_value("Link", Headers),
%%     %% Parse the links to a computable format
%%     parse_links(Links).
%%     %%io:format("DECODED LINKS = ~p~n", [string:tokens(Links, ",")]).

    
%%     %% parse_links(Links).
    


connect(Address, Port, Options) ->
    Prefix = "riak",
    Connection = rhc:create(Address, Port, Prefix, Options),
    %% TODO build a lookup table from the links returned from getting http://Address:Port/
    {ok, Connection}.

ping(Connection, _Timeout) ->
    rhc:ping(Connection).

disconnect(_Connection) ->
    ok.
