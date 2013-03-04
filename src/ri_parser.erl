%%
%% Simple parser for the model format produced by ri
%%
%% Author: Isak Karlsson <isak-kar@dsv.su.se>
%%
-module(ri_parser).
-include("ri.hrl").

-compile(export_all).


init() ->
    ets:new(semantic_vectors, [public, named_table, {write_concurrency, true}]).

stop() ->
    ets:delete(semantic_vectors).

run(File, Cores) ->
    catch stop(),
    init(),
    Io = csv:reader(File),
    Length = parse_header(Io),

    io:format("*** Parsed header, length '~p' *** ~n", [Length]),
    Then = now(),
    Self = self(),
    lists:foreach(fun (_) ->
			  spawn_link(?MODULE, parse_model_process, [Self, Io, Length, []])
		  end, lists:seq(1, Cores)),
    R = collect_parse_model_processes(Self, Cores, []),
    io:format(standard_error, "*** Read model in ~p second(s) *** ~n", 
	      [timer:now_diff(now(), Then) / 1000000]),
    wait_for_user_input(dict:from_list(R)).

wait_for_user_input(R) ->
    Commands = io:get_line(">> "),
    try 
	execute(string:tokens(Commands, ", \n"), R)
    catch
	_:X ->
	    io:format("Error: ~p ~n", [X])
    end,	
    wait_for_user_input(R).

execute(["cmp", X, Y], R) ->
    Sim = ri_similarity:cmp(X, Y, R),
    io:format("~p~n", [Sim]);
execute(["to", Word, Min], R) ->
    SimilarTo = ri_similarity:to(Word, list_to_float(Min), R),
    lists:foreach(fun ({Cmp, Sim}) ->
			  io:format("~s\t~p ~n", [Cmp, Sim])
		  end, SimilarTo);
execute(["to", Word, "min-length", Length], R) ->
    SimilarTo = ri_simiarlity:to(Word, fun(Item, _Sim) -> length(Item) > Length end, R),
    io:format("~p~n", [SimilarTo]); %% NOTE:Error undef
execute(["halt"], _) ->
    halt().

parse_model_process(Parent, Io, Length, Acc) ->
    case csv:get_next_line(Io) of
	{ok, Item, _} ->
	    {Word, Vector} = parse_item(Item),
	    parse_model_process(Parent, Io, Length, 
				[{Word, #semantic_vector{length=Length, values=Vector}}|Acc]);
	eof ->
	    Parent ! {done, Parent, Acc}
    end.

collect_parse_model_processes(Self, Cores, Acc) ->
    if Cores == 0 ->
	    Acc;
       true ->
	    receive
		{done, Self, R} ->
		    collect_parse_model_processes(Self, Cores - 1, Acc ++ R)
	    end
    end.

parse_header(Io) ->
    case csv:get_next_line(Io) of
	{ok, Item, _} ->
	    case parse_header_item(Item, []) of
		[Len] ->
		    Len;
		_ -> 
		    throw({error, invalid_header})
	    end;
	eof ->
	    throw({error, invalid_header})
    end.

parse_item([Word|Indicies]) ->
    {Word, dict:from_list(parse_item(Indicies, []))}.
parse_item([], Acc) ->
    Acc;
parse_item(["empty"], Acc) ->
    Acc;
parse_item([Index, Value|Rest], Acc) ->
    parse_item(Rest, [{to_number(Index), to_number(Value)}|Acc]).
parse_header_item([], Acc) ->
    Acc;
parse_header_item(["header"|Rest], Acc) ->
    parse_header_item(Rest, Acc);
parse_header_item(["length", Length|Rest], Acc) ->
    parse_header_item(Rest, [to_number(Length)|Acc]).

to_repr(Str) ->
    case is_numeric(Str) of
	{true, Number} ->
	    Number;
	false ->
	    Str
    end.

to_number(Str) ->
    case is_numeric(Str) of
	{true, Number} ->
	    Number;
	_ ->
	    throw({invalid_number})
    end.
	    

is_numeric(L) ->
    Float = (catch erlang:list_to_float(L)),
    Int = (catch erlang:list_to_integer(L)),
    case is_number(Float) of
	true ->
	    {true, Float};
	false ->
	    case is_number(Int) of
		true ->
		    {true, Int};
		false ->
		    false
	    end
    end.
