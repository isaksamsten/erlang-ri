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
			  spawn_link(?MODULE, parse_model_process, [Self, Io, Length])
		  end, lists:seq(1, Cores)),
    collect_parse_model_processes(Self, Cores),
    io:format(standard_error, "*** Read model in ~p second(s) *** ~n", 
	      [timer:now_diff(now(), Then) / 1000000]),
    wait_for_user_input().

wait_for_user_input() ->
    ok.

parse_model_process(Parent, Io, Length) ->
    case csv:get_next_line(Io) of
	{ok, Item} ->
	    {Word, Vector} = parse_item(Item),
	    ets:insert(semantic_vectors, {Word, #semantic_vector{length=Length, values=Vector}}),
	    parse_model_process(Parent, Io, Length);
	eof ->
	    Parent ! {done, Parent}
    end.

collect_parse_model_processes(Self, Cores) ->
    if Cores == 0 ->
	    ok;
       true ->
	    receive
		{done, Self} ->
		    collect_parse_model_processes(Self, Cores - 1)
	    end
    end.

parse_header(Io) ->
    case csv:get_next_line(Io) of
	{ok, Item} ->
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
