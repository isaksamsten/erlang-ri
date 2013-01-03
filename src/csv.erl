-module(csv).
-compile(export_all).

parse(file, File) ->
    case file:open(File, [raw,read]) of
	{ok, IO} ->
	    parse(acc, IO, []);
	_ ->
	    throw({error, file_dont_exist})
    end.
parse(file, File, Fun) ->
    case file:open(File, [raw,read]) of
	{ok, Io} ->
	    parse(call, Io, {0, Fun});
	_ ->
	    throw({error, file_dont_exist})
    end;
parse(acc, Io, Acc) ->
    case file:read_line(Io) of
	{ok, Line} ->
	    parse(acc, Io, [parse_line(Line, [])| Acc]);
	eof ->
	    lists:reverse(Acc);
	{error, Reason} ->
	    throw({error, Reason})
    end;
parse(call, Io, {Id, Fun}) ->
    case file:read_line(Io) of
	{ok, Line} ->
	    Fun(Id, parse_line(Line, [])),
	    parse(call, Io, {Id + 1, Fun});
	eof ->
	    Fun(eof, []),
	    ok;
	{error, Reason} ->
	    throw({error, Reason})
    end.

spawn_parser(File) ->
    case file:open(File, [raw, read]) of
	{ok, Io} ->
	    parse_incremental(Io);
	_ ->
	    throw({error, file_not_found})
    end.

parse_incremental(Io) ->
    receive
	{more, Parent} ->
	    case file:read_line(Io) of
		{ok, Line} ->
		    Item = parse_line(Line, []),
		    Parent ! {ok, Parent, Item},
		    parse_incremental(Io);
		eof ->
		    Parent ! {eof, Parent};
		{error, Reason} ->
		    throw({error, Reason})
	    end
    end.

get_next_line(Pid) ->
    Self = self(),
    Pid ! {more, Self},
    receive
	{ok, Self, Item} ->
	    {ok, Item};
	{eof, Self} ->
	    eof
    end.
	    

parse_line(Line, Acc) ->
    lists:reverse(parse_line(Line, [], Acc)).
parse_line([End], Str, Acc) ->
    Str0 = case End of
	       $\n ->
		   Str;
	       _ -> % NOTE: The last line of the file
		   [End|Str]
	   end,		   
    [lists:reverse(Str0)|Acc];
parse_line([$,|R], Str, Acc) ->
    parse_line(R, [], [lists:reverse(Str)|Acc]);
parse_line([I|R], Str, Acc) ->
    parse_line(R, [I|Str], Acc).


to_repr(Str) ->
    case is_numeric(Str) of
	{true, Number} ->
	    Number;
	false ->
	    list_to_atom(Str)
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
