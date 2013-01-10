-module(ri_parser).
-compile(export_all).

run(Cores, Io) ->
    Length = parse_header(Io),

    Self = self(),
    lists:foreach(fun (_) ->
			  spawn_link(?MODULE, parse_model_process, [Self, Io, Length])
		  end, lists:seq(1, Cores)),
    collect_parse_model_processes(Self, Cores),
    wait_for_user_input().

parse_model_process(Io, Length) ->
    ok.

collect_parse_model_process(Self, Cores) ->
    if Cores == 0 ->
	    ok;
       true ->
	    receive
		{done, Self} ->
		    collect_parse_model_process(Self, Cores - 1)
	    end
    end.

parse_header(Io) ->
    case csv:get_next_line(Io) of
	{ok, Item} ->
	    case parse_header_item(Item) of
		{ok, Len} ->
		    Len;
		_ -> 
		    throw({error, invalid_header})
	    end;
	eof ->
	    throw({error, invalid_header})
    end.
	
