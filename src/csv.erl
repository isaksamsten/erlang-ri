%%
%% Very basic CSV parser of limited use.
%%
%% It can only handle one type of separators (,) and only one level of
%% string nesting
%%
%% Author: Isak Karlsson <isak-kar@dsv.su.se>
%%
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

reader(File) ->
    spawn_link(?MODULE, spawn_parser, [File]).

spawn_parser(File) ->
    case file:open(File, [raw, read, read_ahead]) of
	{ok, Io} ->
	    parse_incremental(Io);
	_ ->
	    throw({error, file_not_found})
    end.

parse_incremental(Io) ->
    case file:read_line(Io) of
	{ok, Line} ->
	    receive
		{more, Parent} ->
		    Item = parse_line(Line, []),
		    Parent ! {ok, Parent, Item},
		    parse_incremental(Io)
	    end;
	eof ->
	    receive 
		{more, Parent} ->
		    Parent ! {eof, Parent},
		    parse_incremental(Io)			
	    end;
	{error, Reason} ->
	    throw({error, Reason})
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
    [string:strip(lists:reverse(Str0))|Acc];
parse_line([$", $,|R], _, Acc) ->
    parse_line(R, [], ["\""|Acc]);
parse_line([$"|R], Str, Acc) ->
    parse_string_item(R, Str, Acc);
parse_line([$,|R], Str, Acc) ->
    parse_line(R, [], [string:strip(lists:reverse(Str))|Acc]);
parse_line([I|R], Str, Acc) ->
    parse_line(R, [I|Str], Acc).

parse_string_item([$", $,|R], Str, Acc) ->
    parse_line(R, [], [string:strip(lists:reverse(Str))|Acc]);
parse_string_item([$"|R], Str, Acc) ->
    parse_line(R, [], [string:strip(lists:reverse(Str))|Acc]);
parse_string_item([I|R], Str, Acc) ->
    parse_string_item(R, [I|Str], Acc).
