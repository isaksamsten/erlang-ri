-module(ri_util).
-include("ri.hrl").
-export([write_model_to_file/3,
	 write_reduced_to_file/3,
	 write_index_to_file/1,
	 get_semantic_vector/2,
	 take_nth/2,
	 unique_keep_order/1]).

unique_keep_order(List) ->
    unique_keep_order(List, sets:new(), []).

unique_keep_order(List, Seen, Acc) ->
    case List of
	[] ->
	    lists:reverse(Acc);
	[H|Rest] ->
	    case sets:is_element(H, Seen) of
		true ->
		    unique_keep_order(Rest, Seen, Acc);
		false ->
		    unique_keep_order(Rest, sets:add_element(H, Seen), [H|Acc])
	    end
    end.

%%
%% Remove item on N
%%
take_nth([A|R], 1) ->
    {A, R};
take_nth(List, N) ->
    {L1, [Item|L2]} = lists:split(N-1, List),
    {Item, L1 ++ L2}.


%%
%% Get the semantic vector for Item from Vectors
%%
get_semantic_vector(Item, Vectors) ->
    case dict:find(Item, Vectors) of
	{ok, Vector} ->
	    {ok, Vector};
	error ->
	    not_found
    end.

%%
%% Write model (Result) to File
%%
write_model_to_file(File, Length, Result) ->
    Io = case file:open(File, [write]) of
	     {ok, Io0} -> Io0;
	     error -> throw({error, file_not_found})
	 end,
    file:write(Io, io_lib:format("header,length,~p~n", [Length])),
    dict:fold(fun (Word, #semantic_vector{values=Vector}, _) ->
		      file:write(Io, io_lib:format("~p,", [Word])),
		      case dict:size(Vector) of
			  0 ->
			      file:write(Io, io_lib:format("empty~n", []));
			  _ ->
			      Indicies = lists:reverse(
					   dict:fold(fun (Index, Value, Acc) ->
							     [io_lib:format("~p,~p", [Index, Value])|Acc]
						     end, [], Vector)),
			      file:write(Io, io_lib:format("~s~n", [string:join(Indicies, ",")]))
		      end
	      end, [], Result),
    file:close(Io).

write_reduced_to_file(File, Length, Result) ->
    Io = case file:open(File, [raw, write]) of
	     {ok, Io0} -> Io0;
	     error -> throw({error, file_not_found})
	 end,
    
    file:write(Io, io_lib:format("~s,", [string:join(
					   [io_lib:format("~p", [S]) || S <- lists:seq(1, Length)], ",")])),
    
    file:write(Io, "class\n"),
    dict:fold(fun (Doc, #semantic_vector{class=Class, values=Vector}, _) ->
		      Values = lists:map(fun(Index) ->
						 case dict:find(Index, Vector) of
						     {ok, Value} ->
							 integer_to_list(Value);
						     error ->
							 "0"
						 end
					 end, lists:seq(1, Length)),
		      io:format("Writing doc: ~p ~p ~p ~n", [Doc, Class, length(Values)]),
		      file:write(Io, io_lib:format("~s,~s~n", [string:join(Values, ","), Class]))
	      end, [], Result),
    file:close(Io).
		      
%%
%% Write the random index to File
%%
write_index_to_file(File) ->
    Io = case file:open(File, [raw, write]) of
	     {ok, Io0} -> Io0;
	     _ -> throw({error, file_not_found})
	 end,
    ets:foldl(fun ({Word, Indicies}, _) ->
		      file:write(Io, io_lib:format("\"~s\",", [Word])),
		      Str = lists:foldl(fun ({Index, Value}, Acc) ->
						[io_lib:format("~p:~p", [Index, Value])|Acc]
					end, [], Indicies),
		      file:write(Io, io_lib:format("~s ~n", [string:join(Str, ",")]))
	      end, [], index_vectors),
    file:close(Io).
