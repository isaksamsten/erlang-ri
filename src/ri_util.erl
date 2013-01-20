-module(ri_util).
-include("ri.hrl").
-export([write_model_to_file/3,
	 write_reduced_to_file/3,
	 write_index_to_file/1,
	 get_semantic_vector/2,
	 take_nth/2]).

%%
%% Remove item on N
%%
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
    file:write(Io, "id,class,"),
    file:write(Io, io_lib:format("~s~n", [string:join(lists:duplicate(Length, "numeric"), ",")])),

    file:write(Io, "id,class,"),
    file:write(Io, io_lib:format("~s~n", [string:join(
					    [io_lib:format("~p", [S]) || S <- lists:seq(1, Length)], ",")])),
    dict:fold(fun (Doc, #semantic_vector{class=Class, values=Vector}, _) ->
		      file:write(Io, io_lib:format("~p,", [Doc])),
		      Values = lists:map(fun(Index) ->
						 case dict:find(Index, Vector) of
						     {ok, Value} ->
							 io_lib:format("~p", [Value]);
						     error ->
							 "0"
						 end
					 end, lists:seq(1, Length)),
		      file:write(Io, io_lib:format("~s,", [Class])),
		      file:write(Io, io_lib:format("~s~n", [string:join(Values, ",")]))
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
