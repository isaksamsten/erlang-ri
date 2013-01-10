-module(ri_util).

-export([write_model_to_file/2,
	 write_index_to_file/1,
	 get_semantic_vector/2]).


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
write_model_to_file(File, Result) ->
    Io = case file:open(File, [raw, write]) of
	     {ok, Io0} -> Io0;
	     error -> throw({error, file_not_found})
	 end,
    dict:fold(fun (Word, Vector, _) ->
		      file:write(Io, io_lib:format("\"~s\",", [Word])),
		      case dict:size(Vector) of
			  0 ->
			      file:write(Io, io_lib:format("empty ~n", []));
			  _ ->
			      Indicies = lists:reverse(
					   dict:fold(fun (Index, Value, Acc) ->
							     [io_lib:format("~p:~p", [Index, Value])|Acc]
						     end, [], Vector)),
			      file:write(Io, io_lib:format("~s ~n", [string:join(Indicies, ",")]))
		      end
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
