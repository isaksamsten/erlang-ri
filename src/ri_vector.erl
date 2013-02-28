-module(ri_vector).
-compile(export_all).

-include("ri.hrl").

%%
%% Init a random vector of Length lenght and the Prob prob to
%% spawn -1 or 1
%%
get_index_vector(Item, Length, Prob, Variance) ->	    
    case ets:lookup(index_vectors, Item) of
	[{_, Vector}] ->
	    Vector;
	[] ->
	    Vector = new_index_vector(Length, Prob, Variance),
	    ets:insert(index_vectors, {Item, Vector}),
	    Vector
    end.

get_index_vector(Item, #index_vector{pid=Pid}) ->
    Self = self(),
    Pid ! {get, Self, Item},
    receive
	{get, Pid, IndexVector} ->
	    IndexVector
    end.

index_vector_handler(Length, Prob, Variance) ->
    spawn_link(fun() ->
		       index_vector_process(Length, Prob, Variance)
	       end).

index_vector_process(Length, Prob, Variance) ->
    receive
	{get, Pid, Item} ->
	    Pid ! {get, self(), get_index_vector(Item, Length, Prob, Variance)},
	    index_vector_process(Length, Prob, Variance)
    end.
	    

%%
%% Merge two collections of semantic vectors
%%
merge_semantic_vectors(VectorsA, VectorsB) ->
    dict:merge(fun (_, A, B) ->
		       merge_semantic_vector(A, B)
	       end, VectorsA, VectorsB).

%%
%% Merge the semantic vectors for two Items
%%
merge_semantic_vector(#semantic_vector{class=Class, length=Length, values=VectorA}, 
		      #semantic_vector{class=Class, length=Length, values=VectorB}) ->
    #semantic_vector{class=Class, length=Length,
		     values=dict:merge(fun (_Key, ValueA, ValueB) ->
					       ValueA + ValueB
				       end, VectorA, VectorB)}.

%%
%% Create a new semantic vector
%%
new_semantic_vector(Class, Length) ->
    #semantic_vector{class=Class, length=Length, values=dict:new()}.
    
%%
%% Generate an index vector
%%
generate_index_vector(_, 0, Sets) ->
    sets:to_list(Sets);
generate_index_vector(Length, Set, Sets) ->
    Index = random:uniform(Length),
    case sets:is_element(Index, Sets) of
	true ->
	    generate_index_vector(Length, Set, Sets);
	false ->
	    case random:uniform() of
		X when X > 0.5 ->
		    generate_index_vector(Length, Set - 1, sets:add_element({Index, 1}, Sets));
		_ ->
		    generate_index_vector(Length, Set - 1, sets:add_element({Index, -1}, Sets))
	    end
    end.

new_index_vector(Length, Values, Variance) ->
    Set = round(Values + (random:uniform() * Variance * case random:uniform() of
							    X when X > 0.5 ->
								1;
							    _ -> -1
							end)),
    generate_index_vector(Length, Set, sets:new()).

%%
%% VectorA -> {Length, dict() -> {Index, Value}}
%%
add(#semantic_vector{values=VectorA} = Vector, VectorB) ->
    Vector#semantic_vector{values=lists:foldl(fun ({Index, Value}, Acc) ->
						      dict:update(Index, fun (Old) ->
										 Old + Value
									 end, Value, Acc)
					      end, VectorA, VectorB)}.


%%
%% Calculate the dot product of two semantic vectors
%%
dot_product(#semantic_vector{values=A}, 
	    #semantic_vector{values=B}) ->
    dict:fold(fun (Index, ValueA, Dot) ->
		      case dict:find(Index, B) of
			  {ok, ValueB} ->
			      Dot + (ValueA * ValueB);
			  error ->
			      Dot
		      end
	      end, 0, A).

%%
%% Calculate the magnitude of a semantic vector
%%
magnitude(#semantic_vector{values=Vector}) ->		      
    math:sqrt(dict:fold(fun (_, Value, Acc) ->
				Acc + (Value * Value)
			end, 0, Vector)).
