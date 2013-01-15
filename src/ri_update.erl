%% Copyright Isak Karlsson <isak-kar@dsv.su.se>
-module(ri_update).
-export([vector_update_process/3,
	 vector_update_collector_process/4,
	 spawn_vector_update_processes/2]).
	 
-include("ri.hrl").

%%
%% Process that updates an item (concurrently), by reading a new line
%% from Io.
%%
vector_update_process(Parent, RiConf, IndexVector) ->
    random:seed(erlang:now()),
    vector_update_process(Parent, RiConf, IndexVector, dict:new()).

vector_update_process(Parent, #ri_conf{file=Io, window=Window} = RiConf, IndexVector, Result) ->
    case csv:get_next_line(Io) of
	{ok, Item, Id} ->
	    Result0 = case Window of
			  X when is_number(X) ->
			      update_item(Result, Item, Window, IndexVector);
			  doc ->
			      update_all(Result, Item, IndexVector, Id);
			  item ->
			      update_all_pivots(Result, Id, IndexVector, Item)
		      end,			  
	    vector_update_process(Parent, RiConf, IndexVector, Result0);
	eof ->
	    Parent ! {done, self(), Parent, Result}
    end.

%%
%% Either spawn new vector_update_processes if Childrens == 2 otherwise,
%% call spawn_vector_update_process
%%
vector_update_collector_process(Parent, RiConf, IndexVector, Childrens) ->
    Self = self(),
    case Childrens of
	X when X < 2 ->
	    Result = vector_update_process(Parent, RiConf, IndexVector),
	    Parent ! {done, Self, Parent, Result};
	2 ->
	    spawn_link(?MODULE, vector_update_process, [Self, RiConf, IndexVector]),
	    spawn_link(?MODULE, vector_update_process, [Self, RiConf, IndexVector]),
	    Result = wait_for_vector_updates(Self, Childrens, dict:new()),
	    Parent ! {done, Self, Parent, Result};
	X when X > 2 ->
	    Result = spawn_vector_update_processes(RiConf, IndexVector),
	    Parent ! {done, Self, Parent, Result}
    end.

%%
%% Spawn a Cores number of "vector_update_process"
%% that can receive Items for processing
%%
spawn_vector_update_processes(#ri_conf{cores=Cores} = RiConf, IndexVector) ->
    Self = self(),
    Childrens = round(Cores / 2),
    spawn_link(?MODULE, vector_update_collector_process,[Self, RiConf, IndexVector, Childrens]),
    spawn_link(?MODULE, vector_update_collector_process,[Self, RiConf, IndexVector, Childrens]),
    wait_for_vector_updates(Self, 2, dict:new()).

%%
%% Wait for Cores messages to be sent to Self in the form of: {done,
%% Pid, Self}
%%
wait_for_vector_updates(Self, Cores, Result) ->
    if Cores == 0 ->
	    Result;
       true ->
	    receive
		{done, _Pid, Self, Dict} ->
		    Result0 = merge_semantic_vectors(Result, Dict),
		    wait_for_vector_updates(Self, Cores - 1, Result0);
		{'EXIT', _, normal} ->
		    wait_for_vector_updates(Self, Cores, Result);			
		X -> throw({error, some_error, X})
	    end
    end.
   
%%
%% Update an item, using a window length of Window
%% a random index vector or Length
%%
update_item(Result, Items, Window, IndexVector) ->
    update_item(Result, Items, Window, IndexVector, queue:new()).

update_item(Result, [], _Conf, _Iv, _Q) ->
    Result;
update_item(Result, [Pivot|Rest], Window, IndexVector, Queue) ->
    Result0 = update_all(Result, queue:to_list(Queue), IndexVector, Pivot),
    Result1 = update_limit(Result0, {Window, 0}, Rest, IndexVector, Pivot),
    update_item(Result1, Rest, Window, IndexVector, case queue:len(Queue) >= Window of
							true->
							    {_, Old} = queue:out(Queue),
							    queue:in(Pivot, Old);
							false ->
							    queue:in(Pivot, Queue)
						    end).
%%
%% Update Pivot with all items in Items
%%
update_all(Result, Items, IndexVector, Pivot) ->
    case Items of
	[] ->
	    Result;
	[Item|Rest] ->
	    Result0 = update_pivot(Result, Pivot, Item, IndexVector),
	    update_all(Result0, Rest, IndexVector, Pivot)
    end.

update_all_pivots(Result, Item, IndexVector, Pivots) ->
    case Pivots of
	[] ->
	    Result;
	[Pivot|Rest] ->
	    update_all_pivots(update_pivot(Result, Pivot, Item, IndexVector),
			      Item, IndexVector, Rest)
    end.

%% 
%% Update Pivot with Current to Limit number of items
%%
update_limit(Result, {Limit, Current}, Items, IndexVector, Pivot) ->
    case Items of
	[] ->
	    Result;
	[Item|Rest] ->
	    if Limit > Current ->
		    Result0 = update_pivot(Result, Pivot, Item, IndexVector),
		    update_limit(Result0, {Limit, Current + 1}, Rest, IndexVector, Pivot);
	       true ->
		    Result
	    end
    end.

%%
%% Update Pivot (semantic vector) w.r.t. Item (index vector)
%%
update_pivot(Result, Pivot, Item, #index_vector{length=Length} = IndexVectorInfo) ->
    IndexVector = get_index_vector(Item, IndexVectorInfo),
    dict:update(Pivot, fun(PivotVector) ->
			       add_vectors(PivotVector, IndexVector)
		       end, new_semantic_vector(Length), Result).

%%
%% Init a random vector of Length lenght and the Prob prob to
%% spawn -1 or 1
%%
get_index_vector(Item, #index_vector{length=Length,
				     prob=Prob,
				     variance=Variance}) ->	    
    case ets:lookup(index_vectors, Item) of
	[{_, Vector}] ->
	    Vector;
	[] ->
	    Vector = new_index_vector(Length, Prob, Variance),
	    ets:insert(index_vectors, {Item, Vector}),
	    Vector
    end.

%%
%% Merge two collections of semantic vectors
%%
merge_semantic_vectors(VectorA, VectorB) ->
    dict:merge(fun (_, A, B) ->
		       merge_semantic_vector(A, B)
	       end, VectorA, VectorB).

%%
%% Merge the semantic vectors for two Items
%%
merge_semantic_vector(#semantic_vector{length=Length, values=VectorA}, 
		      #semantic_vector{length=Length, values=VectorB}) ->
    #semantic_vector{length=Length,
		     values=dict:merge(fun (_Key, ValueA, ValueB) ->
					       ValueA + ValueB
				       end, VectorA, VectorB)}.

%%
%% Create a new semantic vector
%%
new_semantic_vector(Length) ->   
    #semantic_vector{length=Length, values=dict:new()}.
    
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
add_vectors(#semantic_vector{length=Length, values=VectorA}, VectorB) ->
    #semantic_vector{length=Length, 
		     values=lists:foldl(fun ({Index, Value}, Acc) ->
						dict:update(Index, fun (Old) ->
									   Old + Value
								   end, Value, Acc)
					end, VectorA, VectorB)}.
