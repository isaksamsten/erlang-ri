%% Copyright Isak Karlsson <isak-kar@dsv.su.se>
-module(ri_update).
-export([vector_update_process/5,
	 vector_update_collector_process/5,
	 spawn_vector_update_processes/5]).
	 
-include("ri.hrl").

%%
%% Process that updates an item (concurrently), by reading a new line
%% from Io.
%%
vector_update_process(Parent, Io, Window, IndexVector, Result) ->
    random:seed(erlang:now()),
    case csv:get_next_line(Io) of
	{ok, Item} ->
	    Result0 = update_item(Result, Item, Window, IndexVector),
	    vector_update_process(Parent, Io, Window, IndexVector, Result0);
	eof ->
	    Parent ! {done, self(), Parent, Result}
    end.

%%
%% Either spawn new vector_update_processes if Childrens == 2 otherwise,
%% call spawn_vector_update_process
%%
vector_update_collector_process(Parent, Io, Window, IndexVector, Childrens) ->
    Self = self(),
    case Childrens of
	X when X < 2 ->
	    spawn_link(?MODULE, vector_update_process, [Self, Io, Window, IndexVector, dict:new()]),
	    Result = wait_for_vector_updates(Self, Childrens, dict:new()),
	    Parent ! {done, Self, Parent, Result};
	2 ->
	    spawn_link(?MODULE, vector_update_process, [Self, Io, Window, IndexVector, dict:new()]),
	    spawn_link(?MODULE, vector_update_process, [Self, Io, Window, IndexVector, dict:new()]),
	    Result = wait_for_vector_updates(Self, Childrens, dict:new()),
	    Parent ! {done, Self, Parent, Result};
	X when X > 2 ->
	    Result = spawn_vector_update_processes(Childrens, 2, Io, Window, IndexVector),
	    Parent ! {done, Self, Parent, Result}
    end.

%%
%% Spawn a Cores number of "vector_update_process"
%% that can receive Items for processing
%%
spawn_vector_update_processes(Cores, Collectors, Io, Window, IndexVector) ->
    Self = self(),
    Childrens = round(Cores / Collectors),
    spawn_link(?MODULE, vector_update_collector_process,[Self, Io, Window, IndexVector, Childrens]),
    spawn_link(?MODULE, vector_update_collector_process,[Self, Io, Window, IndexVector, Childrens]),
    wait_for_vector_updates(Self, Collectors, dict:new()).

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
update_item(Result, Items, Window, IndexVector, Queue) ->
    case Items of
	[] ->
	    Result;
	[Pivot|Rest] ->
	    Result0 = update_all(Result, queue:to_list(Queue), IndexVector, Pivot),
	    Result1 = update_limit(Result0, {Window, 0}, Rest, IndexVector, Pivot),
	    update_item(Result1, Rest, Window, IndexVector, case queue:len(Queue) >= Window of
								true->
								    {_, Old} = queue:out(Queue),
								    queue:in(Pivot, Old);
								false ->
								    queue:in(Pivot, Queue)
							    end)
    end.

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
update_pivot(Result, Pivot, Item, IndexVectorInfo) ->
    IndexVector  = get_index_vector(Item, IndexVectorInfo),
    dict:update(Pivot, fun(PivotVector) ->
			       add_vectors(PivotVector, IndexVector)
		       end, new_semantic_vector(), Result).

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
merge_semantic_vector(VectorA, VectorB) ->
    dict:merge(fun (_Key, ValueA, ValueB) ->
		       ValueA + ValueB
	       end, VectorA, VectorB).

%%
%% Create a new semantic vector
%%
new_semantic_vector() ->   
    dict:new().
    
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
add_vectors(VectorA, VectorB) ->
    lists:foldl(fun ({Index, Value}, Acc) ->
			dict:update(Index, fun (Old) ->
						   Old + Value
					   end, Value, Acc)
		end, VectorA, VectorB).
