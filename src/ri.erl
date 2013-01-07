-module(ri).
-compile(export_all).

-record(index_vector, {length, prob, variance}).

%%
%% Initialize the ets-tables (index_vectors and semantic_vectors)
%%
init() ->
    ets:new(index_vectors, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ok.

%%
%% Destruct the ets-tables
%%
stop() ->
    ets:delete(index_vectors),
    ok.

%%
%% Process that updates an item (concurrently)
%% Receives: {new, Items, Window, Length, Prob} or exit
%%
vector_update_process(Parent, Io, Window, IndexVector, Result) ->
    case csv:get_next_line(Io) of
	{ok, Item} ->
	    Result0 = update_item(Result, Item, Window, IndexVector),
	    vector_update_process(Parent, Io, Window, IndexVector, Result0);
	eof ->
	    Parent ! {done, self(), Parent, Result}
    end.

vector_update_collector_process(Parent, Io, Window, IndexVector, Childrens) ->
    Self = self(),
    io:format(standard_error, "Collector ~p spawning ~p updaters ~n", [Self, Childrens]),
    [spawn_link(?MODULE, vector_update_process,
		[Self, Io, Window, IndexVector, dict:new()]) || _ <- lists:seq(1, Childrens)],
    Result = wait_for_vector_updates(Self, Childrens, dict:new()),
    Parent ! {done, Self, Parent, Result}.

%%
%% Spawn a Cores number of "vector_update_process"
%% that can receive Items for processing
%%
spawn_vector_update_processes(Cores, Collectors, Io, Window, IndexVector) ->
    io:format(standard_error, "Spawning ~p collectors ~n", [Collectors]),
    Self = self(),
    Childrens = round(Cores / Collectors),
    lists:foreach(fun (_) ->
			  spawn_link(?MODULE, vector_update_collector_process,
				     [Self, Io, Window, IndexVector, Childrens])
		  end, lists:seq(1, Collectors)),
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
		{done, Pid, Self, Dict} ->
		    Then = now(),
		    Result0 = merge_semantic_vectors(Result, Dict),
		    io:format(standard_error, "Merging vectors from ~p in ~p second(s) ~n", 
			      [Pid, timer:now_diff(erlang:now(), Then) / 1000000]),
		    wait_for_vector_updates(Self, Cores - 1, Result0);
		_ -> throw({error, some_error})
	    end
    end.
   
%%
%% Update Items (a list of Items)
%%
update(Parent, Items, Window, IndexVector) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    update_item(Parent, Item, Window, IndexVector),
	    update(Parent, Rest, Window, IndexVector)
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

cosine(A, B) ->
    case dot_product(A, B) of 
	0.0 -> 0;
	X -> case magnitude(A) of
		 0.0 -> 0.0;
		 Y -> case magnitude(B) of
			  0.0 -> 0.0;
			  Z -> X / (Y * Z)
		      end
	     end
    end.

dot_product(A, B) ->
    dict:fold(fun (Index, ValueA, Dot) ->
		      case dict:find(Index, B) of
			  {ok, ValueB} ->
			      Dot + (ValueA * ValueB);
			  error ->
			      Dot
		      end
	      end, 0, A).

magnitude(Vector) ->		      
    math:sqrt(dict:fold(fun (_, Value, Acc) ->
				Acc + (Value * Value)
			end, 0, Vector)).
 
%%
%% Compare two semantic vectors
%%
similarity(A, B, Vectors) ->
    {ok, SemanticVectorA} = get_semantic_vector(A, Vectors),
    {ok, SemanticVectorB} = get_semantic_vector(B, Vectors),
    cosine(SemanticVectorA,SemanticVectorB).

%%
%% Get items similar to A
%%
similar_to(A, Min, Max, Vectors) ->
    case get_semantic_vector(A, Vectors) of
	{ok, VectorA} ->
	    lists:keysort(1, dict:fold(fun (Word, VectorB, Acc) ->
					       Similarity = cosine(VectorA, VectorB),
					       if Similarity > Min, Similarity =< Max, A /= Word ->
						       [{Similarity, Word}| Acc];
						  true ->
						       Acc
					       end
				       end, [], Vectors));
	not_found ->
	    not_found
    end.

			 
		      
%%
%% Running a file of documents
%%
run(File, Cores, Collectors, Window, Length, Prob, Variance) ->
    io:format(standard_error, "*** Running '~p' on ~p/~p core(s) *** ~n", [File, Cores, Collectors]),
    io:format(standard_error, "*** Sliding window: ~p, Index vector: ~p, Non zero bits: ~p+-~p *** ~n",
	      [Window, Length, Prob, Variance]),
    Pid = csv:reader(File),
    run_experiment(Pid, Cores, Collectors, Window, Length, Prob, Variance).
    

%%
%% Run a list of lists
%%
run_experiment(Io, Cores, Collectors, Window, Length, Prob, Variance) ->
    catch stop(),
    init(),
    Then = now(),
    Result = spawn_vector_update_processes(Cores, Collectors, Io, Window, #index_vector{length=Length, 
											prob=Prob, 
											variance=Variance}),
    io:format(standard_error, "Updating vectors took: ~p ~n", [timer:now_diff(now(), Then) / 1000000]),
    Result.

test() ->
    Vector0 = new_semantic_vector(),
    _Vector1 = new_semantic_vector(),
    _Vector2 = new_semantic_vector(),

    Index0 = new_index_vector(1000, 7, 2),
    Index1 = new_index_vector(1000, 7, 2),
    _Index2 = new_index_vector(1000, 7, 2),

    Vi0 = add_vectors(Vector0, Index0),
    Vi1 = add_vectors(Vi0, Index1),
    
    Vi2 = add_vectors(Vector0, Index0),
    Vi3 = add_vectors(Vector0, Index1),

    equal(Vi0, Vi1),
    equal(Vi1, merge_semantic_vector(Vi2, Vi3)).
    


equal(D1, D2) ->
    equal(dict:fetch_keys(D1) ++ dict:fetch_keys(D2), D1, D2).

equal([], _, _) ->
    true;
equal([H|Rest], D1, D2) ->
    case dict:find(H, D1) of
	{ok, Value} ->
	    case dict:find(H, D2) of
		{ok, Value} ->
		    equal(Rest, D1, D2);
		_ ->
		    false
	    end;
	_ ->
	    false
    end.

