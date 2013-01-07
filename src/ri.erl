-module(ri).
-compile(export_all).

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
vector_update_process(Parent, Result) ->
    receive
	{new, {Item, Window, Length, Prob}} ->
	    Result0 = update_item(Result, Item, Window, Length, Prob),
	    vector_update_process(Parent, Result0);
	exit ->
	    Parent ! {done, self(), Parent, Result}
    end.

%%
%% Spawn a Cores number of "vector_update_process"
%% that can receive Items for processing
%%
spawn_vector_update_processes(Cores, Items, Window, Length, Prob) ->
    Self = self(),
    Runners = [spawn_link(?MODULE, vector_update_process, [Self, dict:new()]) || _ <-  lists:seq(1, Cores)],
    run_vector_update_processes(queue:from_list(Runners), Items, Window, Length, Prob),
    wait_for_vector_updates(Self, Cores, dict:new()).

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
		    Result0 = dict:merge(fun (_, A, B) ->
						 merge_semantic_vector(A, B)
					 end, Result, Dict),
		    io:format(standard_error, "Merging vectors from ~p in ~p second(s) ~n", 
			      [Pid, timer:now_diff(erlang:now(), Then) / 1000000]),
		    wait_for_vector_updates(Self, Cores - 1, Result0);
		_ -> throw({error, some_error})
	    end
    end.

%%
%% Run each Item in Items in a separate process (from Runners).
%% When length(Items) > length(Runners) each process handle more items
%% in a "circle"
%%
run_vector_update_processes(Runners, [], _, _, _) ->
    [R ! exit || R <- queue:to_list(Runners)];
run_vector_update_processes(Runners, [Item|Rest], Window, Length, Prob) ->
    {{value, R}, Queue} = queue:out(Runners),
    R ! {new, {Item, Window, Length, Prob}},
    run_vector_update_processes(queue:in(R, Queue), Rest, Window, Length, Prob);
run_vector_update_processes(Runners, {csv, Pid} = Items, Window, Length, Prob) ->
    {{value, R}, Queue} = queue:out(Runners),
    case csv:get_next_line(Pid) of
	{ok, Item} ->
	    R ! {new, {Item, Window, Length, Prob}},
	    run_vector_update_processes(queue:in(R, Queue), Items, Window, Length, Prob);
	eof ->
	    [Process ! exit || Process <- queue:to_list(Runners)]
    end.

   
%%
%% Update Items (a list of Items)
%%
update(Parent, Items, Window, Length, Prob) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    update_item(Parent, Item, Window, Length, Prob),
	    update(Parent, Rest, Window, Length, Prob)
    end.


%%
%% Update an item, using a window length of Window
%% a random index vector or Length
%%
update_item(Result, Items, Window, Length, Prob) ->
    update_item(Result, Items, Window, Length, Prob, queue:new()).
update_item(Result, Items, Window, Length, Prob, Queue) ->
    case Items of
	[] ->
	    Result;
	[Pivot|Rest] ->
	    Result0 = update_all(Result, queue:to_list(Queue), Length, Prob, Pivot),
	    Result1 = update_limit(Result0, {Window, 0}, Rest, Length, Prob, Pivot),
	    update_item(Result1, Rest, Window, Length, Prob, case queue:len(Queue) >= Window of
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
update_all(Result, Items, Length, Prob, Pivot) ->
    case Items of
	[] ->
	    Result;
	[Item|Rest] ->
	    %Parent ! {update, self(), Parent, {Pivot, Item, Length, Prob}},
	    Result0 = update_pivot(Result, Pivot, Item, Length, Prob),
	    update_all(Result0, Rest, Length, Prob, Pivot)
    end.

%% 
%% Update Pivot with Current to Limit number of items
%%
update_limit(Result, {Limit, Current}, Items, Length, Prob, Pivot) ->
    case Items of
	[] ->
	    Result;
	[Item|Rest] ->
	    if Limit > Current ->
		    %Parent ! {update, self(), Parent, {Pivot, Item, Length, Prob}},
		    Result0 = update_pivot(Result, Pivot, Item, Length, Prob),
		    update_limit(Result0, {Limit, Current + 1}, Rest, Length, Prob, Pivot);
	       true ->
		    Result
	    end
    end.

%%
%% Update Pivot (semantic vector) w.r.t. Item (index vector), do this in current thread.
%%
update_pivot(Result, Pivot, Item, Length, Prob) ->
    IndexVector  = get_index_vector(Item, Length, Prob),
    dict:update(Pivot, fun(PivotVector) ->
			       add_vectors(PivotVector, IndexVector)
		       end, new_semantic_vector(Length), Result).

%%
%% Init a random vector of Length lenght and the Prob prob to
%% spawn -1 or 1
%%
get_index_vector(Item, Length, Prob) ->	    
    case ets:lookup(index_vectors, Item) of
	[{_, Vector}] ->
	    Vector;
	[] ->
	    Vector = new_index_vector(Length, Prob, 0),
	    ets:insert(index_vectors, {Item, Vector}),
	    Vector
    end.

get_semantic_vector(Item, Vectors) ->
    case dict:find(Item, Vectors) of
	{ok, Vector} ->
	    {ok, Vector};
	error ->
	    not_found
    end.

%%
%% Create a new semantic vector
%%
new_semantic_vector(_Length) ->   
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
%% Run a file
%%
run(file, File, Cores, Window, Length, Prob) ->
    io:format(standard_error, "*** Running '~p' on ~p core(s) *** ~n", [File, Cores]),
    Then = now(),
    Items = csv:parse(file, File),
    io:format("Reading file took: ~p ~n", [timer:now_diff(now(), Then) / 1000000]),
    run_experiment(Items, Cores, Window, Length, Prob);

run(incremental, File, Cores, Window, Length, Prob) ->
    io:format(standard_error, "*** Running '~p' on ~p core(s) *** ~n", [File, Cores]),
    Pid = spawn_link(csv, spawn_parser, [File]),
    run_experiment({csv, Pid}, Cores, Window, Length, Prob).
    

%%
%% Run a list of lists
%%
run_experiment(Items, Cores, Window, Length, Prob) ->
    catch stop(),
    init(),
    Then = now(),
    Result = spawn_vector_update_processes(Cores, Items, Window, Length, Prob),
    io:format(standard_error, "Updating vectors took: ~p ~n", [timer:now_diff(now(), Then) / 1000000]),
    Result.

test() ->
    Vector0 = new_semantic_vector(1000),
    _Vector1 = new_semantic_vector(1000),
    _Vector2 = new_semantic_vector(1000),

    Index0 = new_index_vector(1000, 7, 0),
    Index1 = new_index_vector(1000, 7, 0),
    _Index2 = new_index_vector(1000, 7, 0),

    Vi0 = add_vectors(Vector0, Index0),
    Vi1 = add_vectors(Vi0, Index1),
    
    Vi2 = add_vectors(Vector0, Index0),
    Vi3 = add_vectors(Vector0, Index1),

    equal(Vi0, Vi1),
    equal(Vi1, merge_semantic_vector(Vi2, Vi3)).
    

merge_semantic_vectors([Head|Rest]) ->
    lists:foldl(fun (Vector, Acc) ->
			dict:merge(fun (_, A, B) ->
					   merge_semantic_vector(A, B)
				   end, Vector, Acc)
		end, Head, Rest).

merge_semantic_vector(VectorA, VectorB) ->
    dict:merge(fun (_Key, ValueA, ValueB) ->
		       ValueA + ValueB
	       end, VectorA, VectorB).

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

