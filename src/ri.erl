-module(ri).
-compile(export_all).

%%
%% Initialize the ets-tables (index_vectors and semantic_vectors)
%%
init() ->
    ets:new(index_vectors, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ets:new(semantic_vectors, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ok.

%%
%% Destruct the ets-tables
%%
stop() ->
    ets:delete(index_vectors),
    ets:delete(semantic_vectors),
    ok.

%%
%% Process that updates an item (concurrently)
%% Receives: {new, Items, Window, Length, Prob} or exit
%%
vector_update_process(Parent) ->
    receive
	{new, {Item, Window, Length, Prob}} ->
	    update_item(Parent, Item, Window, Length, Prob),
	    vector_update_process(Parent);
	exit ->
	    Parent ! {done, self(), Parent}
    end.

%%
%% Spawn a Cores number of "vector_update_process"
%% that can receive Items for processing
%%
spawn_vector_update_processes(Cores, Items, Window, Length, Prob) ->
    Self = self(),
    Runners = [spawn(?MODULE, vector_update_process, [Self]) || _ <-  lists:seq(1, Cores)],
    run_vector_update_processes(queue:from_list(Runners), Items, Window, Length, Prob),
    wait_for_vector_updates(Self, Cores).

%%
%% Wait for Cores messages to be sent to Self in the form of: {done,
%% Pid, Self}
%%
wait_for_vector_updates(Self, Cores) ->
    if Cores == 0 ->
	    ok;
       true ->
	    receive
		{done, _, Self} ->
		    wait_for_vector_updates(Self, Cores - 1);
		{update, _Pid, Self, {Pivot, Item, Length, Prob}} ->
		    update_pivot(Pivot, Item, Length, Prob),
		    wait_for_vector_updates(Self, Cores);
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
update_item(Parent, Items, Window, Length, Prob) ->
    update_item(Parent, Items, Window, Length, Prob, queue:new()).
update_item(Parent, Items, Window, Length, Prob, Queue) ->
    case Items of
	[] ->
	    ok;
	[Pivot|Rest] ->
	    update_all(Parent, queue:to_list(Queue), Length, Prob, Pivot),
	    update_limit(Parent, {Window, 0}, Rest, Length, Prob, Pivot),
	    update_item(Parent, Rest, Window, Length, Prob, case queue:len(Queue) >= Window of
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
update_all(Parent, Items, Length, Prob, Pivot) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    %Parent ! {update, self(), Parent, {Pivot, Item, Length, Prob}},
	    update_pivot_transaction(Pivot, Item, Length, Prob),
	    update_all(Parent, Rest, Length, Prob, Pivot)
    end.

%% 
%% Update Pivot with Current to Limit number of items
%%
update_limit(Parent, {Limit, Current}, Items, Length, Prob, Pivot) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    if Limit > Current ->
		    %Parent ! {update, self(), Parent, {Pivot, Item, Length, Prob}},
		    update_pivot_transaction(Pivot, Item, Length, Prob),
		    update_limit(Parent, {Limit, Current + 1}, Rest, Length, Prob, Pivot);
	       true ->
		    ok
	    end
    end.

%%
%% Update Pivot (semantic vector) w.r.t. Item (index vector), do this in current thread.
%%
update_pivot(Pivot, Item, Length, Prob) ->
    PivotVector = get_semantic_vector(Pivot, Length),
    IndexVector  = get_index_vector(Item, Length, Prob),
    ets:insert(semantic_vectors, {Pivot, add_vectors2(PivotVector, IndexVector)}).

%%
%% Update Pivot w.r.t Item. If index vector for any of the two
%% does not exist, create it. Do this in a transaction (mnesia)
%% 
update_pivot_transaction(Pivot, Item, Length, Prob) ->
    IndexVector  = get_index_vector(Item, Length, Prob),
    db:update(Pivot, 
	      fun (PivotVector) ->
		      add_vectors2(PivotVector, IndexVector)
	      end, 
	      fun () ->
		      PivotVector = new_semantic_vector(Length),
		      add_vectors2(PivotVector, IndexVector)
	      end).



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

%%
%% Get the semantic vector for Item (if dont exist, create)
%%
get_semantic_vector(Item, Length) ->
    case ets:lookup(semantic_vectors, Item) of
	[{_, Vector}] ->
	    Vector;
	[] ->
	    Vector = new_semantic_vector(Length),
	    ets:insert(semantic_vectors, {Item, Vector}),
	    Vector
    end.

get_semantic_vector_ets(Item) ->
    case ets:lookup(semantic_vectors, Item) of
	[{_, Vector}] ->
	    {ok, Vector};
	[] ->
	    not_found
    end.

%%
%% Get semantic vector for item
%%
get_semantic_vector(Item) ->
    case db:lookup(Item) of
	{ok, Vector} ->
	    {ok, Vector};
	[] -> not_found
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
add_vectors2(VectorA, VectorB) ->
    lists:foldl(fun ({Index, Value}, Acc) ->
			dict:update(Index, fun (Old) ->
						   Old + Value
					   end, Value, Acc)
		end, VectorA, VectorB).
			
    
cosine(A, B) ->
    NewDot = dict:fold(fun (Index, ValueA, Dot) ->
			  case dict:find(Index, B) of
			      {ok, ValueB} ->
				  Dot + (ValueA * ValueB);
			      error ->
				  Dot
			  end
		       end, 0, A),
    LenA = magnitude(A),
    LenB = magnitude(B),
    NewDot / (LenA * LenB).

magnitude(Vector) ->		      
    math:sqrt(dict:fold(fun (_, Value, Acc) ->
				Acc + (Value * Value)
			end, 0, Vector)).
 
%%
%% Compare two semantic vectors
%%
similarity(A, B) ->
    {ok, SemanticVectorA} = get_semantic_vector_ets(A),
    {ok, SemanticVectorB} = get_semantic_vector_ets(B),
    cosine(SemanticVectorA,SemanticVectorB).

%%
%% Get items similar to A
%%
similar_to(A, Threshold) ->
    case get_semantic_vector(A) of
	{ok, VectorA} ->
	    lists:reverse(ets:foldl(fun ({Word, VectorB}, Acc) ->
					    Similarity = cosine(tuple_to_list(VectorA), tuple_to_list(VectorB)),
					    if Similarity > Threshold, A /= Word ->
						    ordsets:add_element({Similarity, Word}, Acc);
					       true ->
						    Acc
					    end
				    end, ordsets:new(), semantic_vectors));
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
    spawn_vector_update_processes(Cores, Items, Window, Length, Prob),
    io:format(standard_error, "Updating vectors took: ~p ~n", [timer:now_diff(erlang:now(), Then) / 1000000]).

