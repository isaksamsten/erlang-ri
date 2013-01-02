-module(ri).
-compile(export_all).

init() ->
    ets:new(index_vectors, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ets:new(semantic_vectors, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ok.

stop() ->
    ets:delete(index_vectors),
    ets:delete(semantic_vectors),
    ok.

%%
%% Run a file
%%
run(file, File, Cores, Window, Length, Prob) ->
    Items = csv:parse(file, File),
    spawn_runner(Cores, Items, Window, Length, Prob);

%%
%% Run a list of lists
%%
run(list, List, Cores, Window, Length, Prob) ->
    spawn_runner(Cores, List, Window, Length, Prob).


%%
%% Process that updates an item (concurrently)
%% Receives: {new, Items, Window, Length, Prob} or exit
%%
vector_update_process(Parent) ->
    receive
	{new, {Items, Window, Length, Prob}} ->
	    update_item(Items, Window, Length, Prob),
	    vector_update_process(Parent);
	exit ->
	    Parent ! {done, self(), Parent}
    end.

%%
%% Spawn a Cores number of "vector_update_process"
%% that can receive Items for processing
%%
spawn_runner(Cores, Items, Window, Length, Prob) ->
    Self = self(),
    Runners = [spawn(?MODULE, vector_update_process, [Self]) || _ <-  lists:seq(1, Cores)],
    run_vector_update_processes(queue:from_list(Runners), Items, Window, Length, Prob),
    wait_for_vector_updates(Self, Cores).

wait_for_vector_updates(Self, Cores) ->
    if Cores == 0 ->
	    ok;
       true ->
	    receive
		{done, _, Self} ->
		    wait_for_vector_updates(Self, Cores - 1);
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
    run_vector_update_processes(queue:in(R, Queue), Rest, Window, Length, Prob).
   

%%
%% Update Items (a list of Items)
%%
update(Items, Window, Length, Prob) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    update_item(Item, Window, Length, Prob),
	    update(Rest, Window, Length, Prob)
    end.


%%
%% Update an item, using a window length of Window
%% a random index vector or Length
%%
update_item(Items, Window, Length, Prob) ->
    update_item(Items, Window, Length, Prob, queue:new()).
update_item(Items, Window, Length, Prob, Queue) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    update_all(queue:to_list(Queue), Length, Prob, Item),
	    update_limit({Window, 0}, Rest, Length, Prob, Item),
	    update_item(Rest, Window, Length, Prob, case queue:len(Queue) >= Window of
							true->
							    {_, Old} = queue:out(Queue),
							    queue:in(Item, Old);
							false ->
							    queue:in(Item, Queue)
						    end)
    end.

%%
%% Update Pivot with all items in Items
%%
update_all(Items, Length, Prob, Pivot) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    update_pivot(Pivot, Item, Length, Prob),
	    update_all(Rest, Length, Prob, Pivot)
    end.

%% 
%% Update Pivot with Current to Limit number of items
%%
update_limit({Limit, Current}, Items, Length, Prob, Pivot) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    if Limit > Current ->
		    update_pivot(Pivot, Item, Length, Prob),
		    update_limit({Limit, Current + 1}, Rest, Length, Prob, Pivot);
	       true ->
		    ok
	    end
    end.

%%
%% Update Pivot w.r.t Item. If index vector for any of the two
%% does not exist, create it
%% 
update_pivot(Pivot, Item, Length, Prob) ->
    PivotVector = get_semantic_vector(Item, Length),
    ItemVector  = get_index_vector(Pivot, Length, Prob),
    ets:insert(semantic_vectors, {Pivot, vector_addition(PivotVector, ItemVector)}).

%%
%% Init a random vector of Length lenght and the Prob prob to
%% spawn -1 or 1
%%
get_index_vector(Item, Length, Prob) ->	    
    case ets:lookup(index_vectors, Item) of
	[{_, Vector}] ->
	    Vector;
	[] ->
	    Vector = new_index_vector(Length, Prob),
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

%%
%% Get semantic vector for item
%%
get_semantic_vector(Item) ->
    case ets:lookup(semantic_vectors, Item) of
	[{_, Vector}] ->
	    Vector;
	[] -> throw({error, item_not_found})
    end.

%%
%% Create a new semantic vector
%%
new_semantic_vector(Length) ->   
    [0 || _ <- lists:seq(1, Length)].
    
%%
%% Create new index vectors for Items in List
%%
new_index_vectors(List, Length, Prob) ->
    case List of
	[] ->
	    ok;
	[Item|Rest] ->
	    ets:insert(index_vectors, {Item, new_index_vector(Length, Prob)}),
	    new_index_vectors(Rest, Length, Prob)
    end.

%%
%% Create a new index vector of length Length
%%
new_index_vector(Length, Prob) ->
    new_index_vector(Length, Prob, []).

new_index_vector(0, _, Acc) ->
    lists:reverse(Acc);
new_index_vector(Length, Prob, Acc) ->
    new_index_vector(Length - 1, Prob, [random_index(Prob) | Acc]).

%%
%% Return -1, 1 or 0
%%
random_index(Prob) ->
    X = random:uniform(),
    if
       X >= 1 - Prob ->
	    1;
       X =< Prob ->
	    -1;
       true -> 0
    end.

%%
%% Add two vectors
%%
vector_addition(A, B) ->
    vector_addition(A, B, []).

vector_addition([], [], Acc) ->
    lists:reverse(Acc);
vector_addition([], _, _) ->
    throw({not_same_magnitude});
vector_addition(_, [], _) ->
    throw({not_same_magnitude});
vector_addition([A|Ar], [B|Br], Acc) ->
    vector_addition(Ar, Br, [A + if 
				     B > 0 -> 1;
				     B < 0 -> -1;
				     true -> 0						  
				 end | Acc]).

%%
%% Get the cosine similarity between two vectors
%%
cosine_similarity(A, B) ->
    cosine_similarity(A, B, {0, 0, 0}).

cosine_similarity([], [], {Dot, A, B}) ->
    Dot / (math:sqrt(A) * math:sqrt(B));
cosine_similarity([A|Ar], [B|Br], Acc) ->
    {Dot, LenA, LenB} = Acc,
    NewDot = Dot + (A * B),
    NewLenA = LenA + A * A,
    NewLenB = LenB + B * B,
    cosine_similarity(Ar, Br, {NewDot, NewLenA, NewLenB}).


%%
%% Compare two semantic vectors
%%
compare_items(A, B) ->
    SemanticVectorA = get_semantic_vector(A),
    SemanticVectorB = get_semantic_vector(B),
    cosine_similarity(SemanticVectorA, SemanticVectorB).
