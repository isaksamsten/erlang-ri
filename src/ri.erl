-module(ri).
-compile(export_all).

init() ->
    ets:new(index_vectors, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ets:new(attr_vectors, [public, named_table]),
    ok.

stop() ->
    ets:delete(index_vectors),
    ets:delete(attr_vectors),
    ok.


store_attr_vector(Id, Values) ->
    ets:insert(attr_vectors, {Id, Values}),
    ok.

load(table, File) ->
    csv:parse(file, File, fun store_attr_vector/2),
    ok;
load(list, File) ->
    csv:parse(file, File).
load(run, File, Fun) ->
    csv:parse(file, File, Fun),
    ok.

run(file, File, Cores, Window, Length, Prob) ->
    stop(),
    init(),

    Items = load(list, File),
    spawn_runner(Cores, Items, Window, Length, Prob);
run(list, List, Cores, Window, Length, Prob) ->
    init(),
    spawn_runner(Cores, List, Window, Length, Prob).


update_runner(Parent) ->
    receive
	{new, {Items, Window, Length, Prob}} ->
	    update_item(Items, Window, Length, Prob),
	    update_runner(Parent);
	exit ->
	    Parent ! {done, self(), Parent}
    end.

spawn_runner(Cores, Items, Window, Length, Prob) ->
    Self = self(),
    Runners = [spawn(?MODULE, update_runner, [Self]) || _ <-  lists:seq(1, Cores)],
    run_runners(queue:from_list(Runners), Items, Window, Length, Prob),
    is_done(Self, Cores).

is_done(Self, Cores) ->
    if Cores == 0 ->
	    ok;
       true ->
	    receive
		{done, _, Self} ->
		    is_done(Self, Cores - 1);
		_ -> throw({error, some_error})
	    end
    end.

run_runners(Runners, [], _, _, _) ->
    [R ! exit || R <- queue:to_list(Runners)];
run_runners(Runners, [Item|Rest], Window, Length, Prob) ->
    {{value, R}, Queue} = queue:out(Runners),
    R ! {new, {Item, Window, Length, Prob}},
    run_runners(queue:in_r(R, Queue), Rest, Window, Length, Prob).
    

    
    
    

update(Items, Window, Length, Prob) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    update_item(Item, Window, Length, Prob),
	    update(Rest, Window, Length, Prob)
    end.


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

update_all(Items, Length, Prob, Pivot) ->
    case Items of
	[] ->
	    ok;
	[Item|Rest] ->
	    update_pivot(Pivot, Item, Length, Prob),
	    update_all(Rest, Length, Prob, Pivot)
    end.

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


update_pivot(Pivot, Item, Length, Prob) ->
   % io:format("Updating: ~p with index vector of ~p ~n", [Pivot, Item]),
    PivotVector = init_random_vector(Item, Length, Prob),
    ItemVector  = init_random_vector(Pivot, Length, Prob),
    ets:insert(index_vectors, {Pivot, vector_addition(PivotVector, ItemVector)}).

init_random_vector(Item, Length, Prob) ->	    
    case ets:lookup(index_vectors, Item) of
	[{_, Vector}] ->
	    Vector;
	[] ->
	    Vector = random_index_vector(Length, Prob),
	    ets:insert(index_vectors, {Item, Vector}),
	    Vector
    end.
	    

    
random_vector(List, Length, Prob) ->
    case List of
	[] ->
	    ok;
	[Item|Rest] ->
	    ets:insert(index_vectors, {Item, random_index_vector(Length, Prob)}),
	    random_vector(Rest, Length, Prob)
    end.

random_index_vector(Length, Prob) ->
    random_index_vector(Length, Prob, []).

random_index_vector(0, _, Acc) ->
    lists:reverse(Acc);
random_index_vector(Length, Prob, Acc) ->
    random_index_vector(Length - 1, Prob, [random_index(Prob) | Acc]).

random_index(Prob) ->
    X = random:uniform(),
    if
       X >= 1 - Prob ->
	    1;
       X =< Prob ->
	    -1;
       true -> 0
    end.

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

vector_addition(A, B) ->
    vector_addition(A, B, []).

cosine_similarity([], [], {Dot, A, B}) ->
    Dot / (math:sqrt(A) * math:sqrt(B));
cosine_similarity([A|Ar], [B|Br], Acc) ->
    {Dot, LenA, LenB} = Acc,
    NewDot = Dot + (A * B),
    NewLenA = LenA + A * A,
    NewLenB = LenB + B * B,
    cosine_similarity(Ar, Br, {NewDot, NewLenA, NewLenB}).
cosine_similarity(A, B) ->
    cosine_similarity(A, B, {0, 0, 0}).


