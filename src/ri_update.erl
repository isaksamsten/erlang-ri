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

vector_update_process(Parent, #ri_conf{file=Io, 
				       window=Window, 
				       class=ClassIdx,
				       unique=Unique} = RiConf, IndexVector, Result) ->
    case csv:get_next_line(Io) of
	{ok, Item, Line} ->
	    Result0 = case Window of
			  {window, WindowSize} ->
			      update_item(Result, Item, WindowSize, IndexVector);
			  {before, WindowSize} ->
			      update_item_before(Result, Item, WindowSize, IndexVector);
			  reduce ->
			      Item0 = if Unique ->
					      ri_util:unique_keep_order(Item);
					 true ->
					      Item
				      end,
			      case ClassIdx of
				  undefined -> %% unsupervised?
				      update_all(Result, Item0, IndexVector, Line);
				  _ ->
				      {Class, Rest} = ri_util:take_nth(Item0, ClassIdx),
				      update_all_class(Result, Rest, IndexVector, Line, Class)
			      end;
			  item ->
			      update_all_with(Result, Line, IndexVector, Item)
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
	    Result = spawn_vector_update_processes(RiConf#ri_conf{cores=Childrens}, IndexVector),
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
wait_for_vector_updates(_, 0, Result) ->
    Result;
wait_for_vector_updates(Self, Cores, Result) ->
    receive
	{done, _Pid, Self, Dict} ->
	    Result0 = ri_vector:merge_semantic_vectors(Result, Dict),
	    wait_for_vector_updates(Self, Cores - 1, Result0);
	{'EXIT', _, normal} ->
	    wait_for_vector_updates(Self, Cores, Result);			
	X -> throw({error, some_error, X})
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

update_item_before(Result, Items, Window, IndexVector) ->
    update_item_before(Result, Items, Window, IndexVector, queue:new()).

update_item_before(Result, [], _Conf, _Iv, _Q) ->
    Result;
update_item_before(Result, [Pivot|Rest], Window, IndexVector, Queue) ->
    Result1 = update_all(Result, queue:to_list(Queue), IndexVector, Pivot),
    update_item_before(Result1, Rest, Window, IndexVector, case queue:len(Queue) >= Window of
							       true->
								   {_, Old} = queue:out(Queue),
								   queue:in(Pivot, Old);
							       false ->
								   queue:in(Pivot, Queue)
							   end).


update_all_class(Result, [], #index_vector{length=L}, Pivot, Class) ->
    dict:store(Pivot, ri_vector:new_semantic_vector(Class, L), Result);
update_all_class(Result, Items, IndexVector, Pivot, Class) ->
    lists:foldl(fun (Item, Result0) ->
			update_pivot(Result0, Pivot, Item, IndexVector, Class)
		end, Result, Items).


%%
%% Update Pivot with all items in Items
%%
update_all(Result, Items, IndexVector, Pivot) ->
    lists:foldl(fun (Item, Result0) ->
			update_pivot(Result0, Pivot, Item, IndexVector)
		end, Result, Items).

%%
%% Update all items in Pivots with the index vector of
%% Item
%%
update_all_with(Result, Item, IndexVector, Pivots) ->
    lists:foldl(fun (Pivot, Result0) ->
			update_pivot(Result0, Pivot, Item, IndexVector)
		end, Result, Pivots).

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
update_pivot(Result, Pivot, Item, #index_vector{length=Length} = IndexVectorInfo, Class) ->
    IndexVector = ri_vector:get_index_vector(Item, IndexVectorInfo),
    dict:update(Pivot, fun(PivotVector) ->
			       ri_vector:add(PivotVector, IndexVector)
		       end, ri_vector:new_semantic_vector(Class, Length), Result).

update_pivot(Result, Pivot, Item, IndexVector) ->
    update_pivot(Result, Pivot, Item, IndexVector, undefined).
