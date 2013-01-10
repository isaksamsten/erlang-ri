-module(ri_similarity).

-export([cosine/2,
	 dot_product/2,
	 magnitude/1,
	 similarity/3,
	 similarity/4,
	 similar_to/3,
	 concurrent_similar_to/4,
	 similarity_calculation_process/5]).

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
similarity(A, B, Fun, Vectors) ->
    {ok, SemanticVectorA} = ri_util:get_semantic_vector(A, Vectors),
    {ok, SemanticVectorB} = ri_util:get_semantic_vector(B, Vectors),
    Fun(SemanticVectorA, SemanticVectorB).

%%
%% Compare two semantic vectors using cosine similarity
%%
similarity(A, B, Vectors) ->
    similarity(A, B, fun cosine/2, Vectors).

%%
%% Get items similar to A
%%
similar_to(A, {Min, Max}, Vectors) when is_number(Min), 
					is_number(Max)->
    similar_to(A, fun (_, Similarity) ->
			  (Similarity =< Max) and (Similarity > Min)
		  end, Vectors);
similar_to(A, Fun, Vectors) when is_function(Fun)->
    concurrent_similar_to(A, Fun, fun cosine/2, Vectors);
similar_to(A, {Fun, Not}, Vectors) ->
    concurrent_similar_to(A, Fun, 
			  fun (VectorA, VectorB) ->
				  cosine(VectorA, VectorB) -
				      case dict:find(Not, Vectors) of
					  {ok, VectorC} ->
					      cosine(VectorC, VectorB);
					  error ->
					      1
				      end
			  end, Vectors);
similar_to(A, Not, Vectors) ->
    similar_to(A, { fun (_, _) ->
			    true
		    end, Not}, Vectors).

%%
%% Calculate the similarity between A and all other Items
%% Fun -> fun (Word, Similarity) -> boolean()
%% Sim -> fun (VectorA, VectorB) -> float()
%%
concurrent_similar_to(A, Fun, SimFun, Vectors) ->
    case dict:find(A, Vectors) of
	{ok, VectorA} ->
	    Self = self(),
	    Cores = erlang:system_info(schedulers),
	    Runners = [spawn_link(?MODULE, similarity_calculation_process, [Self, VectorA, Fun, SimFun, []]) || 
			  _ <- lists:seq(1, Cores)],
	    distribute_similarity_collections(queue:from_list(Runners), Self, dict:erase(A, Vectors)),
	    collect_similarity_processes(Self, Cores, []);
	error ->
	    not_found
    end.

%%
%% Distribute the similarity calculations evenly over all
%% availible processor
%%
distribute_similarity_collections(Queue0, Self, Vectors) ->
    dict:fold(fun (Word, Vector, Queue) ->
		       {{value, R}, Rest} = queue:out(Queue),
		       R ! {calculate, Self, R, {Word, Vector}},
		       queue:in(R, Rest)
	       end, Queue0, Vectors),
    lists:foreach(fun (R) ->
			  R ! {done, calculate}
		  end, queue:to_list(Queue0)).

%%
%% Calculate the similarity between VectorA and another vector
%% received, {calculate, _, _, {Word, VectorB}}, using Fun
%%
similarity_calculation_process(Parent, VectorA, Fun, SimFun, Acc) -> 
    Self = self(),
    receive
	{calculate, Parent, Self, {WordB, VectorB}} ->
	    Similarity = SimFun(VectorA, VectorB),
	    case Fun(WordB, Similarity) of
		true ->
		    similarity_calculation_process(Parent, VectorA, Fun, SimFun, [{WordB, Similarity}|Acc]);		
		false ->
		    similarity_calculation_process(Parent, VectorA, Fun, SimFun, Acc)
	    end;		    
	{done, calculate} ->
	    Parent ! {similarity, Parent, Self, Acc}
    end.

%%
%% Collect the result from calculating the similarites.
%%
collect_similarity_processes(Self, Cores, Acc) ->
    case Cores of
	0 -> lists:reverse(lists:keysort(2, Acc));
	_ -> 
	    receive
		{similarity, Self, _, Similarity} ->
		    collect_similarity_processes(Self, Cores - 1, Similarity ++ Acc)
	    end
    end.



	
