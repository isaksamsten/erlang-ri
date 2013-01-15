-module(ri_similarity).
-include("ri.hrl").
-export([cosine/2,
	 euclidian/2,
	 minkowski/3,
	 dot_product/2,
	 magnitude/1,
	 cmp/3,
	 cmp/4,
	 to/3,
	 to/4,
	 concurrent_similar_to/4,
	 similarity_calculation_process/5]).

euclidian(#semantic_vector{length=Length, values=A},
	  #semantic_vector{length=Length, values=B}) ->
    math:sqrt(lists:foldl(fun(Index, Acc) ->
				  ValueA = case dict:find(Index, A) of
					       {ok, ValueA0} ->
						   ValueA0;
					       error ->
						   0
					   end,
				  ValueB = case dict:find(Index, B) of
					       {ok, ValueB0} ->
						   ValueB0;
					       error ->
						   0
					   end,
				  Acc + math:pow(ValueA - ValueB, 2)
			  end, 0, dict:fetch_keys(A) ++ dict:fetch_keys(B))).

minkowski(#semantic_vector{length=Length, values=A},
	  #semantic_vector{length=Length, values=B}, P) ->
    math:pow(lists:foldl(fun(Index, Acc) ->
				 ValueA = case dict:find(Index, A) of
					      {ok, ValueA0} ->
						  ValueA0;
					      error ->
						  0
					  end,
				 ValueB = case dict:find(Index, B) of
					      {ok, ValueB0} ->
						  ValueB0;
					      error ->
						  0
					  end,
				 Acc + math:pow(abs(ValueA - ValueB), P)
			 end, 0, dict:fetch_keys(A) ++ dict:fetch_keys(B)), 1/P).
				 
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

magnitude(#semantic_vector{values=Vector}) ->		      
    math:sqrt(dict:fold(fun (_, Value, Acc) ->
				Acc + (Value * Value)
			end, 0, Vector)).
 
%%
%% Compare two semantic vectors
%%
cmp(A, B, Fun, Vectors) ->
    {ok, SemanticVectorA} = ri_util:get_semantic_vector(A, Vectors),
    {ok, SemanticVectorB} = ri_util:get_semantic_vector(B, Vectors),
    Fun(SemanticVectorA, SemanticVectorB).

%%
%% Compare two semantic vectors using cosine similarity
%%
cmp(A, B, Vectors) ->
    cmp(A, B, fun cosine/2, Vectors).

%%
%% Get items similar to A
%%
to(A, Min, Vectors) when is_number(Min) ->
    to(A, fun (_, Similarity) ->
			  Similarity > Min
		  end, Vectors);
to(A, Fun, Vectors) when is_function(Fun)->
    concurrent_similar_to(A, Fun, fun cosine/2, Vectors);
to(A, {Fun, Not}, Vectors) ->
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
to(A, Not, Vectors) ->
    to(A, { fun (_, _) ->
			    true
		    end, Not}, Vectors).

to(A, Max, Sim, Vectors) ->
    lists:sublist(concurrent_similar_to(A, fun(_,_) -> true end, Sim, Vectors), Max).
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



	
