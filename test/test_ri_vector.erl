-module(test_ri_vector).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("ri.hrl").
 
update_vector_test() ->
    random:seed(now()),
    Length = 1000,
    {Vector0} =  get_semantic_vectors(Length, undefined, 1),
    {Index0, Index1, Index2} = get_index_vectors(Length, 7, 2, 3),


    Vi0 = ri_vector:add(Vector0, Index0),
    Vi1 = ri_vector:add(Vi0, Index1),
    Vi2 = ri_vector:add(Vi1, Index2),
    
    Vi3 = ri_vector:add(Vector0, Index0),
    Vi4 = ri_vector:add(Vector0, Index1),

    % Test equal
    ?assert(equal(Vi0, Vi1) =:= false),
    
    % Test merge semantic vector
    ?assert(equal(Vi2, ri_vector:merge_semantic_vector(ri_vector:merge_semantic_vector(Vi3, Vi4), 
						       ri_vector:add(Vector0, Index2))) =:= true).

dot_product_test() ->
    {A0, B0} = get_semantic_vectors(3, undefined, 2),
    
    A = ri_vector:add(A0, [{1, 1}, {2, 0}, {3, 1}]),
    B = ri_vector:add(B0, [{1, 0}, {2, 2}, {3, 3}]),
    
    ?assert(ri_vector:dot_product(A, B) =:= 3).

magnitude_test() ->
    {A0} = get_semantic_vectors(3, undefined, 1),
    A = ri_vector:add(A0, [{1,3}, {2, 2}, {3, 1}]),
    ?assert(ri_vector:magnitude(A) =:= 3.7416573867739413).


get_semantic_vectors(Length, Class, Num) ->
    list_to_tuple(lists:map(fun(_) ->
				    ri_vector:new_semantic_vector(Length, Class)
			    end, lists:seq(1, Num))).

get_index_vectors(Length, Prob, V, Num) ->
    list_to_tuple(lists:map(fun(_) ->
				    ri_vector:new_index_vector(Length, Prob, V)
			    end, lists:seq(1, Num))).



equal(#semantic_vector{values=D1}, #semantic_vector{values=D2}) ->
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

-endif.
