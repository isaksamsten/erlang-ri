test() ->
    random:seed(now()),
    Vector0 = new_semantic_vector(),
    _Vector1 = new_semantic_vector(),
    _Vector2 = new_semantic_vector(),

    Index0 = new_index_vector(1000000, 7, 2),
    Index1 = new_index_vector(1000000, 7, 2),
    Index2 = new_index_vector(1000000, 7, 2),

    Vi0 = add_vectors(Vector0, Index0),
    Vi1 = add_vectors(Vi0, Index1),
    Vi2 = add_vectors(Vi1, Index2),
    
    Vi3 = add_vectors(Vector0, Index0),
    Vi4 = add_vectors(Vector0, Index1),

    equal(Vi0, Vi1),
    equal(Vi2, merge_semantic_vector(merge_semantic_vector(Vi3, Vi4), add_vectors(Vector0, Index2))).
    


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
