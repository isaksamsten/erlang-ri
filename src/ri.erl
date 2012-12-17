-module(ri).
-compile(export_all).

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
    vector_addition(Ar, Br, [A + B|Acc]).

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


