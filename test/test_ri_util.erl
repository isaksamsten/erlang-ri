-module(test_ri_util).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("ri.hrl").

unique_keep_order_test() ->
    List0 = [a, b, c, c, d, e, e],
    List1 = [a, b, c, d, e],
    
    ?assertEqual(List1, ri_util:unique_keep_order(List0)).
    
-endif.
