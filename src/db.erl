-module(db).
-compile(export_all).

-record(data, {key, value}).

init() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(data, [{attributes, record_info(fields, data)}]).

clear() ->
    mnesia:clear_table(data).

insert(Key, Value) ->
    R = #data{key=Key, value=Value},
    mnesia:transaction(fun () ->
			       mnesia:write(R)
		       end).

lookup(Key) ->
    case mnesia:transaction(fun () ->
				    mnesia:read({data, Key})
			    end) of
	{atomic, [#data{value=Data}]} ->
	    {ok, Data};
	_ -> []
    end.

update(Key, Exist, DontExist) ->
    mnesia:transaction(fun () ->
			       case mnesia:wread({data, Key}) of
				   [#data{value=Data}] ->
				       New = Exist(Data),
				       mnesia:write(#data{key=Key, value=New});
				   _ ->
				       New = DontExist(),
				       mnesia:write(#data{key=Key, value=New})
			       end
		       end).

iterate(Function, Acc) ->			       
    {_, R} = mnesia:transaction(fun() -> mnesia:foldl(fun (#data{key=Key, value=Value}, Acc0) ->
							      Function({Key, Value}, Acc0)
						      end, Acc, data) end),
    R.

    
    
