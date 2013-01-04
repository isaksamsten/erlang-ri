-module(lock).
-compile(export_all).

init() ->
    Pid = spawn(?MODULE, init, [dict:new()]),
    register(locker, Pid).

close() ->
    locker ! exit.


init(Dict) ->    
    receive 
	{lock, Locker, Item} ->
	    try_lock(Locker, Item),
	    Locker ! locked,
	    init(dict:store(Item, Locker, Dict));
	{unlock, Locker, Item} ->
	    Locker ! unlocked,
	    init(dict:erase(Item, Dict));
	exit ->
	    ok
    end.

try_lock(Locker, Item) ->
    case 
