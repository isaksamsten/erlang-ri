-module(ri).
-compile(export_all).

-define(DATA, "2013-01-07").
-define(MAJOR_VERSION, 0).
-define(MINOR_VERSION, 1).
-define(REVISION, 'beta-2').

-define(AUTHOR, "Isak Karlsson <isak-kar@dsv.su.se>").

-include("ri.hrl").

%%
%% Initialize the ets-tables (index_vectors and semantic_vectors)
%%
%% NOTE: two processes might write, and read, at the same time. This
%% must be fixed.
%%
init() ->
    ets:new(index_vectors, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ok.

%%
%% Destruct the ets-tables
%%
stop() ->
    ets:delete(index_vectors),
    ok.	 

%%
%% Running a file of documents
%%
run(File, Cores, Collectors, Window, Length, Prob, Variance) ->
    Pid = csv:reader(File),
    run_experiment(Pid, Cores, Collectors, Window, Length, Prob, Variance).
    

%%
%% Run a list of lists
%%
run_experiment(Io, Cores, Window, Length, Prob, Variance) ->
    catch stop(),
    init(),
    ri_update:spawn_vector_update_processes(#ri_conf{file=Io,
						     window=Window,
						     cores = Cores},
					    #index_vector{length=Length, 
							  prob=Prob, 
							  variance=Variance}).


%%
%% Entry for the command line interface
%%
start() ->
    Illegal = fun(Arg) -> stdillegal(Arg) end,
    Return = fun(V) -> V end,
    Default = fun(V) -> fun() -> V end end,
    Warn    = fun(V) -> fun() -> stdwarn(V) end end,

    case init:get_argument(h) of
	{ok, _} ->
	    show_help(),
	    halt();
	_ -> true
    end,

    InputFile = get_argument(i, Return, Illegal, Warn("Input file required")),
    Window   = get_argument(w, fun list_to_integer/1, Illegal, Default(2)),
    Cores    = get_argument(c, fun list_to_integer/1, Illegal, Default(erlang:system_info(schedulers))),
    Length   = get_argument(l, fun list_to_integer/1, Illegal, Default(4000)),
    Prob     = get_argument(p, fun list_to_integer/1, Illegal, Default(7)),
    Variance = get_argument(v, fun list_to_integer/1, Illegal, Default(0)),
    SemanticOutput = get_argument(om, fun(V) -> {ok, V} end, Illegal, Default(error)),
    IndexOutput = get_argument(om, fun(V) -> {ok, V} end, Illegal, Default(error)),


    if
	Variance >= Prob ->
	    stdillegal("v"), halt();
	true -> ok
    end,

    io:format(standard_error, "*** Running '~p' on ~p core(s) *** ~n", [InputFile, Cores]),
    io:format(standard_error, "*** Sliding window: ~p, Index vector: ~p, Non zero bits: ~p+-~p *** ~n",
	      [Window, Length, Prob, Variance]),

    Then = now(),    
    Result = run(InputFile, Cores, Window, Length, Prob, Variance),
    io:format(standard_error, "*** Calculated ~p semantic vectors in ~p second(s)*** ~n", 
	      [dict:size(Result), timer:now_diff(now(), Then) / 1000000]),
    case SemanticOutput of
	{ok, OutputFile} ->
	    io:format(standard_error, "*** Writing model to '~s' *** ~n", [OutputFile]),
	    ri_util:write_model_to_file(OutputFile, Length, Result);
       error ->
	    ok
    end,
    case IndexOutput of
	{ok, IOut} ->
	    io:format(standard_error, "*** Writing index vectors to '~s' *** ~n", [IOut]),
	    ri_util:write_index_to_file(IOut);
	error ->
	    ok
    end,
    halt().

get_argument(Arg, Fun0, Fun1, Fun2) ->	
    case init:get_argument(w) of
	{ok, Ws} ->
	    case Ws of
		[[W]] ->
		    Fun0(W);
		[_] ->
		    Fun1(Arg)
	    end;
	_ -> 
	    Fun2()
    end.

stdwarn(Out) ->
    io:format(standard_error, " **** ~s **** ~n", [Out]),
    io:format(standard_error, "See tr -h for options ~n", []).

stdillegal(Arg) ->
    stdwarn(io_lib:format("Error: Invalid argument(s) to -~s", [Arg])).

show_help() ->
    io:format(standard_error,"~s

Example: ri -i ../data/brown.txt -w 2 -c 4 -l 4000 -p 7 -v 2
         ri -i ../data/brown.txt -w 2
         ri -i ../data/brown.txt -w 2 -o > model.txt

-i   [input-doc]
     Input file. One document per line, words (tokens) are comma separated.

-im  [semantic-vectors-file, index-vectors-file]
     Read already calculated semantic vectors and index vectors from files.

-om  [file]
     Write semantic vectors to file (default: false)

-oi  [file]
     Write index vectors to file (default: false)

-is  Read a model for inspection (default: false)

-w   [number | dv | iv]
     'number': The number of items in each side of the sliding window
     'dv':     Document as index vector for each item in the document
     'iv':     Each item in a document as index vector for the document
     (default: 2)

-c   [cores]
     Number of parallell executions (default: ~p)

-l   [number]
     Length of the index vector (default: 4000)

-p   [number]
     Number of non negative bits in index vector (default: 7)

-v   [number]
     Variance in the number of non negative bits. For example, 
     setting -v to 2 gives 7 +- 2 non negative bits in index vector. Must
     be < -p (default: 0).
", [show_information(), erlang:system_info(schedulers)]).

show_information() ->
    io_lib:format("Random index, Version (of ~s) ~p.~p.~s
All rights reserved ~s", [?DATA, ?MAJOR_VERSION, ?MINOR_VERSION, ?REVISION, ?AUTHOR]).
