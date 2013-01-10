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
run_experiment(Io, Cores, Collectors, Window, Length, Prob, Variance) ->
    catch stop(),
    init(),
    Result = ri_update:spawn_vector_update_processes(Cores, Collectors, 
						     Io, Window, #index_vector{length=Length, 
									       prob=Prob, 
									       variance=Variance}),
    Result.


%%
%% Entry for the command line interface
%%
start() ->
    case init:get_argument(h) of
	{ok, _} ->
	    show_help(),
	    halt();
	_ -> true
    end,
    Datafile = case init:get_argument(i) of
		   {ok, Files} ->
		       case Files of
			   [[File]] ->
			       File;
			   [_] ->
			       stdillegal("i"),
			       halt()
		       end;
		   _ ->
		       stdwarn("Input dataset is required"),
		       halt()
	       end,
    Window = case init:get_argument(w) of
		    {ok, Ws} ->
			case Ws of
			    [[W]] ->
				list_to_integer(W);
			    [_] ->
				stdillegal("w"),
				halt()
			end;
		    _ -> 
			2
	     end,
    Cores = case init:get_argument(c) of
		    {ok, Cs} ->
			case Cs of
			    [[C]] ->
				list_to_integer(C);
			    [_] ->
				stdillegal("c"),
				halt()
			end;
		    _ -> 
			erlang:system_info(schedulers)
	    end,
    Length = case init:get_argument(l) of
		 {ok, Ls} ->
		     case Ls of
			 [[L]] ->
			     list_to_integer(L);
			 [_] ->
			     stdillegal("l"),
			     halt()
		     end;
		 _ -> 
		     4000
	     end,
    Prob = case init:get_argument(p) of
	       {ok, Ps} ->
		   case Ps of
		       [[P]] ->
			   list_to_integer(P);
		       [_] ->
			   stdillegal("p"),
			   halt()
		   end;
	       _ -> 
		   7
	   end,
    Variance = case init:get_argument(v) of
		   {ok, Vs} ->
		       case Vs of
			   [[V]] ->
			       list_to_integer(V);
			   [_] ->
			       stdillegal("v"),
			       halt()
		       end;
		   _ -> 
		       0
	       end,
    SemanticOutput = case init:get_argument(om) of
			 {ok, Os} ->
			     case Os of
				 [[O]] ->
				     {ok, O};
				 [_] ->
				     stdillegal("om"),
				     halt()
			     end;
			 _ -> 
			     error
		     end,
    IndexOutput = case init:get_argument(oi) of
			 {ok, IOs} ->
			     case IOs of
				 [[IO]] ->
				     {ok, IO};
				 [_] ->
				     stdillegal("oi"),
				     halt()
			     end;
			 _ -> 
			  error
		     end,
    if
	Variance >= Prob ->
	    stdillegal("v"), halt();
	true -> ok
    end,

    io:format(standard_error, "*** Running '~p' on ~p core(s) *** ~n", [Datafile, Cores]),
    io:format(standard_error, "*** Sliding window: ~p, Index vector: ~p, Non zero bits: ~p+-~p *** ~n",
	      [Window, Length, Prob, Variance]),

    Then = now(),    
    Result = run(Datafile, Cores, 2, Window, Length, Prob, Variance),
    io:format(standard_error, "*** Calculated ~p semantic vectors in ~p second(s)*** ~n", 
	      [dict:size(Result), timer:now_diff(now(), Then) / 1000000]),
    case SemanticOutput of
	{ok, OutputFile} ->
	    io:format(standard_error, "*** Writing model to '~s' *** ~n", [OutputFile]),
	    ri_util:write_model_to_file(OutputFile, Result);
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

-w   [number]
     The number of items in each side of the sliding window (default: 2)

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
