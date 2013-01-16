-module(ri).
-compile(export_all).

-define(DATA, "2013-01-07").
-define(MAJOR_VERSION, 0).
-define(MINOR_VERSION, 1).
-define(REVISION, 'beta-3').

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

cmd_spec() ->
    [{input_file,  $i,             "input",   string, 
      "Input file consisting of one document per line"},
     {output_model,   undefined,   "model",   undefined,
      "Write semantic vectors to file"},
     {output_index,   undefined,   "index",   undefined,
      "Write index vectors to file"},
     {output_reduced, undefined,   "dataset", undefined,
      "Write a reduced data set to file"},
     {help,           undefined,   "help",    undefined,
      "Show this text"},
     {version,        undefined,   "version", undefined,
      "Show the application version"},
     {window,         $w,          "window",  {integer, 2},
      "Set the size of the sliding window"},
     {reduce,         $r,          "reduce",  {boolean, false},
      "Use the items as the index vector for the document"},
     {item,           undefined,   "item",    {boolean, false},
      "Use the document as index vector for each item"},
     {cores,          $c,          "cores",   {integer, erlang:system_info(schedulers)},
      "Number of parallell executions"},
     {length,         $l,          "length",  {integer, 4000},
      "Length of the index vector"},
     {prob,           $p,          undefined, {integer, 7},
      "Number of non-negative bits in index vector"},
     {variance,       $v,          undefined, {integer, 0},
      "Variance in the number of non-negative bits."},
     {output,         $o,          "output",  string,
      "Output file"}].

%%
%% Running a file of documents
%%
run(File, Cores, Window, Length, Prob, Variance) ->
    Pid = csv:reader(File),
    run_experiment(Pid, Cores,  Window, Length, Prob, Variance).
    

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
main(Args) ->
    Illegal = fun() ->
		      getopt:usage(cmd_spec(), "ri"),
		      halt()
	      end,
    
    Options = case getopt:parse(cmd_spec(), Args) of
		  {ok, Parsed} -> 
		      Parsed;
		  {error, _} ->
		      Illegal()		      
	      end,

    case getsopt(help, Options) of 
	true -> Illegal(); 
	false -> ok 
    end,
    case getsopt(version, Options) of 
	true -> io:format("~s", [show_information()]), halt();
	_ -> ok end,
    InputFile = getopt(input_file, Illegal, Options),
    Window = getopt(window, Illegal, Options),
    Cores = getopt(cores, Illegal, Options),
    Length = getopt(length, Illegal, Options),
    Prob = getopt(prob, Illegal, Options),
    Variance = getopt(variance, Illegal, Options),

    Outputs = try 
		  merge_opts(getopts([output_model, output_index, output_reduced], Options), Options)
	      catch
		  _:_ ->
		      Illegal()
	      end,
    if
	Variance >= Prob ->
	    Illegal();
	true -> ok
    end,

    io:format(standard_error, "*** Running '~p' on ~p core(s) *** ~n", [InputFile, Cores]),
    io:format(standard_error, "*** Sliding window: ~p, Index vector: ~p, Non zero bits: ~p+-~p *** ~n",
	      [Window, Length, Prob, Variance]),

    Then = now(),    
    Result = run(InputFile, Cores, Window, Length, Prob, Variance),
    io:format(standard_error, "*** Calculated ~p semantic vectors in ~p second(s)*** ~n", 
	      [dict:size(Result), timer:now_diff(now(), Then) / 1000000]),


    write_files(Outputs, Length, Result),
    halt().

write_files([], _, _) ->
    ok;
write_files([{output_model, File}|Rest], Length, Result) ->
    io:format(standard_error, "*** Writing model to '~s' *** ~n", [File]),
    ri_util:write_model_to_file(File, Length, Result),
    write_files(Rest, Length, Result);
write_files([{output_index, File}|Rest], L, R) ->
    io:format(standard_error, "*** Writing index vectors to '~s' *** ~n", [File]),
    ri_util:write_index_to_file(File),
    write_files(Rest, L, R).

    

getopt(Arg, Fun1, {Options, _}) ->	
    case lists:keyfind(Arg, 1, Options) of
	{Arg, Ws} ->
	    Ws;
	false -> 
	    Fun1()
    end.

getsopt(Arg, {Options, _ }) ->
    lists:any(fun (K) ->
		      K == Arg
	      end, Options).

getopt(Arg, {Options,_}) ->
    case lists:keyfind(Arg, 1, Options) of
	{Arg, Ws} ->
	    Ws;
	false ->
	    false
    end.

getopts(Match, {Options, _}) ->
    Match0 = sets:from_list(Match),
    lists:filter(fun (Option) ->
			 sets:is_element(Option, Match0)
		 end, Options).

merge_opts(Matches, {Options, _}) ->
    lists:zip(Matches, lists:reverse(lists:foldl(fun (O, Acc) ->
							 case O of
							     {output, File} ->
								 [File|Acc];
							     _ -> Acc
							 end
						 end, [], Options))).

show_information() ->
    io_lib:format("Random index, Version (of ~s) ~p.~p.~s
All rights reserved ~s", [?DATA, ?MAJOR_VERSION, ?MINOR_VERSION, ?REVISION, ?AUTHOR]).
