-module(ri).
-export([main/1]).

-define(DATE, "2013-02-28").
-define(MAJOR_VERSION, "1").
-define(MINOR_VERSION, "0").
-define(REVISION, "0").

-define(AUTHOR, "Isak Karlsson <isak-kar@dsv.su.se>").

-include("ri.hrl").

%%
%% Initialize the ets-tables (index_vectors and semantic_vectors)
%%
%% NOTE: two processes might write, and read, at the same time. This
%% must be fixed.
%%
init() ->
    ets:new(index_vectors, [public, named_table, 
			    {write_concurrency, true}, 
			    {read_concurrency, true}]),
    ok.

%%
%% Destruct the ets-tables
%%
stop() ->
    ets:delete(index_vectors),
    ok.	 

%%
%% Defines the spec() for the command line interface
%%
cmd_spec() ->
    [{input_file,     $i,          "input",   string, 
      "Input file consisting of one document per line"},
     {class_index,    undefined,   "class",   {integer, undefined},
      "Item index to use as class"},
     {ignore_index,   undefined,   "ignore",  {integer, undefined},
      "Ignore items with index"},
     {unique,         undefined,   "unique",  {boolean, false},
      "Unique all items in a document"},
     {load,           undefined,   "load",    undefined,
      "Load -i (--input) as a model"},
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
     {reduce,         $r,          "reduce",  undefined,
      "Use the items as the index vector for the document"},
     {item,           undefined,   "item",    undefined,
      "Use the document as index vector for each item"},
     {cores,          $c,          "cores",   {integer, erlang:system_info(schedulers)},
      "Number of parallell executions"},
     {length,         $l,          "length",  {integer, 4000},
      "Length of the index vector"},
     {prob,           $p,          undefined, {integer, 7},
      "Number of non-negative bits in index vector"},
     {rds,            undefined,   "rds",     {boolean, false},
      "Generate a RDS-header when using --reduce and --dataset"},
     {variance,       $v,          undefined, {integer, 0},
      "Variance in the number of non-negative bits."},
     {output,         $o,          "output",  string,
      "Output file"}].

%%
%% Running a file of documents
%%
run(File, Cores, Window, Length, Prob, Variance, Class, Unique) ->
    Pid = csv:reader(File),
    run_experiment(Pid, Cores,  Window, Length, Prob, Variance, Class, Unique).
    

%%
%% Run a list of lists
%%
run_experiment(Io, Cores, Window, Length, Prob, Variance, Class, Unique) ->
    catch stop(),
    init(),
    ri_update:spawn_vector_update_processes(#ri_conf{file=Io,
						     window=Window,
						     cores = Cores,
						     class=Class,
						     unique=Unique},
					    #index_vector{length=Length, 
							  prob=Prob, 
							  variance=Variance,
							  pid=ri_vector:index_vector_handler(Length, Prob, Variance)}).

%%
%% Halts the program if illegal arguments are supplied
%%
illegal() ->
    getopt:usage(cmd_spec(), "ri"),
    halt().

%%
%% Entry for the command line interface
%%
main(Args) ->
    Options = case getopt:parse(cmd_spec(), Args) of
		  {ok, Parsed} -> 
		      Parsed;
		  {error, _} ->
		      illegal()		      
	      end,

    case has_opt(help, Options) of 
	true -> illegal(); 
	false -> ok 
    end,
    case has_opt(version, Options) of 
	true -> io:format("~s", [show_information()]), halt();
	_ -> ok end,

    case has_opt(load, Options) of
	true ->
	    load_model(Options);
	false ->
	    generate_model(Options)
    end.
	    
load_model(Options) ->
    InputFile = get_opt(input_file, fun illegal/0, Options),
    ri_parser:run(InputFile, get_opt(cores, fun illegal/0, Options)).

generate_model(Options) ->
    InputFile = get_opt(input_file, fun illegal/0, Options),
    Window = case has_opt(reduce, Options) of
		 true -> reduce;
		 false -> case  has_opt(item, Options) of
			      true -> item;
			      false -> get_opt(window, fun illegal/0, Options)
			  end
	     end,
    Cores = get_opt(cores, fun illegal/0, Options),
    Length = get_opt(length, fun illegal/0, Options),
    Prob = get_opt(prob, fun illegal/0, Options),
    Variance = get_opt(variance, fun illegal/0, Options),
    Class = get_opt(class_index, fun illegal/0, Options),
    _Ignore = get_opt(ignore_index, fun illegal/0, Options),
    Unique = get_opt(unique, fun illegal/0, Options),
    
    Outputs = try 
		  merge_opts(get_opts([output_model, output_index, output_reduced], Options), 
			     output, Options)
	      catch
		  _:_ ->
		      io:format("*** Error: To few files (or to many output models)"),
		      illegal()
	      end,
    if
	Variance >= Prob ->
	    io:format("*** Error: Variance cannot be larger than the number of non-negative bits ~n"),
	    illegal();
	true -> ok
    end,

    io:format(standard_error, "*** Running '~p' on ~p core(s) *** ~n", [InputFile, Cores]),
    io:format(standard_error, "*** Sliding window: ~p, Index vector: ~p, Non zero bits: ~p+-~p *** ~n",
	      [Window, Length, Prob, Variance]),

    Then = now(),    
    Result = run(InputFile, Cores, Window, Length, Prob, Variance, Class, Unique),
    io:format(standard_error, "*** Calculated ~p semantic vectors in ~p second(s)*** ~n", 
	      [dict:size(Result), timer:now_diff(now(), Then) / 1000000]),

    Then0 = now(),
    case length(Outputs) of
	X when X > 0 -> 
	    write_models(Outputs, Length, Result, Options),
	    io:format(standard_error, "*** Wrote ~p models in ~p second(s)*** ~n", 
		      [X, timer:now_diff(now(), Then0) / 1000000]);
	_ -> ok
    end.
    



write_models(Models, Length, Result, Options) ->
    Self = self(),
    lists:foreach(fun ({output_model, File}) ->
			  io:format(standard_error, "*** Writing model to '~s' *** ~n", [File]),
			  spawn_link(fun() ->
					     ri_util:write_model_to_file(File, Length, Result),
					     Self ! done
				     end);
		      ({output_index, File}) ->
			  io:format(standard_error, "*** Writing index vectors to '~s' *** ~n", [File]),
			  spawn_link(fun() ->
					     ri_util:write_index_to_file(File),
					     Self ! done
				     end);
		      ({output_reduced, File}) ->
			  io:format(standard_error, "*** Writing reduced model to '~s' *** ~n", [File]),
			  spawn_link(fun() ->
					     ri_util:write_reduced_to_file(File, Length, Result, get_opt(rds, fun illegal/0, Options)),
					     Self ! done
				     end)
		  end, Models),
    lists:foreach(fun (_) ->
			  receive
			      done  ->
				  ok
			  end
		  end, lists:seq(1, length(Models))).			       

%%
%% Get command line option Arg, calling Fun1 if not found
%%	     
get_opt(Arg, Fun1, {Options, _}) ->	
    case lists:keyfind(Arg, 1, Options) of
	{Arg, Ws} ->
	    Ws;
	false -> 
	    Fun1()
    end.

%%
%% Return true if Arg exist
%%
has_opt(Arg, {Options, _ }) ->
    lists:any(fun (K) ->
		      K == Arg
	      end, Options).


%%
%% Get all options from the list
%%
get_opts(Match, {Options, _}) ->
    Match0 = sets:from_list(Match),
    lists:filter(fun (Option) ->
			 sets:is_element(Option, Match0)
		 end, Options).

%%
%% Merge Matches with options found with key Key
%%
merge_opts(Matches, Key, {Options, _}) ->
    lists:zip(Matches, lists:reverse(lists:foldl(fun (O, Acc) ->
							 case O of
							     {Key, File} ->
								 [File|Acc];
							     _ -> Acc
							 end
						 end, [], Options))).

show_information() -> 
    io_lib:format("ri (Random Index) ~s.~s.~s (build date: ~s)
Copyright (C) 2013+ ~s

Written by ~s ~n", [?MAJOR_VERSION, ?MINOR_VERSION, ?REVISION, ?DATE,
		  ?AUTHOR, ?AUTHOR]).
