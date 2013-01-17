%%
%% Record for storing the index vectors
%%
%% length:   the length of the index vector
%% prob:     the number of non-zero bits
%% variance: the variance in number of non-zero bits
%%
-record(index_vector, {length, prob, variance}).

%%
%% Record for storing information regarding the length and contents
%% of the semantic vector
%%
%% class: undefined() | string()
%% length: int() showing the length of the vector
%% values: dict() -> {term(), integer()} of indicies and values
%%
-record(semantic_vector, {class, length, values}).


%%
%% Record for storing configuration options
%% 
%% file: Pid, to csv:reader/2
%% window: window size, doc or item
%% cores: number of processing cores
%% ignore: ignore items with id 
%% class: treat index as class 
%%
-record(ri_conf, {file, window, cores, ignore, class}).
