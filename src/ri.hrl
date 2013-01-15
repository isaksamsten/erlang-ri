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
-record(semantic_vector, {length, values}).


%%
%% Record for 
%%
%%
-record(ri_conf, {file, window, cores}).
