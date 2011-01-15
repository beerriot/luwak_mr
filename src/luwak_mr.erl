-module(luwak_mr).

-export([file/3]).

-include("luwak.hrl").

file(FlowPid, Filename, _Timeout) when is_binary(Filename) ->
    {ok, Client} = riak:local_client(),

    {ok, File} = luwak_file:get(Client, Filename),
    V = riak_object:get_value(File),
    {block_size, BlockSize} = lists:keyfind(block_size, 1, V),

    case lists:keyfind(root, 1, V) of
        {root, RootKey} -> tree(FlowPid, Client, BlockSize, RootKey, 0);
        false           -> ok
    end,

    luke_flow:finish_inputs(FlowPid).

tree(FlowPid, Client, BlockSize, Key, Offset) ->
    {ok, #n{children=Children}} = luwak_tree:get(Client, Key),
    lists:foldl(
      fun({SubTree, Size}, SubOffset) when Size > BlockSize ->
              tree(FlowPid, Client, BlockSize, SubTree, SubOffset),
              SubOffset+Size;
         ({Leaf, Size}, LeafOffset) ->
              luke_flow:add_inputs(
                FlowPid, [{{?N_BUCKET, Leaf}, LeafOffset}]),
              LeafOffset+Size
      end,
      Offset,
      Children).
