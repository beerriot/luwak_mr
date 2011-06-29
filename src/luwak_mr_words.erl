%% -------------------------------------------------------------------
%% luwak_mr: utilities for map/reducing on Luwak data
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%  http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Word-splitting, as an example of using {@link luwak_mr:file/3}.
%%
%%      This module is an example of how one might use the
%%      `luwak_mr:file' dynamic map/reduce input generator to run a
%%      computation over a Luwak file.  The example demonstrated is
%%      that of splitting a file into its constituent words.
%%      Inspiration for the method was derived from Guy Steele's talk
%%<a href="http://www.infoq.com/presentations/Thinking-Parallel-Programming">
%%      "How to Think about Parallel Programming--Not!"</a>
%%
%%      The basic idea is to use Luwak's division of blocks as the
%%      division of parallelizable labor.  (The green lines on slide
%%      55/21 of Guy's presentation.)
%%
%%      Once you have put this module and `luwak_mr' in Riak's code
%%      path, you can use it by first filling a Luwak file with
%%      Latin-1 text.  Then run:
%%```
%%      luwak_mr_words:in_file(<<"my_file_name">>).
%%'''
%%      The function should return a list of binaries, each binary
%%      being one word from the file.  The words will remain in the
%%      order they were in the file.  "Words" are defined as
%%      consecutive non-space (code 32) characters.
%%
%%      Do not run this on a very large file in production.  It is
%%      demo code, and therefore does some inefficient things with
%%      binaries.  The `in_file/1' and `reduce/2' functions also expect
%%      to, at some point, hold the entire result set in memory.

-module(luwak_mr_words).

-export([
         in_file/1,
         map/3,
         reduce/2
        ]).

%% A chunk is a block of text whose boundaries we haven't found yet
-record(chunk, {data}).
%% A segment is a block of text containing some bounded words, with a
%% possible chunk on its left and/or right side.
-record(segment, {left, words=[], right}).

%% @spec in_file(binary()) -> [binary()]
%% @doc Split a Luwak file into "words" (strings of characters between
%%      spaces).
in_file(Filename) ->
    {ok, C} = riak:local_client(),
    Inputs = {modfun, luwak_mr, file, Filename},
    Spec = [{map, {modfun, ?MODULE, map}, none, false},
            {reduce, {modfun, ?MODULE, reduce}, none, true}],
    {ok, [{_Start, _End, R}]} =
        case riak_kv_util:mapred_system() of
            pipe ->
                riak_kv_mrc_pipe:mapred(Inputs, Spec);
            legacy ->
                C:mapred(Inputs, Spec)
        end,
    strip_record(R).

%% @spec strip_record(chunk()|segment()) -> [binary()]
%% @doc Converts a chunk or segment record into a list of words.
strip_record(#chunk{data=Data}) ->
    [Data];
strip_record(#segment{left=Left, words=Words, right=Right}) ->
    RMR = case Right of
              undefined -> Words;
              _         -> Words++[Right]
          end,
    RLMR = case Left of
               undefined -> RMR;
               _         -> [Left|RMR]
           end,
    RLMR.

%% @spec map(luwak_block(), integer(), term()) -> [block_result()]
%% @type block_result() = {integer(), integer(), chunk()|segment()}
%% @doc Splits a luwak_block into a chunk or segment.  Used in the map
%%      phase of in_file/1.
%%      The return value is a 3-tuple, with elements:
%%        1: the byte-offset of the start of the block
%%        2: the byte-offset of the next byte after the block
%%        3: the chunk or segment that the block parsed to
map(Block, Offset, _) ->
    Data = luwak_block:data(Block),
    [{Offset, Offset+size(Data), words(Data)}].

%% @spec reduce([block_result()], term()) -> [block_result()]
%% @doc Combines chunks and segments into larger chunks and segments.
reduce([], _) -> [];
reduce(Maps, _) ->
    [First|Sorted] = lists:keysort(1, Maps),
    lists:foldl(fun reduce_fun/2, [First], Sorted).

%% @spec reduce_fun(block_result(), [block_result()]) -> [block_result()]
%% @doc Fold implementation of reduce/2.
%%      The name of this function saddens me.
reduce_fun({Seam, End, Map}, [{Start, Seam, LastMap}|Acc]) ->
    [{Start, End, merge(LastMap, Map)}|Acc];
reduce_fun(Next, Acc) ->
    [Next|Acc].

%% @spec words(binary()) -> chunk()|segment()
%% @doc Split a binary into a chunk (if there are no spaces at all)
%%      or a segment (if there is one or more spaces).
words(Binary) ->
    case words_firstchunk(Binary) of
        whole ->
            #chunk{data=Binary};
        {FirstChunk, Rest} ->
            {Words, LastChunk} = words_rest(Rest),
            #segment{left=FirstChunk, words=Words, right=LastChunk}
    end.

%% @spec words_firstchunk(binary()) -> whole|{binary(), binary()}
%% @doc Recursive finder of the first chunk of a binary.  Returns
%%      'whole' if there are no spaces (so the binary is one big
%%      chunk) or a 2-tuple of:
%%        1: the characters before the first space
%%        2: the characters after the first space
words_firstchunk(Binary) -> words_firstchunk(Binary, []).
words_firstchunk(<<>>, _Acc) ->
    whole; %% don't recreate that whole binary
words_firstchunk(<<$\s,Rest/binary>>, Acc) ->
    FirstChunk = case Acc of
                     [] -> undefined;
                     _  -> list_to_binary(lists:reverse(Acc))
                 end,
    {FirstChunk, Rest};
words_firstchunk(<<C,Rest/binary>>, Acc) ->
    words_firstchunk(Rest, [C|Acc]).

%% @spec words_rest(binary()) -> {[binary()], binary()}
%% @doc Recursive splitter of the rest of a binary.  Returns
%%      a 2-tuple of:
%%        1: a list of space-delimited words
%%        2: the list of characters after the final space
words_rest(Binary) -> words_rest(Binary, [], []).
words_rest(<<>>, CharAcc, WordAcc) ->
    LastChunk = case CharAcc of
                    [] -> undefined;
                    _  -> list_to_binary(lists:reverse(CharAcc))
                end,
    {lists:reverse(WordAcc), LastChunk};
words_rest(<<$\s,Rest/binary>>, CharAcc, WordAcc) ->
    words_rest(Rest, [],
               case CharAcc of
                   [] -> WordAcc;
                   _  -> [list_to_binary(lists:reverse(CharAcc))|WordAcc]
               end);
words_rest(<<C,Rest/binary>>, CharAcc, WordAcc) ->
    words_rest(Rest, [C|CharAcc], WordAcc).

%% @spec merge(chunk()|segment, chunk()|segment()) -> chunk()|segment()
%% @doc Merge a chunk or segment with another chunk or segment.
%%      The first argument should be the chunk or segement imediately
%%      preceding the chunk or segment in the second argument.
merge(#chunk{data=TC}, #chunk{data=OC}) ->
    #chunk{data=iolist_to_binary([TC,OC])};
merge(#chunk{data=TC}, #segment{left=OL}=O) ->
    NL = case OL of
             undefined -> TC;
             _         -> iolist_to_binary([TC,OL])
         end,
    O#segment{left=NL};
merge(#segment{right=TR}=T, #chunk{data=OC}) ->
    NR = case TR of
             undefined -> OC;
             _         -> iolist_to_binary([TR,OC])
               end,
    T#segment{right=NR};
merge(#segment{left=TL, words=TW, right=TR},
      #segment{left=OL, words=OW, right=OR}) ->
    NW = case {TR, OL} of
             {undefined, undefined} -> TW++OW;
             {_        , undefined} -> TW++[TR|OW];
             {undefined, _        } -> TW++[OL|OW];
             {_        , _        } -> TW++[iolist_to_binary([TR,OL])|OW]
         end,
    #segment{left=TL, words=NW, right=OR}.
