-module(luwak_mr_words).

-export([
         in_file/1,
         map/3,
         reduce/2
        ]).

-record(chunk, {data}).
-record(segment, {left, words=[], right}).

in_file(Filename) ->
    {ok, C} = riak:local_client(),
    {ok, [{_Start, _End, R}]} =
        C:mapred({modfun, luwak_mr, file, Filename},
                 [{map, {modfun, ?MODULE, map}, none, false},
                  {reduce, {modfun, ?MODULE, reduce}, none, true}]),
    strip_record(R).

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

map(Block, Offset, _) ->
    Data = luwak_block:data(Block),
    [{Offset, Offset+size(Data), words(Data)}].

reduce([], _) -> [];
reduce(Maps, _) ->
    [First|Sorted] = lists:keysort(1, Maps),
    lists:foldl(fun reduce_fun/2, [First], Sorted).

%% so sad
reduce_fun({Seam, End, Map}, [{Start, Seam, LastMap}|Acc]) ->
    [{Start, End, merge(LastMap, Map)}|Acc];
reduce_fun(Next, Acc) ->
    [Next|Acc].

words(Binary) ->
    case words_firstchunk(Binary) of
        whole ->
            #chunk{data=Binary};
        {FirstChunk, Rest} ->
            {Words, LastChunk} = words_rest(Rest),
            #segment{left=FirstChunk, words=Words, right=LastChunk}
    end.

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
