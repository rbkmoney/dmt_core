-module(bench_apply_operations).

%% API
-export([
    apply_operations/1,
    bench_apply_operations/2
]).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

-type snapshot() :: dmsl_domain_config_thrift:'Snapshot'().
-type commit() :: dmsl_domain_config_thrift:'Commit'().

-record(st, {
    snapshot = #'Snapshot'{version = 0, domain = dmt_domain:new()} :: snapshot(),
    history = #{} :: #{_Version => commit()}
}).

-spec repository_state() ->
    term().
repository_state() ->
    % NOTE
    % There's no such file in the repo, sorry. If you want to employ this benchmarking code you'd
    % have to synthesize it yourself, luckily there's a typespec.
    {ok, Bin} = file:read_file("test/domain-config.history.10.bert"),
    erlang:binary_to_term(Bin).

-spec apply_operations({input, _State}) ->
    term().
apply_operations({input, _}) ->
    St0 = #st{history = History0} = repository_state(),
    History1 = maps:with(lists:sublist(lists:sort(maps:keys(History0)), 3), History0),
    St1 = St0#st{history = History1},
    _ = compute_operation_stats(St1),
    St1.

compute_operation_stats(#st{snapshot = Snapshot, history = History}) ->
    NumInserts = count_history_operations(insert, History),
    NumUpdates = count_history_operations(update, History),
    NumRemoves = count_history_operations(remove, History),
    _ = io:format(user, "~n", []),
    [io:format(user, "~24.ts: ~p~n", Args) || Args <- [
        ["Snapshot version"     , Snapshot#'Snapshot'.version],
        ["Snapshot size"        , maps:size(Snapshot#'Snapshot'.domain)],
        ["Number of commits"    , maps:size(History)],
        ["Number of operations" , NumInserts + NumUpdates + NumRemoves],
        ["Number of insertions" , NumInserts],
        ["Number of updates"    , NumUpdates],
        ["Number of removals"   , NumRemoves]
    ]],
    io:format(user, "~n", []).

count_history_operations(Type, History = #{}) ->
    maps:fold(
        fun (_, #'Commit'{ops = Ops}, N) ->
            N + count_commit_operations(Type, Ops)
        end,
        0,
        History
    ).

count_commit_operations(Type, Ops) ->
    lists:foldl(
        fun ({TypeOp, _Op}, N) ->
            case TypeOp of
                Type -> N + 1;
                _ -> N
            end
        end,
        0,
        Ops
    ).

-spec bench_apply_operations(#st{}, _State) ->
    term().
bench_apply_operations(#st{snapshot = Snapshot, history = History}, _) ->
    dmt_history:head(History, Snapshot).
