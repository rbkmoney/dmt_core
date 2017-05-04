-module(dmt_history).

-export([head/1]).
-export([head/2]).
-export([travel/3]).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

-type history() :: dmsl_domain_config_thrift:'History'().
-type version() :: dmsl_domain_config_thrift:'Version'().
-type snapshot() :: dmsl_domain_config_thrift:'Snapshot'().

-spec head(history()) -> snapshot().
head(History) ->
    head(History, #'Snapshot'{version = 0, domain = dmt_domain:new()}).

-spec head(history(), snapshot()) -> snapshot().
head(History, Snapshot) when map_size(History) =:= 0 ->
    Snapshot;
head(History, Snapshot) ->
    Head = lists:max(maps:keys(History)),
    travel(Head, History, Snapshot).

-spec travel(version(), history(), snapshot()) -> snapshot().
travel(To, _History, #'Snapshot'{version = From} = Snapshot)
when To =:= From ->
    Snapshot;
travel(To, History, #'Snapshot'{version = From, domain = Domain})
when To > From ->
    #'Commit'{ops = Ops} = maps:get(From + 1, History),
    NextSnapshot = #'Snapshot'{
        version = From + 1,
        domain = dmt_domain:apply_operations(Ops, Domain)
    },
    travel(To, History, NextSnapshot);
travel(To, History, #'Snapshot'{version = From, domain = Domain})
when To < From ->
    #'Commit'{ops = Ops} = maps:get(From, History),
    PreviousSnapshot = #'Snapshot'{
        version = From - 1,
        domain = dmt_domain:revert_operations(Ops, Domain)
    },
    travel(To, History, PreviousSnapshot).
