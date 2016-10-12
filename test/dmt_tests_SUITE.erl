-module(dmt_tests_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([application_stop/1]).
-export([integrity/1]).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%%
%% tests descriptions
%%
-spec all() -> [term()].
all() ->
    [
        {group, basic_lifecycle}
    ].

-spec groups() -> [term()].
groups() ->
    [
        {basic_lifecycle, [sequence], [
            integrity
        ]}
    ].

%%
%% starting/stopping
-spec init_per_suite(term()) -> term().
init_per_suite(C) ->
    {ok, Apps} = application:ensure_all_started(dmt),
    [{apps, Apps}|C].

-spec end_per_suite(term()) -> term().
end_per_suite(C) ->
    [application_stop(App) || App <- proplists:get_value(apps, C)].

-spec application_stop(term()) -> term().
application_stop(App) ->
    application:stop(App).

%%
%% tests
-spec integrity(term()) -> term().
integrity(_C) ->
    integrity_check_failed   = insert(dummy_link(<<"0">>, <<"0">>)),
    #'Snapshot'{version = 1} = insert(dummy(<<"0">>)),
    #'Snapshot'{version = 2} = insert(dummy_link(<<"0">>, <<"0">>)),
    integrity_check_failed   = update(dummy_link(<<"0">>, <<"0">>), dummy_link(<<"0">>, <<"1">>)),
    #'Snapshot'{version = 3} = insert(dummy(<<"1">>)),
    #'Snapshot'{version = 4} = update(dummy_link(<<"0">>, <<"0">>), dummy_link(<<"0">>, <<"1">>)),
    integrity_check_failed   = remove(dummy(<<"1">>)),
    #'Snapshot'{version = 5} = remove(dummy_link(<<"0">>, <<"1">>)),
    #'Snapshot'{version = 6} = remove(dummy(<<"1">>)),
    ok.

insert(Object) ->
    Commit = #'Commit'{
        ops = [
            {insert, #'InsertOp'{
                object = Object
            }}
        ]
    },
    (catch dmt_cache:commit(Commit)).

update(OldObject, NewObject) ->
    Commit = #'Commit'{
        ops = [
            {update, #'UpdateOp'{
                old_object = OldObject,
                new_object = NewObject
            }}
        ]
    },
    (catch dmt_cache:commit(Commit)).

remove(Object) ->
    Commit = #'Commit'{
        ops = [
            {remove, #'RemoveOp'{
                object = Object
            }}
        ]
    },
    (catch dmt_cache:commit(Commit)).

dummy(Id) ->
    {dummy, #'DummyObject'{
        ref = #'DummyRef'{
            id = Id
        },
        data = #'Dummy'{}
    }}.

dummy_link(Id, Link) ->
    {dummy_link, #'DummyLinkObject'{
        ref = #'DummyLinkRef'{
            id = Id
        },
        data = #'DummyLink'{
            link = #'DummyRef'{
                id = Link
            }
        }
    }}.
