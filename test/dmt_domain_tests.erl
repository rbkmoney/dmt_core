-module(dmt_domain_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

-type testcase() :: {_, fun()}.

-spec test() -> _.
-spec conflict_test_() -> [testcase()].

%%

-define(dummy(ID),
    {dummy, #domain_DummyObject{
        ref = #domain_DummyRef{id = ID},
        data = #domain_Dummy{}
    }}
).

-define(dummy_link(ID, To),
    {dummy_link, #domain_DummyLinkObject{
        ref = #domain_DummyLinkRef{id = ID},
        data = #domain_DummyLink{link = #domain_DummyRef{id = To}}
    }}
).

-define(insert(O), {insert, #'InsertOp'{object = O}}).
-define(remove(O), {remove, #'RemoveOp'{object = O}}).
-define(update(O1, O2), {update, #'UpdateOp'{old_object = O1, new_object = O2}}).

conflict_test_() ->
    Fixture = construct_fixture(),
    [
        ?_assertEqual(
            Fixture,
            dmt_domain:apply_operations([], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_already_exists, ?dummy_link(1337, 42)}},
            dmt_domain:apply_operations([?insert(?dummy_link(1337, 1))], Fixture)
        ),
        ?_assertMatch(
            #{},
            dmt_domain:apply_operations([?remove(?dummy(42)), ?remove(?dummy(44))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_not_found, ?dummy(1)}},
            dmt_domain:apply_operations([?remove(?dummy(1))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_not_found, ?dummy(42)}},
            dmt_domain:apply_operations([?remove(?dummy(42)), ?remove(?dummy(42))], Fixture)
        ),
        ?_assertMatch(
            #{},
            dmt_domain:apply_operations([?update(?dummy_link(1337, 42), ?dummy_link(1337, 44))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_reference_mismatch, {dummy, #domain_DummyRef{id = 1}}}},
            dmt_domain:apply_operations([?update(?dummy(42), ?dummy(1))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_not_found, ?dummy_link(1, 42)}},
            dmt_domain:apply_operations([?update(?dummy_link(1, 42), ?dummy_link(1, 44))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_not_found, ?dummy_link(1337, 1)}},
            dmt_domain:apply_operations([?update(?dummy_link(1337, 1), ?dummy_link(1337, 42))], Fixture)
        )
    ].

%%

construct_fixture() ->
    maps:from_list([{{Type, Ref}, Object} || Object = {Type, {_, Ref, _}} <- [
        ?dummy(42),
        ?dummy(43),
        ?dummy(44),
        ?dummy_link(1337, 42),
        ?dummy_link(1338, 43)
    ]]).
