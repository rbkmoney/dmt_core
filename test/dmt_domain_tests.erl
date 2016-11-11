-module(dmt_domain_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("dmsl/include/dmsl_domain_thrift.hrl").
-include("dmt_domain_tests_helper.hrl").

-type testcase() :: {_, fun()}.

-spec test() -> _.
-spec conflict_test_() -> [testcase()].

conflict_test_() ->
    Fixture = construct_fixture(),
    [
        ?_assertEqual(
            Fixture,
            dmt_domain:apply_operations([], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_already_exists, ?dummy_link(1337, 42)}},
            dmt_domain:apply_operations([?insert(?dummy_link(1337, 43))], Fixture)
        ),
        ?_assertThrow(
            {integrity_check_failed, {non_existent, [{dummy, #domain_DummyRef{id = 0}}]}},
            dmt_domain:apply_operations([?insert(?dummy_link(1, 0))], Fixture)
        ),
        ?_assertThrow(
            {integrity_check_failed, {referenced, [?dummy_link(1337, 42)]}},
            dmt_domain:apply_operations([?remove(?dummy(42))], Fixture)
        ),
        ?_assertMatch(
            #{},
            dmt_domain:apply_operations([?remove(?dummy_link(1337, 42)), ?remove(?dummy(42)), ?remove(?dummy(44))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_not_found, ?dummy(1)}},
            dmt_domain:apply_operations([?remove(?dummy(1))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_not_found, ?dummy(41)}},
            dmt_domain:apply_operations([?remove(?dummy(41)), ?remove(?dummy(41))], Fixture)
        ),
        ?_assertThrow(
            {integrity_check_failed, {non_existent, [{dummy, #domain_DummyRef{id = 0}}]}},
            dmt_domain:apply_operations([?update(?dummy_link(1337, 42), ?dummy_link(1337, 0))], Fixture)
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
        ?dummy(41),
        ?dummy(42),
        ?dummy(43),
        ?dummy(44),
        ?dummy_link(1337, 42),
        ?dummy_link(1338, 43)
    ]]).
