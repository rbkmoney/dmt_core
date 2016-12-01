-module(dmt_domain_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("dmsl/include/dmsl_domain_thrift.hrl").
-include("dmt_domain_tests_helper.hrl").

-type testcase() :: {_, fun()}.

-spec test() -> _.
-spec basic_flow_test_() -> [testcase()].
-spec nested_links_test() -> [testcase()].
-spec batch_link_test() -> [testcase()].

basic_flow_test_() ->
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
            {integrity_check_failed, {references_nonexistent, [{dummy, #domain_DummyRef{id = 0}}]}},
            dmt_domain:apply_operations([?insert(?dummy_link(1, 0))], Fixture)
        ),
        ?_assertThrow(
            {integrity_check_failed, {referenced_by, [?dummy_link(1337, 42)]}},
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
            {integrity_check_failed, {references_nonexistent, [{dummy, #domain_DummyRef{id = 0}}]}},
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

nested_links_test() ->
    Fixture = construct_fixture(),

    DomainObject = #domain_GlobalsObject{
        ref = #domain_GlobalsRef{},
        data = #domain_Globals{
            party_prototype = ?party_prototype_ref(0),
            providers = {
                predicates,
                ordsets:from_list([
                    #domain_ProviderPredicate{
                        if_ = {
                            all_of,
                            ordsets:from_list([
                                {
                                    condition,
                                    {category_is, ?category_ref(0)}
                                }
                            ])
                        },
                        then_ = {
                            value,
                            ordsets:from_list([
                                ?provider_ref(0)
                            ])
                        }

                    },
                    #domain_ProviderPredicate{
                        if_ = {
                            condition,
                            {category_is, ?category_ref(1)}
                        },
                        then_ = {
                            value,
                            ordsets:from_list([
                                ?provider_ref(1),
                                ?provider_ref(2)
                            ])
                        }
                    }
                ])
            },
            system_accounts = {
                value,
                ordsets:from_list([
                    ?system_account_set_ref(0)
                ])
            }
        }
    },
    ?assertThrow(
            {integrity_check_failed,
                {
                    references_nonexistent,
                    [
                        {system_account_set, ?system_account_set_ref(0)},
                        {provider, ?provider_ref(2)},
                        {provider, ?provider_ref(1)},
                        {provider, ?provider_ref(0)},
                        {category, ?category_ref(0)},
                        {party_prototype, ?party_prototype_ref(0)}
                    ]
                }
            },
            dmt_domain:apply_operations([?insert({globals, DomainObject})], Fixture)
        )
    .

batch_link_test() ->
    Fixture = construct_fixture(),
    Sas = {system_account_set, #domain_SystemAccountSetObject{
        ref = ?sas_ref(1),
        data = #domain_SystemAccountSet{
            name = <<"Primaries">>,
            description = <<"Primaries">>,
            currency = ?currency_ref(<<"USD">>),
            compensation = []
        }
    }},
    Currency = {currency, #domain_CurrencyObject{
        ref = ?currency_ref(<<"USD">>),
        data = #domain_Currency{
            name = <<"US Dollars">>,
            numeric_code = 840,
            symbolic_code = <<"USD">>,
            exponent = 2
        }
    }},
    ?assertMatch(
        #{},
        dmt_domain:apply_operations([?insert(Sas), ?insert(Currency)], Fixture)
    ).

%%

construct_fixture() ->
    maps:from_list([{{Type, Ref}, Object} || Object = {Type, {_, Ref, _}} <- [
        ?dummy(41),
        ?dummy(42),
        ?dummy(43),
        ?dummy(44),
        ?dummy_link(1337, 42),
        ?dummy_link(1338, 43),
        ?category(1, <<"testCategory">>, <<"testDescription">>)
    ]]).
