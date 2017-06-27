-module(dmt_domain_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("dmsl/include/dmsl_domain_thrift.hrl").
-include("dmt_domain_tests_helper.hrl").

-type testcase() :: {_, fun()}.

-spec test() -> _.
-spec basic_flow_test_() -> [testcase()].
-spec nested_links_test() -> [testcase()].
-spec batch_link_test() -> [testcase()].
-spec wrong_spec_order_test() -> [testcase()].

basic_flow_test_() ->
    Fixture = construct_fixture(),
    [
        ?_assertEqual(
            Fixture,
            dmt_domain:apply_operations([], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_already_exists, {dummy_link, #domain_DummyLinkRef{id = 1337}}}},
            dmt_domain:apply_operations([?insert(?dummy_link(1337, 43))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {objects_not_exist, [{{dummy, #domain_DummyRef{id = 0}}, [{dummy_link, #domain_DummyLinkRef{id = 1}}]}]}},
            dmt_domain:apply_operations([?insert(?dummy_link(1, 0))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {objects_not_exist, [{{dummy, #domain_DummyRef{id = 42}}, [{dummy_link, #domain_DummyLinkRef{id = 1337}}]}]}},
            dmt_domain:apply_operations([?remove(?dummy(42))], Fixture)
        ),
        ?_assertMatch(
            #{},
            dmt_domain:apply_operations([?remove(?dummy_link(1337, 42)), ?remove(?dummy(42)), ?remove(?dummy(44))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_not_found, {dummy, #domain_DummyRef{id = 1}}}},
            dmt_domain:apply_operations([?remove(?dummy(1))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_not_found, {dummy, #domain_DummyRef{id = 41}}}},
            dmt_domain:apply_operations([?remove(?dummy(41)), ?remove(?dummy(41))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {objects_not_exist, [{{dummy, #domain_DummyRef{id = 0}}, [{dummy_link, #domain_DummyLinkRef{id = 1337}}]}]}},
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
            {conflict, {object_not_found, {dummy_link, #domain_DummyLinkRef{id = 1}}}},
            dmt_domain:apply_operations([?update(?dummy_link(1, 42), ?dummy_link(1, 44))], Fixture)
        ),
        ?_assertThrow(
            {conflict, {object_not_found, {dummy_link, #domain_DummyLinkRef{id = 1337}}}},
            dmt_domain:apply_operations([?update(?dummy_link(1337, 1), ?dummy_link(1337, 42))], Fixture)
        )
    ].

nested_links_test() ->
    DomainObject = #domain_GlobalsObject{
        ref = #domain_GlobalsRef{},
        data = #domain_Globals{
            party_prototype = ?party_prototype_ref(0),
            providers = {
                decisions,
                [
                    #domain_ProviderDecision{
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
                    #domain_ProviderDecision{
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
                ]
            },
            system_account_set = {value, ?system_account_set_ref(0)},
            inspector = {value, ?inspector_ref(1)},
            default_contract_template = ?contract_template_ref(1)
        }
    },
    ?assertThrow(
        {conflict,
            {objects_not_exist,
                [
                    {{contract_template, ?contract_template_ref(1)}, [{globals,{domain_GlobalsRef}}]},
                    {{inspector, ?inspector_ref(1)}, [{globals,{domain_GlobalsRef}}]},
                    {{system_account_set, ?system_account_set_ref(0)}, [{globals,{domain_GlobalsRef}}]},
                    {{provider, ?provider_ref(2)}, [{globals,{domain_GlobalsRef}}]},
                    {{provider, ?provider_ref(1)}, [{globals,{domain_GlobalsRef}}]},
                    {{provider, ?provider_ref(0)}, [{globals,{domain_GlobalsRef}}]},
                    {{category, ?category_ref(0)}, [{globals,{domain_GlobalsRef}}]},
                    {{party_prototype, ?party_prototype_ref(0)}, [{globals,{domain_GlobalsRef}}]}
                ]
            }
        },
        dmt_domain:apply_operations([?insert({globals, DomainObject})], construct_fixture())
    ).

batch_link_test() ->
    Sas = {system_account_set, #domain_SystemAccountSetObject{
        ref = ?sas_ref(1),
        data = #domain_SystemAccountSet{
            name = <<"Primaries">>,
            description = <<"Primaries">>,
            accounts = #{
                ?currency_ref(<<"USD">>) => #domain_SystemAccount{settlement = 424242}
            }
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
        dmt_domain:apply_operations([?insert(Sas), ?insert(Currency)], #{})
    ).

wrong_spec_order_test() ->
    Terminal = {
        terminal,
        #domain_TerminalObject{
            ref = ?terminal_ref(1),
            data = #domain_Terminal{
                name = <<"Terminal 1">>,
                description = <<"Test terminal 1">>,
                payment_method = #domain_PaymentMethodRef{id = {bank_card, visa}},
                category = ?category_ref(1),
                cash_flow = [],
                account = ?terminal_account(<<"USD">>),
                options = #{
                    <<"override">> => <<"Terminal 1">>
                }
            }
        }
    },
    Currency = {currency, #domain_CurrencyObject{
        ref = ?currency_ref(<<"USD">>),
        data = #domain_Currency{
            name = <<"US Dollars">>,
            numeric_code = 840,
            symbolic_code = <<"USD">>,
            exponent = 2
        }
    }},
    PaymentMethod = {
        payment_method,
        #domain_PaymentMethodObject{
            ref = ?payment_method_ref({bank_card, visa}),
            data = #domain_PaymentMethodDefinition{
                name = <<"VISA">>,
                description = <<"VISA BANK CARD">>
            }
        }
    },
    ?assertMatch(
        #{},
        dmt_domain:apply_operations(
            [?insert(Terminal), ?insert(Currency), ?insert(PaymentMethod)],
            construct_fixture()
        )
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
