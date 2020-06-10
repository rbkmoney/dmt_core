-module(dmt_domain_tests).
-include_lib("eunit/include/eunit.hrl").
-include("dmt_domain_tests_helper.hrl").

-type testcase() :: {_, fun()}.

-spec test() -> _.
-spec basic_flow_test_() -> [testcase()].
-spec nested_links_test() -> _.
-spec batch_link_test() -> _.
-spec wrong_spec_order_test() -> _.
-spec reference_cycle_test_() -> [testcase()].

basic_flow_test_() ->
    Fixture = construct_fixture(),
    [
        ?_assertEqual(
            {ok, Fixture},
            dmt_domain:apply_operations([], Fixture)
        ),
        ?_assertEqual(
            {error, {conflict, {object_already_exists, ?dummy_link_ref(1337)}}},
            dmt_domain:apply_operations([?insert(?dummy_link(1337, 43))], Fixture)
        ),
        ?_assertEqual(
            {error, {invalid, {objects_not_exist, [{?dummy_ref(0), [?dummy_link_ref(1)]}]}}},
            dmt_domain:apply_operations([?insert(?dummy_link(1, 0))], Fixture)
        ),
        ?_assertEqual(
            {error, {invalid, {objects_not_exist, [{?dummy_ref(42), [?dummy_link_ref(1337)]}]}}},
            dmt_domain:apply_operations([?remove(?dummy(42))], Fixture)
        ),
        ?_assertMatch(
            {ok, #{}},
            dmt_domain:apply_operations([?remove(?dummy_link(1337, 42)), ?remove(?dummy(42)), ?remove(?dummy(44))], Fixture)
        ),
        ?_assertEqual(
            {error, {conflict, {object_not_found, ?dummy_ref(1)}}},
            dmt_domain:apply_operations([?remove(?dummy(1))], Fixture)
        ),
        ?_assertEqual(
            {error, {conflict, {object_not_found, ?dummy_ref(41)}}},
            dmt_domain:apply_operations([?remove(?dummy(41)), ?remove(?dummy(41))], Fixture)
        ),
        ?_assertEqual(
            {error, {invalid, {objects_not_exist, [{?dummy_ref(0), [?dummy_link_ref(1337)]}]}}},
            dmt_domain:apply_operations([?update(?dummy_link(1337, 42), ?dummy_link(1337, 0))], Fixture)
        ),
        ?_assertMatch(
            {ok, #{}},
            dmt_domain:apply_operations([?update(?dummy_link(1337, 42), ?dummy_link(1337, 44))], Fixture)
        ),
        ?_assertEqual(
            {error, {conflict, {object_reference_mismatch, ?dummy_ref(1)}}},
            dmt_domain:apply_operations([?update(?dummy(42), ?dummy(1))], Fixture)
        ),
        ?_assertEqual(
            {error, {conflict, {object_not_found, ?dummy_link_ref(1)}}},
            dmt_domain:apply_operations([?update(?dummy_link(1, 42), ?dummy_link(1, 44))], Fixture)
        ),
        ?_assertEqual(
            {error, {conflict, {object_not_found, ?dummy_link_ref(1337)}}},
            dmt_domain:apply_operations([?update(?dummy_link(1337, 1), ?dummy_link(1337, 42))], Fixture)
        )
    ].

nested_links_test() ->
    DomainObject = #domain_GlobalsObject{
        ref = #domain_GlobalsRef{},
        data = #domain_Globals{
            external_account_set = {value, ?ext_account_set_ref(1)},
            payment_institutions = [
                ?pinst_ref(1),
                ?pinst_ref(2)
            ]
        }
    },
    PaymentInstitution = #domain_PaymentInstitutionObject{
        ref = ?pinst_ref(1),
        data = #domain_PaymentInstitution{
            name = <<"PaymentInstitution">>,
            system_account_set = {value, ?system_account_set_ref(0)},
            default_contract_template = {value, ?contract_template_ref(1)},
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
            inspector = {value, ?inspector_ref(1)},
            realm = test,
            residences = []
        }
    },
    Ops = [?insert({globals, DomainObject}), ?insert({payment_institution, PaymentInstitution})],
    {error, {invalid, {objects_not_exist, Missing}}} = dmt_domain:apply_operations(Ops, construct_fixture()),
    ?assertEqual(
        [
            {{category, ?category_ref(0)}, [{payment_institution, ?pinst_ref(1)}]},
            {{contract_template, ?contract_template_ref(1)}, [{payment_institution, ?pinst_ref(1)}]},
            {{external_account_set, ?ext_account_set_ref(1)}, [{globals, #domain_GlobalsRef{}}]},
            {{inspector, ?inspector_ref(1)}, [{payment_institution, ?pinst_ref(1)}]},
            {{payment_institution, ?pinst_ref(2)}, [{globals, #domain_GlobalsRef{}}]},
            {{provider, ?provider_ref(0)}, [{payment_institution, ?pinst_ref(1)}]},
            {{provider, ?provider_ref(1)}, [{payment_institution, ?pinst_ref(1)}]},
            {{provider, ?provider_ref(2)}, [{payment_institution, ?pinst_ref(1)}]},
            {{system_account_set, ?system_account_set_ref(0)}, [{payment_institution, ?pinst_ref(1)}]}
        ],
        lists:sort(Missing)
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
        {ok, #{}},
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
                options = #{
                    <<"override">> => <<"Terminal 1">>
                },
                terms = #domain_PaymentsProvisionTerms{
                    categories = {value, [?category_ref(1)]},
                    payment_methods = {value, [#domain_PaymentMethodRef{id = {bank_card, visa}}]},
                    cash_flow = {value, []}
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
        {ok, #{}},
        dmt_domain:apply_operations(
            [?insert(Terminal), ?insert(Currency), ?insert(PaymentMethod)],
            construct_fixture()
        )
    ).

reference_cycle_test_() ->
    Fixture = construct_fixture(),
    IDSelf = 42,
    ID1 = 1,
    ID2 = 2,
    ID3 = 3,
    Pred1 = {any_of, ?set([
        {constant, true},
        {condition, {category_is, ?category_ref(1)}},
        {all_of, ?set([
            {is_not, {criterion, ?criterion_ref(ID2)}}
        ])}
    ])},
    Pred2 = {all_of, ?set([
        {any_of, ?set([
            {is_not, {condition, {shop_location_is, {url, <<"BLARG">>}}}},
            {criterion, ?criterion_ref(ID3)},
            {criterion, ?criterion_ref(ID1)}
        ])}
    ])},
    Pred3 = {is_not, {criterion, ?criterion_ref(ID1)}},

    [
        ?_assertEqual(
            {error, {invalid, {object_reference_cycles, [
                [{criterion, ?criterion_ref(IDSelf)}]
            ]}}},
            dmt_domain:apply_operations(
                [?insert(?criterion(IDSelf, <<"Root">>, {criterion, ?criterion_ref(IDSelf)}))],
                Fixture
            )
        ),
        ?_assertEqual(
            [
                [{criterion, ?criterion_ref(ID)} || ID <- [ID1, ID2]],
                [{criterion, ?criterion_ref(ID)} || ID <- [ID1, ID2, ID3]]
            ],
            begin
                {error, {invalid, {object_reference_cycles, Cycles}}} = dmt_domain:apply_operations(
                    [
                        ?insert(?criterion(ID1, <<"There">>, Pred1)),
                        ?insert(?criterion(ID2, <<"Be">>, Pred2)),
                        ?insert(?criterion(ID3, <<"Dragons">>, Pred3))
                    ],
                    Fixture
                ),
                lists:sort(Cycles)
            end
        ),
        ?_assertEqual(
            [
                [{criterion, ?criterion_ref(ID)} || ID <- [ID2, ID1]],
                [{criterion, ?criterion_ref(ID)} || ID <- [ID2, ID3, ID1]]
            ],
            begin
                Criterion1 = ?criterion(ID1, <<"There">>, Pred1),
                Criterion2 = ?criterion(ID2, <<"No">>, {constant, false}),
                Criterion3 = ?criterion(ID3, <<"Dragons">>, Pred3),
                Criterion2Next = ?criterion(ID2, <<"Be">>, Pred2),
                {ok, Domain1} = dmt_domain:apply_operations(
                    [?insert(Criterion1), ?insert(Criterion2), ?insert(Criterion3)],
                    Fixture
                ),
                {error, {invalid, {object_reference_cycles, Cycles}}} = dmt_domain:apply_operations(
                    [?update(Criterion2, Criterion2Next)],
                    Domain1
                ),
                lists:sort(Cycles)
            end
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
        ?dummy_link(1338, 43),
        ?category(1, <<"testCategory">>, <<"testDescription">>)
    ]]).
