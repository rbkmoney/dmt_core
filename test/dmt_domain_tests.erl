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
-spec random_reference_cycle_test_() -> [testcase()].
-spec complete_reference_cycle_test_() -> [testcase()].

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
            dmt_domain:apply_operations(
                [?remove(?dummy_link(1337, 42)), ?remove(?dummy(42)), ?remove(?dummy(44))],
                Fixture
            )
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
    Sas =
        {system_account_set, #domain_SystemAccountSetObject{
            ref = ?sas_ref(1),
            data = #domain_SystemAccountSet{
                name = <<"Primaries">>,
                description = <<"Primaries">>,
                accounts = #{
                    ?currency_ref(<<"USD">>) => #domain_SystemAccount{settlement = 424242}
                }
            }
        }},
    Currency =
        {currency, #domain_CurrencyObject{
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
                terms = #domain_ProvisionTermSet{
                    payments = #domain_PaymentsProvisionTerms{
                        categories = {value, [?category_ref(1)]},
                        payment_methods = {value, [#domain_PaymentMethodRef{id = {bank_card, visa}}]},
                        cash_flow = {value, []}
                    }
                }
            }
        }
    },
    Currency =
        {currency, #domain_CurrencyObject{
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
    Pred1 =
        {any_of,
            ?set([
                {constant, true},
                {condition, {category_is, ?category_ref(1)}},
                {all_of,
                    ?set([
                        {is_not, {criterion, ?criterion_ref(ID2)}}
                    ])}
            ])},
    Pred2 =
        {all_of,
            ?set([
                {any_of,
                    ?set([
                        {is_not, {condition, {shop_location_is, {url, <<"BLARG">>}}}},
                        {criterion, ?criterion_ref(ID3)},
                        {criterion, ?criterion_ref(ID1)}
                    ])}
            ])},
    Pred3 = {is_not, {criterion, ?criterion_ref(ID1)}},
    [
        %%   ┌──┐
        %%   ∨  │
        %% ┌──────┐
        %% │  42  │
        %% └──────┘
        ?_assertEqual(
            {error,
                {invalid,
                    {object_reference_cycles, [
                        [{criterion, ?criterion_ref(IDSelf)}]
                    ]}}},
            dmt_domain:apply_operations(
                [?insert(?criterion(IDSelf, <<"Root">>, {criterion, ?criterion_ref(IDSelf)}))],
                Fixture
            )
        ),
        %%   ┌─────────┐
        %%   ∨         │
        %% ┌───┐     ┌───┐     ┌───┐
        %% │ 1 │ ──> │ 2 │ ──> │ 3 │
        %% └───┘     └───┘     └───┘
        %%   ∧                   │
        %%   └───────────────────┘
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
        ),
        %%      ┌───┐      ┌───┐
        %%   ┌> │ 1 │ ───> │ 2 │
        %%   │  └───┘      └───┘
        %%   │    ∧          │
        %%   │    │          │
        %%   │    ∨          ∨
        %%   │  ┌───┐      ┌───┐
        %%   │  │ 4 │ <──> │ 3 │
        %%   │  └───┘      └───┘
        %%   └───────────────┘
        ?_assertEqual(
            [
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 2, 3]],
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 2, 3, 4]],
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 4]],
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 4, 3]],
                [{criterion, ?criterion_ref(ID)} || ID <- [3, 4]]
            ],
            begin
                C1 = criterion_w_refs(1, [2, 4]),
                C2 = criterion_w_refs(2, [3]),
                C3 = criterion_w_refs(3, [1, 4]),
                C4 = criterion_w_refs(4, [1, 3]),
                {error, {invalid, {object_reference_cycles, Cycles}}} = dmt_domain:apply_operations(
                    [?insert(C) || C <- [C1, C2, C3, C4]],
                    Fixture
                ),
                lists:sort(Cycles)
            end
        ),
        %%            ┌──────────────────────────┐
        %%   ┌────────┼────────┐                 │
        %%   │        │        ∨                 ∨
        %% ┌───┐    ┌───┐    ┌───┐    ┌───┐    ┌───┐
        %% │ 2 │ ─> │ 3 │ ─> │ 4 │ ─> │ 5 │ ─> │ 6 │ ─┐
        %% └───┘    └───┘    └───┘    └───┘    └───┘  │
        %%   ∧        │        │        ∧        ∧    │
        %%   │        └────────┼────────┘        │    │
        %% ┌───┐               │                 │    │
        %% │ 1 │               └─────────────────┘    │
        %% └───┘                                      │
        %%   ∧                                        │
        %%   └────────────────────────────────────────┘
        ?_assertEqual(
            [
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 2, 3, 4, 5, 6]],
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 2, 3, 4, 6]],
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 2, 3, 5, 6]],
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 2, 3, 6]],
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 2, 4, 5, 6]],
                [{criterion, ?criterion_ref(ID)} || ID <- [1, 2, 4, 6]]
            ],
            begin
                C1 = criterion_w_refs(1, [2]),
                C2 = criterion_w_refs(2, [3, 4]),
                C3 = criterion_w_refs(3, [4, 5, 6]),
                C4 = criterion_w_refs(4, [5, 6]),
                C5 = criterion_w_refs(5, [6]),
                C6 = criterion_w_refs(6, [1]),
                {error, {invalid, {object_reference_cycles, Cycles}}} = dmt_domain:apply_operations(
                    [?insert(C) || C <- [C1, C2, C3, C4, C5, C6]],
                    Fixture
                ),
                lists:sort(Cycles)
            end
        )
    ].

random_reference_cycle_test_() ->
    N = 100,
    % conservative, bigger values may explode time complexity
    E = round(N * 1.2),
    Nodes = lists:seq(1, N),
    Nr = rand:uniform(N),
    Edges = lists:foldl(
        fun({U, V}, M) ->
            maps:update_with(U, fun(As) -> [V | As] end, [V], M)
        end,
        #{},
        % ensure at least one cycle
        [{Nr, Nr} | [{rand:uniform(N), rand:uniform(N)} || _ <- lists:seq(1, E)]]
    ),
    Criterions = [criterion_w_refs(ID, maps:get(ID, Edges, [])) || ID <- Nodes],
    ?_assertMatch(
        {error, {invalid, {object_reference_cycles, _}}},
        dmt_domain:apply_operations([?insert(C) || C <- Criterions], dmt_domain:new())
    ).

complete_reference_cycle_test_() ->
    [
        begin
            Nodes = lists:seq(1, N),
            Operations = [?insert(criterion_w_refs(ID, [ID1 || ID1 <- Nodes])) || ID <- Nodes],
            {timeout, 10,
                ?_assertEqual(
                    %% Number of cycles in complete directed graph.
                    %% For each subset of K nodes there are (K-1)! cyclic permutations of this subset,
                    %% plus there are binom(N, K) different subsets for each K.
                    lists:sum([binom(N, K) * factorial(K - 1) || K <- lists:seq(1, N)]),
                    begin
                        {error, {invalid, {object_reference_cycles, Cycles}}} =
                            dmt_domain:apply_operations(Operations, dmt_domain:new()),
                        length(Cycles)
                    end
                )}
        end
        || N <- lists:seq(1, 9)
    ].

binom(N, K) ->
    factorial(N) div (factorial(K) * factorial(N - K)).

factorial(0) -> 1;
factorial(N) -> factorial(N - 1) * N.

criterion_w_refs(ID, Refs) ->
    ?criterion(ID, <<>>, {any_of, ?set([{criterion, ?criterion_ref(Ref)} || Ref <- Refs])}).

%%

construct_fixture() ->
    maps:from_list([
        {{Type, Ref}, Object}
        || Object = {Type, {_, Ref, _}} <- [
               ?dummy(41),
               ?dummy(42),
               ?dummy(43),
               ?dummy(44),
               ?dummy_link(1337, 42),
               ?dummy_link(1338, 43),
               ?category(1, <<"testCategory">>, <<"testDescription">>)
           ]
    ]).
