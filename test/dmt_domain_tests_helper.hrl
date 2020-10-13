-ifndef(dmt_domain_tests_helper_included__).
-define(dmt_domain_tests_helper_included__, yeah).

-include_lib("damsel/include/dmsl_domain_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

-define(dummy_ref(ID), {dummy, #domain_DummyRef{id = ID}}).
-define(dummy_link_ref(ID), {dummy_link, #domain_DummyLinkRef{id = ID}}).

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

-define(category_ref(ID), #domain_CategoryRef{id = ID}).
-define(ext_account_set_ref(ID), #domain_ExternalAccountSetRef{id = ID}).
-define(pinst_ref(ID), #domain_PaymentInstitutionRef{id = ID}).
-define(provider_ref(ID), #domain_ProviderRef{id = ID}).
-define(system_account_set_ref(ID), #domain_SystemAccountSetRef{id = ID}).
-define(currency_ref(ID), #domain_CurrencyRef{symbolic_code = ID}).
-define(sas_ref(ID), #domain_SystemAccountSetRef{id = ID}).
-define(terminal_ref(ID), #domain_TerminalRef{id = ID}).
-define(payment_method_ref(ID), #domain_PaymentMethodRef{id = ID}).
-define(contract_template_ref(ID), #domain_ContractTemplateRef{id = ID}).
-define(inspector_ref(ID), #domain_InspectorRef{id = ID}).

-define(category(ID, Name, Description),
    {category, #domain_CategoryObject{
        ref = ?category_ref(ID),
        data = #domain_Category{
            name = Name,
            description = Description
        }
    }}
).

-define(criterion_ref(ID), #domain_CriterionRef{id = ID}).
-define(criterion(ID, Name, Pred),
    {criterion, #domain_CriterionObject{
        ref = ?criterion_ref(ID),
        data = #domain_Criterion{
            name = Name,
            predicate = Pred
        }
    }}
).

-define(insert(O), {insert, #'InsertOp'{object = O}}).
-define(remove(O), {remove, #'RemoveOp'{object = O}}).
-define(update(O1, O2), {update, #'UpdateOp'{old_object = O1, new_object = O2}}).

-define(set(L),
    (begin
        true = is_list(L),
        ordsets:from_list(L)
    end)
).

-endif.
