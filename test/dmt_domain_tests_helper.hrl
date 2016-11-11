-ifndef(dmt_domain_tests_helper_included__).
-define(dmt_domain_tests_helper_included__, yeah).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").
-include_lib("dmsl/include/dmsl_domain_thrift.hrl").

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

-endif.
