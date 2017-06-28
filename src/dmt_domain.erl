-module(dmt_domain).
-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%%

-export([new/0]).
-export([get_object/2]).
-export([apply_operations/2]).
-export([revert_operations/2]).

-define(DOMAIN, dmsl_domain_thrift).

-export_type([operation_conflict/0]).

%%

-type operation() :: dmsl_domain_config_thrift:'Operation'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type domain() :: dmsl_domain_thrift:'Domain'().
-type domain_object() :: dmsl_domain_thrift:'DomainObject'().
-type operation_conflict() ::
    {object_already_exists, object_ref()} |
    {object_not_found, object_ref()} |
    {object_reference_mismatch, object_ref()} |
    {objects_not_exist, [{object_ref(), [object_ref()]}]}.

-spec new() ->
    domain().
new() ->
    #{}.

-spec get_object(object_ref(), domain()) ->
    {ok, domain_object()} | error.
get_object(ObjectReference, Domain) ->
    maps:find(ObjectReference, Domain).

-spec apply_operations([operation()], domain()) ->
    {ok, domain()} | {error, operation_conflict()}.
apply_operations(Operations, Domain) ->
    apply_operations(Operations, Domain, #{}).

apply_operations([], Domain, Touched) ->
    case integrity_check(Domain, Touched) of
        ok ->
            {ok, Domain};
        {error, _} = Error ->
            Error
    end;
apply_operations(
    [{insert, #'InsertOp'{object = Object}} | Rest],
    Domain,
    Touched
) ->
    case insert(Object, Domain) of
        {ok, NewDomain} ->
            apply_operations(
                Rest,
                NewDomain,
                Touched#{get_ref(Object) => {insert, Object}}
            );
        {error, _} = Error ->
            Error
    end;
apply_operations(
    [{update, #'UpdateOp'{old_object = OldObject, new_object = NewObject}} | Rest],
    Domain,
    Touched
) ->
    case update(OldObject, NewObject, Domain) of
        {ok, NewDomain} ->
            apply_operations(
                Rest,
                NewDomain,
                Touched#{get_ref(NewObject) => {update, NewObject}}
            );
        {error, _} = Error ->
            Error
    end;
apply_operations(
    [{remove, #'RemoveOp'{object = Object}} | Rest],
    Domain,
    Touched
) ->
    case delete(Object, Domain) of
        {ok, NewDomain} ->
            apply_operations(
                Rest,
                NewDomain,
                Touched#{get_ref(Object) => {delete, Object}}
            );
        {error, _} = Error ->
            Error
    end.

%% TO DO: Add tests for revert_operations

-spec revert_operations([operation()], domain()) -> {ok, domain()} | {error, operation_conflict()}.
revert_operations([], Domain) ->
    {ok, Domain};
revert_operations([Operation | Rest], Domain) ->
    case apply_operations([invert_operation(Operation)], Domain) of
        {ok, NewDomain} ->
            revert_operations(Rest, NewDomain);
        {error, _} = Error ->
            Error
    end.

-spec insert(domain_object(), domain()) -> {ok, domain()} | {error, {object_already_exists, object_ref()}}.
insert(Object, Domain) ->
    ObjectReference = get_ref(Object),
    case maps:find(ObjectReference, Domain) of
        error ->
            {ok, maps:put(ObjectReference, Object, Domain)};
        {ok, ObjectWas} ->
            {error, {object_already_exists, get_ref(ObjectWas)}}
    end.

-spec update(domain_object(), domain_object(), domain()) ->
    {ok, domain()} |
    {error, {object_not_found, object_ref()} | {object_reference_mismatch, object_ref()}}.
update(OldObject, NewObject, Domain) ->
    ObjectReference = get_ref(OldObject),
    case get_ref(NewObject) of
        ObjectReference ->
            case maps:find(ObjectReference, Domain) of
                {ok, OldObject} ->
                    {ok, maps:put(ObjectReference, NewObject, Domain)};
                {ok, _ObjectWas} ->
                    {error, {object_not_found, ObjectReference}};
                error ->
                    {error, {object_not_found, ObjectReference}}
            end;
        NewObjectReference ->
            {error, {object_reference_mismatch, NewObjectReference}}
    end.

-spec delete(domain_object(), domain()) -> {ok, domain()} | {error, {object_not_found, object_ref()}}.
delete(Object, Domain) ->
    ObjectReference = get_ref(Object),
    case maps:find(ObjectReference, Domain) of
        {ok, Object} ->
            {ok, maps:remove(ObjectReference, Domain)};
        {ok, _ObjectWas} ->
            {error, {object_not_found, ObjectReference}};
        error ->
            {error, {object_not_found, ObjectReference}}
    end.

integrity_check(Domain, Touched) when is_map(Touched) ->
    integrity_check(Domain, maps:values(Touched));

integrity_check(_Domain, []) ->
    ok;

integrity_check(Domain, [{insert, Object} | Rest]) ->
    case check_correct_refs(Object, Domain) of
        ok ->
            integrity_check(Domain, Rest);
        {error, _} = Error ->
            Error
    end;

integrity_check(Domain, [{update, Object} | Rest]) ->
    case check_correct_refs(Object, Domain) of
        ok ->
            integrity_check(Domain, Rest);
        {error, _} = Error ->
            Error
    end;

integrity_check(Domain, [{delete, Object} | Rest]) ->
    case check_no_refs(Object, Domain) of
        ok ->
            integrity_check(Domain, Rest);
        {error, _} = Error ->
            Error
    end.

get_field(Field, Struct, StructInfo) when is_atom(Field) ->
    FieldIndex = get_field_index(Field, StructInfo),
    element(FieldIndex, Struct).

get_struct_info(StructName) ->
    dmsl_domain_thrift:struct_info(StructName).

get_field_info(Field, {struct, _StructType, FieldsInfo}) ->
    lists:keyfind(Field, 4, FieldsInfo).

get_field_index(Field, {struct, _StructType, FieldsInfo}) ->
    get_field_index(Field, mark_fields(FieldsInfo));

get_field_index(_Field, []) ->
    false;

get_field_index(Field, [F | Rest]) ->
    case F of
        {I, {_, _, _, Field, _}} -> I;
        _ -> get_field_index(Field, Rest)
    end.

mark_fields(FieldsInfo) ->
    lists:zip(lists:seq(2, 1 + length(FieldsInfo)), FieldsInfo).

check_correct_refs(DomainObject, Domain) ->
    NonExistent = lists:filter(
        fun(E) ->
            not object_exists(E, Domain)
        end,
        references(DomainObject)
    ),
    case NonExistent of
        [] ->
            ok;
        _ ->
            Ref = get_ref(DomainObject),
            {error, {objects_not_exist,
                lists:map(fun(X) -> {X, [Ref]} end, NonExistent)
            }}
    end.

check_no_refs(DomainObject, Domain) ->
    case referenced_by(DomainObject, Domain) of
        [] ->
            ok;
        Referenced ->
            {error, {objects_not_exist, [{get_ref(DomainObject), Referenced}]}}
    end.

referenced_by(DomainObject, Domain) ->
    Ref = get_ref(DomainObject),
    maps:fold(
        fun(_K, V, Acc) ->
            case lists:member(Ref, references(V)) of
                true -> [get_ref(V) | Acc];
                false -> Acc
            end
        end,
        [],
        Domain
    ).

references(DomainObject = {Tag, _Object}) ->
    ObjectStructInfo = get_domain_object_schema(Tag),
    Data = get_data(DomainObject),
    {_, _, DataType, _, _} = get_field_info(data, ObjectStructInfo),
    references(Data, DataType).

references(Object, FieldType) ->
    references(Object, FieldType, []).

references(undefined, _StructInfo, Refs) ->
    Refs;
references({Tag, Object}, StructInfo = {struct, union, FieldsInfo}, Refs) when is_list(FieldsInfo) ->
    {_, _, Type, _, _} = get_field_info(Tag, StructInfo),
    check_reference_type(Object, Type, Refs);
references(Object, {struct, struct, FieldsInfo}, Refs) when is_list(FieldsInfo) -> %% what if it's a union?
    lists:foldl(
        fun
            ({I, {_, _Required, FieldType, _Name, _}}, Acc) ->
                check_reference_type(element(I, Object), FieldType, Acc)
        end,
        Refs,
        mark_fields(FieldsInfo)
    );
references(Object, {struct, _, {?DOMAIN, StructName}}, Refs) ->
    StructInfo = get_struct_info(StructName),
    check_reference_type(Object, StructInfo, Refs);
references(Object, {list, FieldType}, Refs) ->
    lists:foldl(
        fun(O, Acc) ->
            check_reference_type(O, FieldType, Acc)
        end,
        Refs,
        Object
    );
references(Object, {set, FieldType}, Refs) ->
    ListObject = ordsets:to_list(Object),
    check_reference_type(ListObject, {list, FieldType}, Refs);
references(Object, {map, KeyType, ValueType}, Refs) ->
    check_reference_type(
        maps:values(Object),
        {list, ValueType},
        check_reference_type(maps:keys(Object), {list, KeyType}, Refs)
    );
references(_DomainObject, _Primitive, Refs) ->
    Refs.

check_reference_type(undefined, _, Refs) ->
    Refs;
check_reference_type(Object, Type, Refs) ->
    case is_reference_type(Type) of
        {true, Tag} ->
            [{Tag, Object} | Refs];
        false ->
            references(Object, Type, Refs)
    end.

-spec get_ref(domain_object()) -> object_ref().
get_ref(DomainObject = {Tag, _Struct}) ->
    {Tag, get_domain_object_field(ref, DomainObject)}.

-spec get_data(domain_object()) -> any().
get_data(DomainObject) ->
    get_domain_object_field(data, DomainObject).

get_domain_object_field(Field, {Tag, Struct}) ->
    get_field(Field, Struct, get_domain_object_schema(Tag)).

get_domain_object_schema(Tag) ->
    SchemaInfo = get_struct_info('DomainObject'),
    {_, _, {struct, _, {_, ObjectStructName}}, _, _} = get_field_info(Tag, SchemaInfo),
    get_struct_info(ObjectStructName).

object_exists(Ref, Domain) ->
    case get_object(Ref, Domain) of
        {ok, _Object} ->
            true;
        error ->
            false
    end.

is_reference_type(Type) ->
    {struct, union, StructInfo} = get_struct_info('Reference'),
    is_reference_type(Type, StructInfo).

is_reference_type(_Type, []) ->
    false;
is_reference_type(Type, [{_, _, Type, Tag, _} | _Rest]) ->
    {true, Tag};
is_reference_type(Type, [_ | Rest]) ->
    is_reference_type(Type, Rest).

invert_operation({insert, #'InsertOp'{object = Object}}) ->
    {remove, #'RemoveOp'{object = Object}};
invert_operation({update, #'UpdateOp'{old_object = OldObject, new_object = NewObject}}) ->
    {update, #'UpdateOp'{old_object = NewObject, new_object = OldObject}};
invert_operation({remove, #'RemoveOp'{object = Object}}) ->
    {insert, #'InsertOp'{object = Object}}.