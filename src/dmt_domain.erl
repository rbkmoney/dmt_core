-module(dmt_domain).
-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%%

-export([new/0]).
-export([get_object/2]).
-export([apply_operations/2]).
-export([revert_operations/2]).

%%

-spec new() ->
    dmt:domain().
new() ->
    #{}.

-spec get_object(dmt:object_ref(), dmt:domain()) ->
    {ok, dmt:domain_object()} | error.
get_object(ObjectReference, Domain) ->
    maps:find(ObjectReference, Domain).

-spec apply_operations([dmt:operation()], dmt:domain()) -> dmt:domain() | no_return().
apply_operations([], Domain) ->
    Domain;
apply_operations([{insert, #'InsertOp'{object = Object}} | Rest], Domain) ->
    apply_operations(Rest, insert(Object, Domain));
apply_operations([{update, #'UpdateOp'{old_object = OldObject, new_object = NewObject}} | Rest], Domain) ->
    apply_operations(Rest, update(OldObject, NewObject, Domain));
apply_operations([{remove, #'RemoveOp'{object = Object}} | Rest], Domain) ->
    apply_operations(Rest, delete(Object, Domain)).

-spec revert_operations([dmt:operation()], dmt:domain()) -> dmt:domain() | no_return().
revert_operations([], Domain) ->
    Domain;
revert_operations([{insert, #'InsertOp'{object = Object}} | Rest], Domain) ->
    revert_operations(Rest, delete(Object, Domain));
revert_operations([{update, #'UpdateOp'{old_object = OldObject, new_object = NewObject}} | Rest], Domain) ->
    revert_operations(Rest, update(NewObject, OldObject, Domain));
revert_operations([{remove, #'RemoveOp'{object = Object}} | Rest], Domain) ->
    revert_operations(Rest, insert(Object, Domain)).

-spec insert(dmt:domain_object(), dmt:domain()) -> dmt:domain() | no_return().
insert(Object, Domain) ->
    ObjectReference = get_ref(Object),
    ok = check_correct_refs(Object, Domain),
    case maps:find(ObjectReference, Domain) of
        error ->
            maps:put(ObjectReference, Object, Domain);
        {ok, ObjectWas} ->
            raise_conflict({object_already_exists, ObjectWas})
    end.

-spec update(dmt:domain_object(), dmt:domain_object(), dmt:domain()) -> dmt:domain() | no_return().
update(OldObject, NewObject, Domain) ->
    ObjectReference = get_ref(OldObject),
    ok = check_correct_refs(NewObject, Domain),
    case get_ref(NewObject) of
        ObjectReference ->
            case maps:find(ObjectReference, Domain) of
                {ok, OldObject} ->
                    maps:put(ObjectReference, NewObject, Domain);
                {ok, _ObjectWas} ->
                    raise_conflict({object_not_found, OldObject});
                error ->
                    raise_conflict({object_not_found, OldObject})
            end;
        NewObjectReference ->
            raise_conflict({object_reference_mismatch, NewObjectReference})
    end.

-spec delete(dmt:domain_object(), dmt:domain()) -> dmt:domain() | no_return().
delete(Object, Domain) ->
    ObjectReference = get_ref(Object),
    ok = check_no_refs(Object, Domain),
    case maps:find(ObjectReference, Domain) of
        {ok, Object} ->
            maps:remove(ObjectReference, Domain);
        {ok, _ObjectWas} ->
            raise_conflict({object_not_found, Object});
        error ->
            raise_conflict({object_not_found, Object})
    end.

-spec get_ref(dmt:domain_object()) -> dmt:object_ref().
get_ref({Tag, Struct}) ->
    {Tag, get_field(ref, Struct)}.

-spec raise_conflict(tuple()) -> no_return().
raise_conflict(Why) ->
    throw({conflict, Why}).

-spec get_data(dmt:domain_object()) -> any().
get_data({_Tag, Struct}) ->
    get_field(data, Struct).

get_field(Field, Struct) when is_atom(Field) ->
    StructName = get_struct_name(Struct),
    StructInfo = get_struct_info(StructName),
    FieldInfo  = get_field_info(Field, StructInfo),
    FieldIndex = get_field_index(FieldInfo),
    get_field(FieldIndex, Struct);
get_field(FieldIndex, Struct) when is_integer(FieldIndex) ->
    element(FieldIndex + 1, Struct).

get_struct_name(Struct) when is_tuple(Struct) ->
    RecordName = element(1, Struct),
    get_struct_name(RecordName);

get_struct_name(RecordName) when is_atom(RecordName) ->
    get_struct_name(RecordName, dmsl_domain_thrift:structs()).

get_struct_name(RecordName, []) ->
    error({badarg, RecordName});

get_struct_name(RecordName, [StructName | Tail]) ->
    try
        case dmsl_domain_thrift:record_name(StructName) of
            RecordName -> StructName;
            _ -> get_struct_name(RecordName, Tail)
        end
    catch
        error:badarg ->
            get_struct_name(RecordName, Tail)
    end.

get_struct_info(StructName) ->
    dmsl_domain_thrift:struct_info(StructName).

get_field_info(Field, {struct, _StructType, FieldInfo}) ->
    lists:keyfind(Field, 4, FieldInfo).

get_field_index({Index, _Required, _Info, _Name, _}) ->
    Index.

check_correct_refs(DomainObject, Domain) ->
    {_, NonExistent} = lists:partition(
        fun(E) ->
            object_exists(E, Domain)
        end,
        referenced_to(DomainObject)
    ),
    case NonExistent of
        [] ->
            ok;
        _ ->
            integrity_check_failed({non_existent, NonExistent})
    end.

check_no_refs(DomainObject, Domain) ->
    case referenced_by(DomainObject, Domain) of
        [] ->
            ok;
        Referenced ->
            integrity_check_failed({referenced, Referenced})
    end.

referenced_by(DomainObject, Domain) ->
    {_Tag, Ref} = get_ref(DomainObject),
    Values = [V ||{_K, V} <- maps:to_list(Domain)],
    lists:foldl(
        fun(V, Acc) ->
            Data = get_data(V),
            case has_ref(Ref, Data) of
                true -> [V | Acc];
                false -> Acc
            end
        end,
        [],
        Values
    ).

referenced_to(DomainObject) ->
    [_Type | Fields] = erlang:tuple_to_list(get_data(DomainObject)),
    lists:foldl(
        fun(MaybeRef, Acc) ->
            [T | _] = erlang:tuple_to_list(MaybeRef),
            case is_reference_type(T) of
                {true, Tag} ->
                    [{Tag, MaybeRef} | Acc];
                false ->
                    Acc
            end
        end,
        [],
        Fields
    ).

object_exists(Ref, Domain) ->
    case maps:find(Ref, Domain) of
        {ok, _Object} ->
            true;
        error ->
            false
    end.

has_ref(Ref, Struct) when is_tuple(Struct) ->
    [_Type | Fields] = erlang:tuple_to_list(Struct),
    lists:member(Ref, Fields);
has_ref(Ref, List) when is_list(List) ->
    lists:any(fun (Element) -> has_ref(Ref, Element) end, List);
has_ref(Ref, Map) when is_map(Map) ->
    List = maps:fold(
        fun(K, V, Acc) ->
            [K, V | Acc]
        end,
        [],
        Map
    ),
    has_ref(Ref, List);
has_ref(_Ref, _Field) ->
    false.

is_reference_type(Type) ->
    StructName = get_struct_name(Type),
    {struct, union, StructInfo} = dmsl_domain_thrift:struct_info('Reference'),
    is_reference_type(StructName, StructInfo).

is_reference_type(_Type, []) ->
    false;
is_reference_type(Type, [{_, _, {_, _, {_, Type}}, Tag, _} | _Rest]) ->
    {true, Tag};
is_reference_type(Type, [_ | Rest]) ->
    is_reference_type(Type, Rest).

-spec integrity_check_failed(Reason :: term()) -> no_return().
integrity_check_failed(Reason) ->
    throw({integrity_check_failed, Reason}).
