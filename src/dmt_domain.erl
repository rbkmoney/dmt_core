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
    ok = check_refs(Object, Domain),
    case maps:find(ObjectReference, Domain) of
        error ->
            maps:put(ObjectReference, Object, Domain);
        {ok, ObjectWas} ->
            raise_conflict({object_already_exists, ObjectWas})
    end.

-spec update(dmt:domain_object(), dmt:domain_object(), dmt:domain()) -> dmt:domain() | no_return().
update(OldObject, NewObject, Domain) ->
    ObjectReference = get_ref(OldObject),
    ok = check_refs(NewObject, Domain),
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
    ok = check_no_refs(ObjectReference, Domain),
    case maps:find(ObjectReference, Domain) of
        {ok, Object} ->
            maps:remove(ObjectReference, Domain);
        {ok, _ObjectWas} ->
            raise_conflict({object_not_found, Object});
        error ->
            raise_conflict({object_not_found, Object})
    end.

%%TODO:elaborate
-spec get_ref(dmt:domain_object()) -> dmt:object_ref().
get_ref({Tag, {_Type, Ref, _Data}}) ->
    {Tag, Ref}.

-spec raise_conflict(tuple()) -> no_return().

raise_conflict(Why) ->
    throw({conflict, Why}).

%%TODO:elaborate
-spec get_data(dmt:domain_object()) -> any().
get_data({_Tag, {_Type, _Ref, Data}}) ->
    Data.

check_refs(DomainObject, Domain) ->
    [_Type | Fields] = erlang:tuple_to_list(get_data(DomainObject)),
    case lists:all(fun (Object) -> check_ref(Object, Domain) end, Fields) of
        true ->
            ok;
        false ->
            throw(integrity_check_failed)
    end.

check_ref(MaybeRef, Domain) ->
    [Type | _Fields] = erlang:tuple_to_list(MaybeRef),
    case is_reference_type(Type) of
        {true, Tag} ->
            object_exists({Tag, MaybeRef}, Domain);
        false ->
            true
    end.

object_exists(Ref, Domain) ->
    case maps:find(Ref, Domain) of
        {ok, _Object} ->
            true;
        error ->
            false
    end.


check_no_refs({_Tag, Ref}, Domain) ->
    case has_ref(Ref, [get_data(V) ||{_K, V} <- maps:to_list(Domain)]) of
        true ->
            throw(integrity_check_failed);
        false ->
            ok
    end.

has_ref(Ref, Struct) when is_tuple(Struct) ->
    [_Type | Fields] = erlang:tuple_to_list(Struct),
    lists:member(Ref, Fields);
has_ref(Ref, List) when is_list(List) ->
    lists:any(fun (Element) -> has_ref(Ref, Element) end, List);
has_ref(Ref, Map) when is_map(Map) ->
    has_ref(Ref, [V || {_K, V} <- maps:to_list(Map)]).

is_reference_type(Type) ->
    {struct, union, StructInfo} = dmt_domain_thrift:struct_info('Reference'),
    is_reference_type(Type, StructInfo).

is_reference_type(_Type, []) ->
    false;
is_reference_type(Type, [{_, _, {_, _, {_, Type}}, Tag, _} | _Rest]) ->
    {true, Tag};
is_reference_type(Type, [_ | Rest]) ->
    is_reference_type(Type, Rest).
