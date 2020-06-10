-module(dmt_domain).
-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

%%

-export([new/0]).
-export([get_object/2]).
-export([apply_operations/2]).
-export([revert_operations/2]).
-export([fold/3]).
-export([insert/2]).

-define(DOMAIN, dmsl_domain_thrift).

-export_type([operation_error/0]).

%%

-type operation() :: dmsl_domain_config_thrift:'Operation'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type domain() :: dmsl_domain_thrift:'Domain'().
-type domain_object() :: dmsl_domain_thrift:'DomainObject'().

-type nonexistent_object() :: {object_ref(), [object_ref()]}.
-type operation_conflict() ::
    {object_already_exists, object_ref()} |
    {object_not_found, object_ref()} |
    {object_reference_mismatch, object_ref()}.
-type operation_invalid() ::
    {objects_not_exist, [nonexistent_object()]} |
    {object_reference_cycles, [[object_ref()]]}.
-type operation_error() ::
    {conflict, operation_conflict()} |
    {invalid, operation_invalid()}.

-type fold_function() :: fun((object_ref(), domain_object(), AccIn :: term()) -> AccOut :: term()).

-spec new() ->
    domain().
new() ->
    #{}.

-spec get_object(object_ref(), domain()) ->
    {ok, domain_object()} | error.
get_object(ObjectReference, Domain) ->
    maps:find(ObjectReference, Domain).

-spec apply_operations([operation()], domain()) ->
    {ok, domain()} | {error, operation_error()}.
apply_operations(Operations, Domain) ->
    apply_operations(Operations, Domain, []).

apply_operations([], Domain, Touched) ->
    case integrity_check(Domain, lists:reverse(Touched)) of
        ok ->
            {ok, Domain};
        {error, Invalid} ->
            {error, {invalid, Invalid}}
    end;
apply_operations(
    [Op | Rest],
    Domain,
    Touched
) ->
    {Result, Touch} = case Op of
        {insert, #'InsertOp'{object = Object}} ->
            {insert(Object, Domain), {insert, Object}};
        {update, #'UpdateOp'{old_object = OldObject, new_object = NewObject}} ->
            {update(OldObject, NewObject, Domain), {update, NewObject}};
        {remove, #'RemoveOp'{object = Object}} ->
            {remove(Object, Domain), {remove, Object}}
    end,
    case Result of
        {ok, NewDomain} ->
            apply_operations(Rest, NewDomain, [Touch | Touched]);
        {error, Conflict} ->
            {error, {conflict, Conflict}}
    end.

%% TO DO: Add tests for revert_operations

-spec revert_operations([operation()], domain()) -> {ok, domain()} | {error, operation_error()}.
revert_operations([], Domain) ->
    {ok, Domain};
revert_operations([Operation | Rest], Domain) ->
    case apply_operations([invert_operation(Operation)], Domain) of
        {ok, NewDomain} ->
            revert_operations(Rest, NewDomain);
        {error, _} = Error ->
            Error
    end.

-spec fold(fold_function(), term(), domain()) -> term().

fold(Fun, AccIn, Domain) ->
    maps:fold(Fun, AccIn, Domain).

-spec insert(domain_object(), domain()) ->
    {ok, domain()} |
    {error, {object_already_exists, object_ref()}}.
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
    {error,
        {object_not_found, object_ref()} |
        {object_reference_mismatch, object_ref()}
    }.
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

-spec remove(domain_object(), domain()) ->
    {ok, domain()} |
    {error, {object_not_found, object_ref()}}.
remove(Object, Domain) ->
    ObjectReference = get_ref(Object),
    case maps:find(ObjectReference, Domain) of
        {ok, Object} ->
            {ok, maps:remove(ObjectReference, Domain)};
        {ok, _ObjectWas} ->
            {error, {object_not_found, ObjectReference}};
        error ->
            {error, {object_not_found, ObjectReference}}
    end.

-type touch() :: {insert | update | remove, domain_object()}.

-spec integrity_check(domain(), [touch()]) ->
    ok |
    {error,
        {objects_not_exist, [nonexistent_object()]} |
        {object_reference_cycles, [[object_ref()]]}
    }.
integrity_check(Domain, Touched) when is_list(Touched) ->
    % TODO
    % Well I guess nothing (but the types) stops us from accumulating
    % errors from every check, instead of just first failed
    run_until_error([
        fun () -> verify_integrity(Domain, Touched, []) end,
        fun () -> verify_acyclicity(Domain, Touched, []) end
    ]).

run_until_error([CheckFun | Rest]) ->
    case CheckFun() of
        ok ->
            run_until_error(Rest);
        {error, _} = Error ->
            Error
    end;
run_until_error([]) ->
    ok.

verify_integrity(_Domain, [], []) ->
    ok;
verify_integrity(_Domain, [], ObjectsNotExist) ->
    {error, {objects_not_exist, ObjectsNotExist}};
verify_integrity(Domain, [{Op, Object} | Rest], Acc) when Op == insert; Op == update ->
    ObjectsNotExist = check_correct_refs(Object, Domain),
    verify_integrity(Domain, Rest, Acc ++ ObjectsNotExist);
verify_integrity(Domain, [{remove, Object} | Rest], Acc) ->
    ObjectsNotExist = check_no_refs(Object, Domain),
    verify_integrity(Domain, Rest, Acc ++ ObjectsNotExist).

verify_acyclicity(_Domain, [], []) ->
    ok;
verify_acyclicity(_Domain, [], Cycles) ->
    {error, {object_reference_cycles, Cycles}};
verify_acyclicity(Domain, [{Op, Object} | Rest], Acc) when Op == insert; Op == update ->
    Ref = get_ref(Object),
    Acc1 = case is_already_in_cycle(Ref, Acc) of
        true  -> Acc;
        false -> track_cycles(Object, [Ref], Acc, Domain)
    end,
    verify_acyclicity(Domain, Rest, Acc1);
verify_acyclicity(Domain, [{remove, _} | Rest], Acc) ->
    verify_acyclicity(Domain, Rest, Acc).

is_already_in_cycle(Ref, Cycles) ->
    lists:any(fun (Cycle) -> lists:member(Ref, Cycle) end, Cycles).

check_correct_refs(DomainObject, Domain) ->
    NonExistent = lists:filter(
        fun(E) ->
            not object_exists(E, Domain)
        end,
        references(DomainObject)
    ),
    Ref = get_ref(DomainObject),
    lists:map(fun(X) -> {X, [Ref]} end, NonExistent).

object_exists(Ref, Domain) ->
    case get_object(Ref, Domain) of
        {ok, _Object} ->
            true;
        error ->
            false
    end.

check_no_refs(DomainObject, Domain) ->
    case referenced_by(DomainObject, Domain) of
        [] ->
            [];
        Referenced ->
            [{get_ref(DomainObject), Referenced}]
    end.

track_cycles(DomainObject, PathRev, CyclesAcc, Domain) ->
    Refs = references(DomainObject),
    CycleRefs = [Ref || Ref <- Refs, lists:member(Ref, PathRev)],
    Cycles = case CycleRefs of
        [] ->
            [];
        _ ->
            Path = lists:reverse(PathRev),
            [lists:dropwhile(fun (PathRef) -> Ref =/= PathRef end, Path) || Ref <- CycleRefs]
    end,
    lists:foldl(
        fun (Ref, Acc) ->
            case lists:member(Ref, CycleRefs) of
                false ->
                    track_cycles_by_ref(Ref, PathRev, Acc, Domain);
                true ->
                    Acc
            end
        end,
        Cycles ++ CyclesAcc,
        Refs
    ).

track_cycles_by_ref(Ref, PathRev, CyclesAcc, Domain) ->
    case get_object(Ref, Domain) of
        {ok, NextObject} ->
            track_cycles(NextObject, [Ref | PathRev], CyclesAcc, Domain);
        error ->
            CyclesAcc
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

references(DomainObject) ->
    {DataType, Data} = get_data(DomainObject),
    references(Data, DataType).

references(Object, DataType) ->
    references(Object, DataType, []).

references(undefined, _StructInfo, Refs) ->
    Refs;
references({Tag, Object}, StructInfo = {struct, union, FieldsInfo}, Refs) when is_list(FieldsInfo) ->
    case get_field_info(Tag, StructInfo) of
        false ->
            erlang:error({<<"field info not found">>, Tag, StructInfo});
        {_, _, Type, _, _} ->
            check_reference_type(Object, Type, Refs)
    end;
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
    {_Type, Ref} = get_domain_object_field(ref, DomainObject),
    {Tag, Ref}.

-spec get_data(domain_object()) -> any().
get_data(DomainObject) ->
    get_domain_object_field(data, DomainObject).

get_domain_object_field(Field, {Tag, Struct}) ->
    get_field(Field, Struct, get_domain_object_schema(Tag)).

get_domain_object_schema(Tag) ->
    SchemaInfo = get_struct_info('DomainObject'),
    {_, _, {struct, _, {_, ObjectStructName}}, _, _} = get_field_info(Tag, SchemaInfo),
    get_struct_info(ObjectStructName).

get_field(Field, Struct, StructInfo) when is_atom(Field) ->
    {FieldIndex, {_, _, Type, _, _}} = get_field_index(Field, StructInfo),
    {Type, element(FieldIndex, Struct)}.

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
        {_, {_, _, _, Field, _}} = Index ->
            Index;
        _ ->
            get_field_index(Field, Rest)
    end.

mark_fields(FieldsInfo) ->
    lists:zip(lists:seq(2, 1 + length(FieldsInfo)), FieldsInfo).

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
