%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Anonymous function extraction API.
%%
%% This module is responsible for extracting the code of an anonymous
%% function. The goal is to be able to store the extracted function and
%% execute it later, regardless of the available of the initial Erlang module
%% which declared it.
%%
%% This module also provides a way for the caller to indicate forbidden
%% operations or function calls.
%%
%% This module works on assembly code to perform all checks and prepare the
%% storable copy of a function. It uses {@link beam_disasm:file/1} from the
%% `compiler' application to extract the assembly code. After the assembly
%% code was extracted and modified, the compiler is used again to compile the
%% code back to an executable module.
%%
%% If the anonymous function calls other functions, either in the same module
%% or in another one, the code of the called functions is extracted and copied
%% as well. This is to make sure the result is completely standalone.
%%
%% To avoid any copies of standard Erlang APIs or Khepri itself, it is
%% possible to specify a list of modules which should not be copied. In this
%% case, calls to functions in those modules are left unmodified.
%%
%% Once the code was extracted and verified, a new module is generated as an
%% "assembly form", ready to be compiled again to an executable module. The
%% generated module has a single `run/N' function. This function contains the
%% code of the extracted anonymous function.
%%
%% Because this process works on the assembly code, it means that if the
%% initial module hosting the anonymous function was compiled with Erlang
%% version N, it will probably not compile or run on older versions of Erlang.
%% The reason is that a newer compiler may use instructions which are unknown
%% to older runtimes.

-module(khepri_fun).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("src/internal.hrl").

-export([to_standalone_fun/2,
         exec/2]).

%% FIXME: compile:forms/2 is incorrectly specified and doesn't accept
%% assembly. This breaks compile/1 and causes a cascade of errors.
%%
%% The following basically disable Dialyzer for this module unfortunately...
-dialyzer({nowarn_function, [compile/1,
                             to_standalone_fun/2,
                             to_standalone_env/1,
                             to_standalone_arg/2]}).

-type fun_info() :: [{arity | env | index | name | module | new_index |
                      new_uniq | pid | type | uniq, any()}].
-type beam_instr() :: atom() | tuple().
-type label() :: pos_integer().

-type ensure_instruction_is_permitted_fun() ::
fun((beam_instr()) -> ok).

-type should_process_function_fun() ::
fun((module(), atom(), arity(), module()) -> boolean()).

-type standalone_fun() :: #standalone_fun{} | fun().
-type options() :: #{ensure_instruction_is_permitted =>
                     ensure_instruction_is_permitted_fun(),
                     should_process_function =>
                     should_process_function_fun()}.

-export_type([standalone_fun/0,
              options/0]).

%% -------------------------------------------------------------------
%% Taken from lib/compiler/src/beam_disasm.hrl,
%% commit 7b3ffa5bb72a2ba84b07fb8a98d755216e78fa79
-record(function, {name      :: atom(),
                   arity     :: byte(),
                   entry     :: beam_lib:label(),    %% unnecessary ?
                   code = [] :: [beam_instr()]}).

-record(beam_file, {module               :: module(),
                    labeled_exports = [] :: [beam_lib:labeled_entry()],
                    attributes      = [] :: [beam_lib:attrib_entry()],
                    compile_info    = [] :: [beam_lib:compinfo_entry()],
                    code            = [] :: [#function{}]}).
%% -------------------------------------------------------------------

-record(state, {generated_module_name :: module() | undefined,
                entrypoint :: mfa() | undefined,
                checksums = #{} :: #{module() => binary()},
                fun_info :: fun_info(),
                calls = #{} :: #{mfa() => true},
                functions = #{} :: #{mfa() => #function{}},

                mfa_in_progress :: mfa() | undefined,
                function_in_progress :: atom() | undefined,
                next_label = 1 :: label(),
                label_map = #{} :: #{{module(), label()} => label()},

                options = #{} :: options()}).

-type asm() :: {module(),
                [{atom(), arity()}],
                [],
                [#function{}],
                label()}.

-spec to_standalone_fun(Fun, Options) -> StandaloneFun when
      Fun :: fun(),
      Options :: options(),
      StandaloneFun :: standalone_fun().

to_standalone_fun(Fun, Options) ->
    Info = erlang:fun_info(Fun),
    State0 = #state{fun_info = Info,
                    options = Options},

    %% Don't copy functions like "fun dict:new/0" which are not meant to be
    %% copied.
    Module = proplists:get_value(module, Info),
    Name = proplists:get_value(name, Info),
    Arity = proplists:get_value(arity, Info),
    ShouldProcess = case proplists:get_value(type, Info) of
                        local ->
                            should_process_function(
                              Module, Name, Arity, Module, State0);
                        external ->
                            should_process_function(
                              Module, Name, Arity, undefined, State0)
                    end,
    case ShouldProcess of
        true ->
            State1 = pass1(State0),
            Asm = pass2(State1),
            {GeneratedModuleName, Beam} = compile(Asm),

            Arity = proplists:get_value(arity, Info),

            %% Fun environment to standalone term.
            Env = to_standalone_env(State1),

            #standalone_fun{module = GeneratedModuleName,
                            beam = Beam,
                            arity = Arity,
                            env = Env};
        false ->
            Fun
    end.

-spec compile(Asm) -> {Module, Beam} when
      Asm :: asm(), %% FIXME: compile:forms/2 is incorrectly specified.
      Module :: module(),
      Beam :: binary().

compile(Asm) ->
    CompilerOptions = [from_asm,
                       binary,
                       warnings_as_errors,
                       return_errors,
                       return_warnings,
                       deterministic],
    case compile:forms(Asm, CompilerOptions) of
        {ok, Module, Beam, []} -> {Module, Beam};
        Error                  -> throw({compilation_failure, Error})
    end.

-spec exec(StandaloneFun, Args) -> Ret when
      StandaloneFun :: standalone_fun(),
      Args :: [any()],
      Ret :: any().

exec(
  #standalone_fun{module = Module,
                  beam = Beam,
                  arity = Arity,
                  env = Env},
  Args) ->
    ?assertEqual(Arity, length(Args)),
    case code:is_loaded(Module) of
        false ->
            {module, _} = code:load_binary(Module, ?MODULE_STRING, Beam),
            ok;
        _ ->
            ok
    end,
    Env1 = to_actual_arg(Env),
    erlang:apply(Module, run, Args ++ Env1);
exec(Fun, Args) ->
    erlang:apply(Fun, Args).

%% -------------------------------------------------------------------
%% Code processing [Pass 1]
%% -------------------------------------------------------------------

-spec pass1(State) -> State when
      State :: #state{}.

pass1(
  #state{fun_info = Info,
         checksums = Checksums} = State) ->
    Module = proplists:get_value(module, Info),
    Name = proplists:get_value(name, Info),
    Arity = proplists:get_value(arity, Info),
    Env = proplists:get_value(env, Info),

    %% Internally, a lambda which takes arguments and values from its
    %% environment (i.e. variables declared in the function which defined that
    %% lambda).
    InternalArity = Arity + length(Env),

    State1 = case proplists:get_value(type, Info) of
                 local ->
                     Checksum = proplists:get_value(new_uniq, Info),
                     ?assert(is_binary(Checksum)),
                     Checksums1 = Checksums#{Module => Checksum},
                     State#state{checksums = Checksums1};
                 external ->
                     State
             end,
    State2 = State1#state{entrypoint = {Module, Name, InternalArity}},

    pass1_process_function(Module, Name, InternalArity, State2).

-spec pass1_process_function(Module, Name, Arity, State) -> State when
      Module :: module(),
      Name :: atom(),
      Arity :: arity(),
      State :: #state{}.

pass1_process_function(
  Module, Name, Arity,
  State) ->
    MFA = {Module, Name, Arity},
    State1 = State#state{mfa_in_progress = MFA,
                         calls = #{}},
    {Function0, State2} = lookup_function(Module, Name, Arity, State1),
    {Function1, State3} = pass1_process_function_code(Function0, State2),

    #state{calls = Calls,
           functions = Functions} = State3,
    Functions1 = Functions#{MFA  => Function1},
    State4 = State3#state{functions = Functions1},

    %% Recurse with called functions.
    maps:fold(
      fun({M, F, A}, true, St) ->
              pass1_process_function(M, F, A, St)
      end, State4, Calls).

-spec pass1_process_function_code(Function, State) -> {Function, State} when
      Function :: #function{},
      State :: #state{}.

pass1_process_function_code(
  #function{entry = OldEntryLabel,
            code = Instructions} = Function,
  #state{mfa_in_progress = {Module, _, _},
         next_label = NextLabel} = State) ->
    %% Compute label diff.
    {label, FirstLabel} = lists:keyfind(label, 1, Instructions),
    LabelDiff = NextLabel - FirstLabel,

    %% pass1_process_instructions
    {Instructions1, State1} = pass1_process_instructions(Instructions, State),

    %% Compute its new entry label.
    #state{label_map = LabelMap} = State1,
    LabelKey = {Module, OldEntryLabel},
    NewEntryLabel = maps:get(LabelKey, LabelMap),
    ?assertEqual(LabelDiff, NewEntryLabel - OldEntryLabel),

    %% Rename function & fix its entry label.
    Function1 = Function#function{
                  entry = NewEntryLabel,
                  code = Instructions1},

    {Function1, State1}.

-spec pass1_process_instructions(Instructions, State) ->
    {Instructions, State} when
      Instructions :: [beam_instr()],
      State :: #state{}.

pass1_process_instructions(Instructions, State) ->
    %% Check allowed & denied instructions.
    %% Record all calls that need their code to be copied.
    %% Adjust label based on label diff.
    pass1_process_instructions(Instructions, State, []).

pass1_process_instructions(
  [{Call, Arity, {Module, Name, Arity}} = Instruction | Rest],
  State,
  Result)
  when Call =:= call orelse Call =:= call_only ->
    ensure_instruction_is_permitted(Instruction, State),
    State1 = pass1_process_call(Module, Name, Arity, State),
    pass1_process_instructions(Rest, State1, [Instruction | Result]);
pass1_process_instructions(
  [{Call, Arity, {extfunc, Module, Name, Arity}} = Instruction | Rest],
  State,
  Result)
  when Call =:= call_ext orelse Call =:= call_ext_only ->
    ensure_instruction_is_permitted(Instruction, State),
    State1 = pass1_process_call(Module, Name, Arity, State),
    pass1_process_instructions(Rest, State1, [Instruction | Result]);
pass1_process_instructions(
  [{call_last, Arity, {Module, Name, Arity}, _} = Instruction
   | Rest],
  State,
  Result) ->
    ensure_instruction_is_permitted(Instruction, State),
    State1 = pass1_process_call(Module, Name, Arity, State),
    pass1_process_instructions(Rest, State1, [Instruction | Result]);
pass1_process_instructions(
  [{call_ext_last, Arity, {extfunc, Module, Name, Arity}, _} = Instruction
   | Rest],
  State,
  Result) ->
    ensure_instruction_is_permitted(Instruction, State),
    State1 = pass1_process_call(Module, Name, Arity, State),
    pass1_process_instructions(Rest, State1, [Instruction | Result]);
pass1_process_instructions(
  [{label, OldLabel} | Rest],
  #state{mfa_in_progress = {Module, _, _},
         next_label = NewLabel,
         label_map = LabelMap} = State,
  Result) ->
    Instruction = {label, NewLabel},
    LabelKey = {Module, OldLabel},
    ?assertNot(maps:is_key(LabelKey, LabelMap)),
    LabelMap1 = LabelMap#{LabelKey => NewLabel},
    State1 = State#state{next_label = NewLabel + 1,
                         label_map = LabelMap1},
    pass1_process_instructions(Rest, State1, [Instruction | Result]);
pass1_process_instructions(
  [{line, _} | Rest],
  State,
  Result) ->
    %% Drop this instruction.
    pass1_process_instructions(Rest, State, Result);
pass1_process_instructions(
  [{make_fun2, {Module, Name, Arity}, _, _, _} = Instruction | Rest],
  State,
  Result) ->
    ensure_instruction_is_permitted(Instruction, State),
    State1 = pass1_process_call(Module, Name, Arity, State),
    pass1_process_instructions(Rest, State1, [Instruction | Result]);
pass1_process_instructions(
  [{make_fun3, {Module, Name, Arity}, _, _, _, _} = Instruction | Rest],
  State,
  Result) ->
    ensure_instruction_is_permitted(Instruction, State),
    State1 = pass1_process_call(Module, Name, Arity, State),
    pass1_process_instructions(Rest, State1, [Instruction | Result]);
pass1_process_instructions(
  [Instruction | Rest],
  State,
  Result) ->
    ensure_instruction_is_permitted(Instruction, State),
    pass1_process_instructions(Rest, State, [Instruction | Result]);
pass1_process_instructions(
  [],
  State,
  Result) ->
    {lists:reverse(Result), State}.

-spec pass1_process_call(Module, Name, Arity, State) -> State when
      Module :: module(),
      Name :: atom(),
      Arity :: arity(),
      State :: #state{}.

pass1_process_call(
  Module, Name, Arity,
  #state{mfa_in_progress = {Module, Name, Arity}} = State) ->
    State;
pass1_process_call(
  Module, Name, Arity,
  #state{mfa_in_progress = {FromModule, _, _},
         functions = Functions,
         calls = Calls} = State) ->
    case should_process_function(Module, Name, Arity, FromModule, State) of
        true ->
            CallKey = {Module, Name, Arity},
            case Functions of
                #{CallKey := _} ->
                    State;
                _ ->
                    Calls1 = Calls#{CallKey => true},
                    State#state{calls = Calls1}
            end;
        false ->
            State
    end.

-spec lookup_function(Module, Name, Arity, State) -> {Function, State} when
      Module :: module(),
      Name :: atom(),
      Arity :: non_neg_integer() | undefined,
      State :: #state{},
      Function :: #function{}.

lookup_function(Module, Name, Arity, State) ->
    {#beam_file{code = Code}, State1} = disassemble_module(Module, State),
    {lookup_function1(Code, Name, Arity), State1}.

lookup_function1(
  [#function{name = Name, arity = Arity} = Function | _],
  Name, Arity) when is_integer(Arity) ->
    Function;
lookup_function1(
  [#function{name = Name} = Function | _],
  Name, undefined) ->
    Function;
lookup_function1(
  [_ | Rest],
  Name, Arity) ->
    lookup_function1(Rest, Name, Arity).

-spec disassemble_module(Module, State) -> {BeamFileRecord, State} when
      Module :: module(),
      State :: #state{},
      BeamFileRecord :: #beam_file{}.

-define(ASM_CACHE_KEY(Module, Checksum),
        {?MODULE, asm_cache, Module, Checksum}).

disassemble_module(Module, #state{checksums = Checksums} = State) ->
    case Checksums of
        #{Module := Checksum} ->
            {BeamFileRecord, Checksum} = disassemble_module1(
                                           Module, Checksum),
            {BeamFileRecord, State};
        _ ->
            {BeamFileRecord, Checksum} = disassemble_module1(
                                           Module, undefined),
            ?assert(is_binary(Checksum)),
            Checksums1 = Checksums#{Module => Checksum},
            State1 = State#state{checksums = Checksums1},
            {BeamFileRecord, State1}
    end.

disassemble_module1(Module, Checksum) when is_binary(Checksum) ->
    Key = ?ASM_CACHE_KEY(Module, Checksum),
    case persistent_term:get(Key, undefined) of
        #beam_file{} = BeamFileRecord ->
            {BeamFileRecord, Checksum};
        undefined ->
            {Module, Beam, _} = code:get_object_code(Module),
            {ok, {Module, ActualChecksum}} = beam_lib:md5(Beam),
            case ActualChecksum of
                Checksum ->
                    BeamFileRecord = do_disassemble_and_cache(
                                       Module, Checksum, Beam),
                    {BeamFileRecord, Checksum};
                _ ->
                    throw(
                      {mismatching_module_checksum,
                       Module, Checksum, ActualChecksum})
            end
    end;
disassemble_module1(Module, undefined) ->
    {Module, Beam, _} = code:get_object_code(Module),
    {ok, {Module, Checksum}} = beam_lib:md5(Beam),
    BeamFileRecord = do_disassemble_and_cache(Module, Checksum, Beam),
    {BeamFileRecord, Checksum}.

do_disassemble_and_cache(Module, Checksum, Beam) ->
    Key = ?ASM_CACHE_KEY(Module, Checksum),
    BeamFileRecord = beam_disasm:file(Beam),
    persistent_term:put(Key, BeamFileRecord),
    BeamFileRecord.

-spec ensure_instruction_is_permitted(Instruction, State) ->
    ok | no_return() when
      Instruction :: beam_instr(),
      State :: #state{}.

ensure_instruction_is_permitted(
  Instruction,
  #state{options = #{ensure_instruction_is_permitted := Callback}})
  when is_function(Callback) ->
    Callback(Instruction);
ensure_instruction_is_permitted(_Instruction, _State) ->
    ok.

-spec should_process_function(Module, Name, Arity, FromModule, State) ->
    ShouldProcess | no_return() when
      Module :: module(),
      Name :: atom(),
      Arity :: arity(),
      FromModule :: module(),
      State :: #state{},
      ShouldProcess :: boolean().

should_process_function(
  Module, Name, Arity, FromModule,
  #state{options = #{should_process_function := Callback}})
  when is_function(Callback) ->
    Callback(Module, Name, Arity, FromModule);
should_process_function(_Module, _Name, _Arity, _FromModule, _State) ->
    false.

%% -------------------------------------------------------------------
%% Code processing [Pass 2]
%% -------------------------------------------------------------------

-spec pass2(State) -> Asm when
      State :: #state{},
      Asm :: asm().

pass2(
  #state{functions = Functions,
         next_label = NextLabel} = State) ->
    %% The module name is based on a hash of its entire code.
    GeneratedModuleName = gen_module_name(Functions, State),
    State1 = State#state{generated_module_name = GeneratedModuleName},

    Functions1 = pass2_process_functions(Functions, State1),

    %% Sort functions by their entrypoint label.
    Functions2 = lists:sort(
                   fun(#function{entry = EntryA},
                       #function{entry = EntryB}) ->
                           EntryA < EntryB
                   end, maps:values(Functions1)),

    %% The first function (the lambda) is the only one exported.
    [#function{name = Name, arity = Arity} | _] = Functions2,
    Exports = [{Name, Arity}],

    Attributes = [],
    Labels = NextLabel,

    {GeneratedModuleName,
     Exports,
     Attributes,
     Functions2,
     Labels}.

-spec pass2_process_functions(Functions, State) -> Functions when
      Functions :: #{mfa() => #function{}},
      State :: #state{}.

pass2_process_functions(Functions, State) ->
    maps:map(
      fun(MFA, Function) ->
              pass2_process_function(MFA, Function, State)
      end, Functions).

-spec pass2_process_function(MFA, Function, State) -> Function when
      MFA :: mfa(),
      Function :: #function{},
      State :: #state{}.

pass2_process_function(
  {Module, Name, Arity},
  #function{name = Name,
            code = Instructions} = Function,
  State) ->
    Name1 = gen_function_name(Module, Name, Arity, State),
    Instructions1 = lists:map(
                      fun(Instruction) ->
                              S1 = State#state{mfa_in_progress = {Module,
                                                                  Name,
                                                                  Arity},
                                               function_in_progress = Name1},
                              pass2_process_instruction(Instruction, S1)
                      end, Instructions),
    Function#function{name = Name1,
                      code = Instructions1}.

-spec pass2_process_instruction(Instruction, State) -> Instruction when
      Instruction :: beam_instr(),
      State :: #state{}.

pass2_process_instruction(
  {Call, Arity, {_, _, _} = MFA} = Instruction,
  #state{functions = Functions})
  when Call =:= call orelse Call =:= call_only ->
    case Functions of
        #{MFA := #function{entry = EntryLabel}} ->
            {Call, Arity, {f, EntryLabel}};
        _ ->
            Instruction
    end;
pass2_process_instruction(
  {Call, Arity, {extfunc, Module, Name, Arity}} = Instruction,
  #state{functions = Functions})
  when Call =:= call_ext orelse Call =:= call_ext_only ->
    MFA = {Module, Name, Arity},
    case Functions of
        #{MFA := #function{entry = EntryLabel}} ->
            Call1 = case Call of
                        call_ext      -> call;
                        call_ext_only -> call_only
                    end,
            {Call1, Arity, {f, EntryLabel}};
        _ ->
            Instruction
    end;
pass2_process_instruction(
  {call_last, Arity, {Module, Name, Arity}, Opaque} = Instruction,
  #state{functions = Functions}) ->
    MFA = {Module, Name, Arity},
    case Functions of
        #{MFA := #function{entry = EntryLabel}} ->
            {call_last, Arity, {f, EntryLabel}, Opaque};
        _ ->
            Instruction
    end;
pass2_process_instruction(
  {call_ext_last, Arity, {extfunc, Module, Name, Arity}, Opaque} = Instruction,
  #state{functions = Functions}) ->
    MFA = {Module, Name, Arity},
    case Functions of
        #{MFA := #function{entry = EntryLabel}} ->
            {call_last, Arity, {f, EntryLabel}, Opaque};
        _ ->
            Instruction
    end;
pass2_process_instruction(
  {func_info, _ModRepr, _NameRepr, Arity},
  #state{generated_module_name = GeneratedModuleName,
         function_in_progress = Name}) ->
    ModRepr = {atom, GeneratedModuleName},
    NameRepr = {atom, Name},
    {func_info, ModRepr, NameRepr, Arity};
pass2_process_instruction(
  {get_map_elements, {f, OldLabel}, _, _} = Instruction,
  #state{mfa_in_progress = {Module, _, _},
         label_map = LabelMap}) ->
    NewLabel = maps:get({Module, OldLabel}, LabelMap),
    setelement(2, Instruction, {f, NewLabel});
pass2_process_instruction(
  {select_val, _, {f, OldEndLabel}, {list, Cases}} = Instruction,
  #state{mfa_in_progress = {Module, _, _},
         label_map = LabelMap}) ->
    Cases1 = [case Case of
                  {f, OldLabel} ->
                      NewLabel = maps:get({Module, OldLabel}, LabelMap),
                      {f, NewLabel};
                  _ ->
                      Case
              end || Case <- Cases],
    NewEndLabel = maps:get({Module, OldEndLabel}, LabelMap),
    Instruction1 = setelement(3, Instruction, {f, NewEndLabel}),
    setelement(4, Instruction1, {list, Cases1});
pass2_process_instruction(
  {test, _, {f, OldLabel}, _} = Instruction,
  #state{mfa_in_progress = {Module, _, _},
         label_map = LabelMap}) ->
    NewLabel = maps:get({Module, OldLabel}, LabelMap),
    setelement(3, Instruction, {f, NewLabel});
pass2_process_instruction(
  {make_fun2, {_, _, _} = MFA, _, _, _} = Instruction,
  #state{functions = Functions}) ->
    case Functions of
        #{MFA := #function{entry = EntryLabel}} ->
            setelement(2, Instruction, {f, EntryLabel});
        _ ->
            Instruction
    end;
pass2_process_instruction(
  {make_fun3, {_, _, _} = MFA, _, _, _, _} = Instruction,
  #state{functions = Functions}) ->
    case Functions of
        #{MFA := #function{entry = EntryLabel}} ->
            setelement(2, Instruction, {f, EntryLabel});
        _ ->
            Instruction
    end;
pass2_process_instruction(
  Instruction,
  _State) ->
    Instruction.

-spec gen_module_name(Functions, State) -> Module when
      Functions :: #{mfa() => #function{}},
      State :: #state{},
      Module :: module().

gen_module_name(Functions, #state{fun_info = Info}) ->
    Module = proplists:get_value(module, Info),
    Name = proplists:get_value(name, Info),
    Checksum = erlang:phash2(Functions),
    InternalName = lists:flatten(
                     io_lib:format(
                       "ktx__~s__~s__~b", [Module, Name, Checksum])),
    list_to_atom(InternalName).

-spec gen_function_name(Module, Name, Arity, State) -> Name when
      Module :: module(),
      Name :: atom(),
      Arity :: arity(),
      State :: #state{}.

gen_function_name(
  Module, Name, Arity,
  #state{entrypoint = {Module, Name, Arity}}) ->
    run;
gen_function_name(
  Module, Name, _Arity,
  _State) ->
    InternalName = lists:flatten(
                     io_lib:format(
                       "~s__~s", [Module, Name])),
    list_to_atom(InternalName).

%% -------------------------------------------------------------------
%% Environment handling.
%% -------------------------------------------------------------------

-spec to_standalone_env(State) -> StandaloneEnv when
      State :: #state{},
      StandaloneEnv :: list().

to_standalone_env(#state{fun_info = Info} = State) ->
    Env = proplists:get_value(env, Info),
    to_standalone_arg(Env, State).

to_standalone_arg(List, State) when is_list(List) ->
    lists:map(
      fun(Item) ->
              to_standalone_arg(Item, State)
      end, List);
to_standalone_arg(Tuple, State) when is_tuple(Tuple) ->
    List0 = tuple_to_list(Tuple),
    List1 = to_standalone_arg(List0, State),
    list_to_tuple(List1);
to_standalone_arg(Map, State) when is_map(Map) ->
    maps:fold(
      fun(Key, Value, Acc) ->
              Key1 = to_standalone_arg(Key, State),
              Value1 = to_standalone_arg(Value, State),
              Acc#{Key1 => Value1}
      end, #{}, Map);
to_standalone_arg(Fun, #state{options = Options})
  when is_function(Fun) ->
    to_standalone_fun(Fun, Options);
to_standalone_arg(Term, _State) ->
    Term.

to_actual_arg(#standalone_fun{arity = Arity} = StandaloneFun) ->
    case Arity of
        0 ->
            fun() -> exec(StandaloneFun, []) end;
        1 ->
            fun(Arg1) -> exec(StandaloneFun, [Arg1]) end;
        2 ->
            fun(Arg1, Arg2) -> exec(StandaloneFun, [Arg1, Arg2]) end;
        3 ->
            fun(Arg1, Arg2, Arg3) ->
                    exec(StandaloneFun, [Arg1, Arg2, Arg3])
            end;
        4 ->
            fun(Arg1, Arg2, Arg3, Arg4) ->
                    exec(StandaloneFun, [Arg1, Arg2, Arg3, Arg4])
            end;
        5 ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5) ->
                    exec(StandaloneFun, [Arg1, Arg2, Arg3, Arg4, Arg5])
            end;
        6 ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6) ->
                    exec(StandaloneFun, [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6])
            end;
        7 ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7) ->
                    exec(
                      StandaloneFun,
                      [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7])
            end;
        8 ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8) ->
                    exec(
                      StandaloneFun,
                      [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8])
            end;
        9 ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9) ->
                    exec(
                      StandaloneFun,
                      [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9])
            end;
        10 ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9, Arg10) ->
                    exec(
                      StandaloneFun,
                      [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                       Arg10])
            end
    end;
to_actual_arg(List) when is_list(List) ->
    lists:map(
      fun(Item) ->
              to_actual_arg(Item)
      end, List);
to_actual_arg(Tuple) when is_tuple(Tuple) ->
    List0 = tuple_to_list(Tuple),
    List1 = to_actual_arg(List0),
    list_to_tuple(List1);
to_actual_arg(Map) when is_map(Map) ->
    maps:fold(
      fun(Key, Value, Acc) ->
              Key1 = to_actual_arg(Key),
              Value1 = to_actual_arg(Value),
              Acc#{Key1 => Value1}
      end, #{}, Map);
to_actual_arg(Term) ->
    Term.
