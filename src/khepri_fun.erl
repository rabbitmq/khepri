%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Anonymous function extraction API.
%%
%% This module is responsible for extracting the code of an anonymous function.
%% The goal is to be able to store the extracted function and execute it later,
%% regardless of the availability of the initial Erlang module which declared
%% it.
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
%%
%% There is a special treatment for anonymous functions evaluated by
%% `erl_eval' (e.g. in the Erlang shell). "erl_eval functions" are lambdas
%% parsed from text and are evaluated using `erl_eval'.
%%
%% This kind of lambdas becomes a local function in the `erl_eval' module.
%%
%% Their assembly code isn't available in the `erl_eval' module. However, the
%% abstract code (i.e. after parsing but before compilation) is available in
%% the `env'. We compile that abstract code and extract the assembly from that
%% compiled beam.

-module(khepri_fun).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("src/internal.hrl").

-export([to_standalone_fun/2,
         exec/2]).

%% FIXME: compile:forms/2 is incorrectly specified and doesn't accept
%% assembly. This breaks compile/1 and causes a cascade of errors.
%%
%% The following basically disable Dialyzer for this module unfortunately...
-dialyzer({nowarn_function, [compile/1,
                             to_standalone_fun/2,
                             to_standalone_fun1/2,
                             to_standalone_fun2/2,
                             to_standalone_env/1,
                             to_standalone_arg/2]}).

-type fun_info() :: #{arity => arity(),
                      env => any(),
                      index => any(),
                      name => atom(),
                      module => module(),
                      new_index => any(),
                      new_uniq => any(),
                      pid => any(),
                      type => local | external,
                      uniq => any()}.
-type beam_instr() :: atom() | tuple().
-type label() :: pos_integer().

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

-type ensure_instruction_is_permitted_fun() ::
fun((beam_instr()) -> ok).

-type should_process_function_fun() ::
fun((module(), atom(), arity(), module()) -> boolean()).

-type is_standalone_fun_still_needed_fun() ::
fun((#{calls := #{mfa() => true},
       errors := [any()]}) -> boolean()).

-type standalone_fun() :: #standalone_fun{} | fun().
-type options() :: #{ensure_instruction_is_permitted =>
                     ensure_instruction_is_permitted_fun(),
                     should_process_function =>
                     should_process_function_fun(),
                     is_standalone_fun_still_needed =>
                     is_standalone_fun_still_needed_fun()}.

-export_type([standalone_fun/0,
              options/0]).

-record(state, {generated_module_name :: module() | undefined,
                entrypoint :: mfa() | undefined,
                checksums = #{} :: #{module() => binary()},
                fun_info :: fun_info(),
                calls = #{} :: #{mfa() => true},
                all_calls = #{} :: #{mfa() => true},
                functions = #{} :: #{mfa() => #function{}},

                mfa_in_progress :: mfa() | undefined,
                function_in_progress :: atom() | undefined,
                next_label = 1 :: label(),
                label_map = #{} :: #{{module(), label()} => label()},

                errors = [] :: [any()],
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
    {StandaloneFun, _State} = to_standalone_fun1(Fun, Options),
    StandaloneFun.

-spec to_standalone_fun1(Fun, Options) -> {StandaloneFun, State} when
      Fun :: fun(),
      Options :: options(),
      StandaloneFun :: standalone_fun(),
      State :: #state{}.

to_standalone_fun1(Fun, Options) ->
    Info = maps:from_list(erlang:fun_info(Fun)),
    #{module := Module,
      name := Name,
      arity := Arity} = Info,
    State0 = #state{fun_info = Info,
                    all_calls = #{{Module, Name, Arity} => true},
                    options = Options},
    to_standalone_fun2(Fun, State0).

-spec to_standalone_fun2(Fun, State) -> {StandaloneFun, State} when
      Fun :: fun(),
      State :: #state{},
      StandaloneFun :: standalone_fun().

to_standalone_fun2(
  Fun,
  #state{fun_info = #{module := Module,
                      name := Name,
                      arity := Arity,
                      type := Type}} = State) ->
    %% Don't extract functions like "fun dict:new/0" which are not meant to be
    %% copied.
    {ShouldProcess,
     State1} = case Type of
                   local ->
                       should_process_function(
                         Module, Name, Arity, Module, State);
                   external ->
                       should_process_function(
                         Module, Name, Arity, undefined, State)
               end,
    case ShouldProcess of
        true ->
            State2 = pass1(State1),

            %% Fun environment to standalone term.
            %%
            %% For "regular" lambdas, variables declared outside of the
            %% function body are put in this `env'. We need to process them in
            %% case they reference other lambdas for instance. We keep the end
            %% result to store it alongside the generated module, but not
            %% inside the module to avoid an increase in the number of
            %% identical modules with different environment.
            %%
            %% However for `erl_eval' functions created from lambdas, the env
            %% contains the parsed source code of the function. We don't need
            %% to interpret it.
            %%
            %% TODO: `to_standalone_env()' uses `to_standalone_fun1()' to
            %% extract and compile lambdas passed as arguments. It means they
            %% are fully compiled even though
            %% `is_standalone_fun_still_needed()' returns false later. This is
            %% a waste of resources and this function can probably be split
            %% into two parts to allow the environment to be extracted before
            %% and compiled after, once we are sure we need to create the
            %% final standalone fun.
            {Env, State3} = case Module =:= erl_eval andalso Type =:= local of
                                false -> to_standalone_env(State2);
                                true  -> {[], State2}
                            end,

            %% We offer one last chance to the caller to determine if a
            %% standalone function is still useful for him.
            %%
            %% This callback is only used for the top-level lambda. In other
            %% words, if the `env' contains other lambdas (i.e. anonymous
            %% functions passed as argument to the top-level one), the
            %% callback is not used. However, calls and errors from those
            %% inner lambdas are accumulated and can be used by the callback.
            case is_standalone_fun_still_needed(State3) of
                true ->
                    process_errors(State3),

                    Asm = pass2(State3),
                    {GeneratedModuleName, Beam} = compile(Asm),

                    StandaloneFun = #standalone_fun{
                                       module = GeneratedModuleName,
                                       beam = Beam,
                                       arity = Arity,
                                       env = Env},
                    {StandaloneFun, State3};
                false ->
                    {Fun, State3}
            end;
        false ->
            process_errors(State1),
            {Fun, State1}
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
  #state{fun_info = #{module := erl_eval, type := local} = Info,
         checksums = Checksums} = State) ->
    #{module := Module,
      name := Name,
      arity := Arity} = Info,

    Checksum = maps:get(new_uniq, Info),
    ?assert(is_binary(Checksum)),
    Checksums1 = Checksums#{Module => Checksum},
    State1 = State#state{checksums = Checksums1,
                         entrypoint = {Module, Name, Arity}},

    pass1_process_function(Module, Name, Arity, State1);
pass1(
  #state{fun_info = Info,
         checksums = Checksums} = State) ->
    #{module := Module,
      name := Name,
      arity := Arity,
      env := Env} = Info,

    %% Internally, a lambda which takes arguments and values from its
    %% environment (i.e. variables declared in the function which defined that
    %% lambda).
    InternalArity = Arity + length(Env),

    State1 = case maps:get(type, Info) of
                 local ->
                     Checksum = maps:get(new_uniq, Info),
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
  #state{functions = Functions} = State)
  when is_map_key({Module, Name, Arity}, Functions) ->
    State;
pass1_process_function(Module, Name, Arity, State) ->
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
  #state{mfa_in_progress = {Module, _, _} = MFA,
         next_label = NextLabel,
         functions = Functions} = State) ->
    ?assertNot(maps:is_key(MFA, Functions)),

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
    State1 = ensure_instruction_is_permitted(Instruction, State),
    State2 = pass1_process_call(Module, Name, Arity, State1),
    pass1_process_instructions(Rest, State2, [Instruction | Result]);
pass1_process_instructions(
  [{Call, Arity, {extfunc, Module, Name, Arity}} = Instruction | Rest],
  State,
  Result)
  when Call =:= call_ext orelse Call =:= call_ext_only ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    State2 = pass1_process_call(Module, Name, Arity, State1),
    pass1_process_instructions(Rest, State2, [Instruction | Result]);
pass1_process_instructions(
  [{call_last, Arity, {Module, Name, Arity}, _} = Instruction
   | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    State2 = pass1_process_call(Module, Name, Arity, State1),
    pass1_process_instructions(Rest, State2, [Instruction | Result]);
pass1_process_instructions(
  [{call_ext_last, Arity, {extfunc, Module, Name, Arity}, _} = Instruction
   | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    State2 = pass1_process_call(Module, Name, Arity, State1),
    pass1_process_instructions(Rest, State2, [Instruction | Result]);
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
    State1 = ensure_instruction_is_permitted(Instruction, State),
    State2 = pass1_process_call(Module, Name, Arity, State1),
    pass1_process_instructions(Rest, State2, [Instruction | Result]);
pass1_process_instructions(
  [{make_fun3, {Module, Name, Arity}, _, _, _, _} = Instruction | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    State2 = pass1_process_call(Module, Name, Arity, State1),
    pass1_process_instructions(Rest, State2, [Instruction | Result]);
pass1_process_instructions(
  [Instruction | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    pass1_process_instructions(Rest, State1, [Instruction | Result]);
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
         calls = Calls,
         all_calls = AllCalls} = State) ->
    CallKey = {Module, Name, Arity},
    AllCalls1 = AllCalls#{CallKey => true},
    case should_process_function(Module, Name, Arity, FromModule, State) of
        {true, State1} ->
            case Functions of
                #{CallKey := _} ->
                    State1;
                _ ->
                    Calls1 = Calls#{CallKey => true},
                    State1#state{calls = Calls1,
                                 all_calls = AllCalls1}
            end;
        {false, State1} ->
            State1#state{all_calls = AllCalls1}
    end.

-spec lookup_function(Module, Name, Arity, State) -> {Function, State} when
      Module :: module(),
      Name :: atom(),
      Arity :: non_neg_integer() | undefined,
      State :: #state{},
      Function :: #function{}.

lookup_function(
  erl_eval = Module, Name, _Arity,
  #state{fun_info = #{module := Module,
                      name := Name,
                      arity := Arity,
                      env := Env}} = State) ->
    %% There is a special case for `erl_eval' local functions: they are
    %% lambdas dynamically parsed, compiled and loaded by `erl_eval' and
    %% appear as local functions inside `erl_eval' directly.
    %%
    %% However `erl_eval' module doesn't contain the assembly for those
    %% functions. Instead, the abstract form of the source code is available
    %% in the lambda's env.
    %%
    %% There here, we compile the abstract form and extract the assembly from
    %% the compiled beam. This allows to use the rest of `khepri_fun'
    %% unmodified.
    #beam_file{code = Code} = erl_eval_fun_to_asm(Module, Name, Arity, Env),
    {lookup_function1(Code, Name, Arity), State};
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

-spec erl_eval_fun_to_asm(Module, Name, Arity, Env) -> BeamFileRecord when
      Module :: module(),
      Name :: atom(),
      Arity :: arity(),
      Env :: any(),
      BeamFileRecord :: #beam_file{}.
%% @private

erl_eval_fun_to_asm(Module, Name, Arity, [{Bindings, _, _, Clauses}])
  when Bindings =:= [] orelse %% Erlang is using a list for bindings,
       Bindings =:= #{} ->    %% but Elixir is using a map.
    %% We construct an abstract form based on the `env' of the lambda loaded
    %% by `erl_eval'.
    Anno = erl_anno:from_term(1),
    Forms = [{attribute, Anno, module, Module},
             {attribute, Anno, export, [{Name, Arity}]},
             {function, Anno, Name, Arity, Clauses}],

    %% The abstract form is now compiled to binary code. Then, the assembly
    %% code is extracted from the compiled beam.
    CompilerOptions = [from_abstr,
                       binary,
                       return_errors,
                       return_warnings,
                       deterministic],
    case compile:forms(Forms, CompilerOptions) of
        {ok, Module, Beam, _Warnings} ->
            %% We can ignore warnings because the lambda was already parsed
            %% and compiled before by `erl_eval' previously.
            do_disassemble(Beam);
        Error ->
            throw({erl_eval_fun_compilation_failure, Error})
    end.

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
    BeamFileRecord = do_disassemble(Beam),
    persistent_term:put(Key, BeamFileRecord),
    BeamFileRecord.

do_disassemble(Beam) ->
    beam_disasm:file(Beam).

-spec ensure_instruction_is_permitted(Instruction, State) ->
    State when
      Instruction :: beam_instr(),
      State :: #state{}.

ensure_instruction_is_permitted(
  Instruction,
  #state{options = #{ensure_instruction_is_permitted := Callback},
         errors = Errors} = State)
  when is_function(Callback) ->
    try
        Callback(Instruction),
        State
    catch
        throw:Error ->
            Errors1 = Errors ++ [Error],
            State#state{errors = Errors1}
    end;
ensure_instruction_is_permitted(_Instruction, State) ->
    State.

-spec should_process_function(Module, Name, Arity, FromModule, State) ->
    {ShouldProcess, State} when
      Module :: module(),
      Name :: atom(),
      Arity :: arity(),
      FromModule :: module(),
      State :: #state{},
      ShouldProcess :: boolean().

should_process_function(
  erl_eval, Name, Arity, _FromModule,
  #state{fun_info = #{module := erl_eval,
                      name := Name,
                      arity := Arity,
                      type := local}} = State) ->
    %% We want to process lambas loaded by `erl_eval'
    %% even though we wouldn't do that with the
    %% regular `erl_eval' API.
    {true, State};
should_process_function(
  Module, Name, Arity, FromModule,
  #state{options = #{should_process_function := Callback},
         errors = Errors} = State)
  when is_function(Callback) ->
    try
        ShouldProcess = Callback(Module, Name, Arity, FromModule),
        {ShouldProcess, State}
    catch
        throw:Error ->
            Errors1 = Errors ++ [Error],
            State1 = State#state{errors = Errors1},
            {false, State1}
    end;
should_process_function(Module, Name, Arity, _FromModule, State) ->
    {default_should_process_function(Module, Name, Arity),
     State}.

default_should_process_function(erlang, _Name, _Arity)  -> false;
default_should_process_function(_Module, _Name, _Arity) -> true.

-spec is_standalone_fun_still_needed(State) -> IsNeeded when
      State :: #state{},
      IsNeeded :: boolean().

is_standalone_fun_still_needed(
  #state{options = #{is_standalone_fun_still_needed := Callback},
         all_calls = Calls,
         errors = Errors})
  when is_function(Callback) ->
    Callback(#{calls => Calls,
               errors => Errors});
is_standalone_fun_still_needed(_State) ->
    true.

-spec process_errors(State) -> ok | no_return() when
      State :: #state{}.

%% TODO: Return all errors?
process_errors(#state{errors = []})          -> ok;
process_errors(#state{errors = [Error | _]}) -> throw(Error).

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
  {loop_rec, {f, OldLabel}, _} = Instruction,
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
  {test, _, {f, OldLabel}, _, _} = Instruction,
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
  {wait_timeout, {f, OldLabel}, _} = Instruction,
  #state{mfa_in_progress = {Module, _, _},
         label_map = LabelMap}) ->
    NewLabel = maps:get({Module, OldLabel}, LabelMap),
    setelement(2, Instruction, {f, NewLabel});
pass2_process_instruction(
  Instruction,
  _State) ->
    Instruction.

-spec gen_module_name(Functions, State) -> Module when
      Functions :: #{mfa() => #function{}},
      State :: #state{},
      Module :: module().

gen_module_name(Functions, #state{fun_info = Info}) ->
    #{module := Module,
      name := Name} = Info,
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

-spec to_standalone_env(State) -> {StandaloneEnv, State} when
      State :: #state{},
      StandaloneEnv :: list().

to_standalone_env(#state{fun_info = #{env := Env},
                         options = Options} = State) ->
    State1 = State#state{options = maps:remove(
                                     is_standalone_fun_still_needed,
                                     Options)},
    {Env1, State2} = to_standalone_arg(Env, State1),
    State3 = State2#state{options = Options},
    {Env1, State3}.

to_standalone_arg(List, State) when is_list(List) ->
    lists:foldr(
      fun(Item, {L, St}) ->
              {Item1, St1} = to_standalone_arg(Item, St),
              {[Item1 | L], St1}
      end, {[], State}, List);
to_standalone_arg(Tuple, State) when is_tuple(Tuple) ->
    List0 = tuple_to_list(Tuple),
    {List1, State1} = to_standalone_arg(List0, State),
    Tuple1 = list_to_tuple(List1),
    {Tuple1, State1};
to_standalone_arg(Map, State) when is_map(Map) ->
    maps:fold(
      fun(Key, Value, {M, St}) ->
              {Key1, St1} = to_standalone_arg(Key, St),
              {Value1, St2} = to_standalone_arg(Value, St1),
              M1 = M#{Key1 => Value1},
              {M1, St2}
      end, {#{}, State}, Map);
to_standalone_arg(Fun, #state{options = Options,
                              all_calls = AllCalls,
                              errors = Errors} = State)
  when is_function(Fun) ->
    {StandaloneFun, InnerState} = to_standalone_fun1(Fun, Options),
    #state{all_calls = InnerAllCalls,
           errors = InnerErrors} = InnerState,
    AllCalls1 = maps:merge(AllCalls, InnerAllCalls),
    Errors1 = Errors ++ InnerErrors,
    State1 = State#state{all_calls = AllCalls1,
                         errors = Errors1},
    {StandaloneFun, State1};
to_standalone_arg(Term, State) ->
    {Term, State}.

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
