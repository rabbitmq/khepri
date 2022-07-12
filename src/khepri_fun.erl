%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Anonymous function extraction private API.
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
%%
%% This module is private. The documentation is still visible because it may
%% help understand some implementation details. However, this module should
%% never be called directly outside of Khepri.

-module(khepri_fun).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("src/khepri_fun.hrl").

-export([to_standalone_fun/1,
         to_standalone_fun/2,
         exec/2]).

-ifdef(TEST).
-export([standalone_fun_cache_key/5,
         override_object_code/2,
         get_object_code/1,
         decode_line_chunk/2,
         compile/1]).
-endif.

%% FIXME: compile:forms/2 is incorrectly specified and doesn't accept
%% assembly. This breaks compile/1 and causes a cascade of errors.
%%
%% The following basically disables Dialyzer for this module unfortunately...
%% This can be removed once we start using Erlang 25 to run Dialyzer.
-dialyzer({nowarn_function, [compile/1,
                             to_standalone_fun/1,
                             to_standalone_fun/2,
                             to_standalone_fun1/2,
                             to_standalone_fun2/2,
                             to_standalone_fun3/2,
                             to_standalone_env/1,
                             to_standalone_arg/2,
                             standalone_fun_cache_key/1,
                             cache_standalone_fun/2,
                             handle_compilation_error/2,
                             handle_validation_error/3,
                             add_comments_and_retry/5,
                             add_comments_to_function/5,
                             add_comments_to_code/3,
                             add_comments_to_code/4,
                             find_comments_in_branch/2,
                             find_comments_in_branch/4,
                             split_comments/1,
                             split_comments/2,
                             merge_comments/2]}).

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

%% The following records are used to store the decoded "Line" beam chunk. They
%% are used while processing `line' instructions to restore the correct
%% location. This is needed so that exception stacktraces point to real
%% locations in source files.

-record(lines, {item_count,
                items = [],
                name_count,
                names = [],
                location_size}).

-record(line, {name_index,
               location}).

%% The following record is also linked to the decoding of the "Line" beam
%% chunk.

-record(tag, {tag,
              size,
              word_value,
              ptr_value}).

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

-record(beam_file_ext, {module               :: module(),
                        labeled_exports = [] :: [beam_lib:labeled_entry()],
                        attributes      = [] :: [beam_lib:attrib_entry()],
                        compile_info    = [] :: [beam_lib:compinfo_entry()],
                        code            = [] :: [#function{}],
                        %% Added in this module to stored the decoded "Line"
                        %% chunk.
                        lines                :: #lines{} | undefined,
                        strings              :: binary() | undefined}).

-type ensure_instruction_is_permitted_fun() ::
fun((Instruction :: beam_instr()) -> ok).
%% Function which evaluates the given instruction and returns `ok' if it is
%% permitted, throws an exception otherwise.
%%
%% Example:
%%
%% ```
%% Fun = fun
%%           ({jump, _})    -> ok;
%%           ({move, _, _}) -> ok;
%%           ({trim, _, _}) -> ok;
%%           (Unknown)      -> throw({unknown_instruction, Unknown})
%%       end.
%% '''

-type should_process_function_fun() ::
fun((Module :: module(),
     Function :: atom(),
     Arity :: arity(),
     FromModule :: module()) -> ShouldProcess :: boolean()).
%% Function which returns true if a called function should be extracted and
%% followed, false otherwise.
%%
%% `Module', `Function' and `Arity' qualify the function being called.
%%
%% `FromModule' indicates the module performing the call. This is useful to
%% distinguish local calls (`FromModule' == `Module') from remote calls.
%%
%% Example:
%%
%% ```
%% Fun = fun(Module, Name, Arity, FromModule) ->
%%               Module =:= FromModule orelse
%%               erlang:function_exported(Module, Name, Arity)
%%       end.
%% '''

-type is_standalone_fun_still_needed_fun() ::
fun((#{calls := #{Call :: mfa() => true},
       errors := [Error :: any()]}) -> IsNeeded :: boolean()).
%% Function which evaluates if the extracted function is still relevant in the
%% end. It returns true if it is, false otherwise.
%%
%% It takes a map with the following members:
%% <ul>
%% <li>`calls', a map of all the calls performed by the extracted code (only
%% the key is useful, the value is always true).</li>
%% <li>`errors', a list of errors collected during the extraction.</li>
%% </ul>

-type standalone_fun() :: #standalone_fun{} | fun().
%% The result of an extraction, as returned by {@link to_standalone_fun/2}.
%%
%% It can be stored, passed between processes and Erlang nodes. To execute the
%% extracted function, simply call {@link exec/2} which works like {@link
%% erlang:apply/2}.

-type options() :: #{ensure_instruction_is_permitted =>
                     ensure_instruction_is_permitted_fun(),
                     should_process_function =>
                     should_process_function_fun(),
                     is_standalone_fun_still_needed =>
                     is_standalone_fun_still_needed_fun()}.
%% Options to tune the extraction of an anonymous function.
%%
%% <ul>
%% <li>`ensure_instruction_is_permitted': a function which evaluates if an
%% instruction is permitted or not.</li>
%% <li>`should_process_function': a function which returns if a called module
%% and function should be extracted as well or left alone.</li>
%% <li>`is_standalone_fun_still_needed': a function which returns if, after
%% the extraction is finished, the extracted function is still needed in
%% comparison to keeping the initial anonymous function.</li>
%% </ul>

-export_type([standalone_fun/0,
              options/0]).

-record(state, {generated_module_name :: module() | undefined,
                entrypoint :: mfa() | undefined,
                checksums = #{} :: #{module() => binary()},
                fun_info :: fun_info(),
                calls = #{} :: #{mfa() => true},
                all_calls = #{} :: #{mfa() => true},
                functions = #{} :: #{mfa() => #function{}},

                lines_in_progress :: #lines{} | undefined,
                strings_in_progress :: binary() | undefined,
                mfa_in_progress :: mfa() | undefined,
                function_in_progress :: atom() | undefined,
                next_label = 1 :: label(),
                label_map = #{} :: #{{module(), label()} => label()},
                literal_funs = #{} :: #{fun() => standalone_fun()},

                errors = [] :: [any()],
                options = #{} :: options()}).

-type asm() :: {module(),
                [{atom(), arity()}],
                [],
                [#function{}],
                label()}.

-define(SF_ENTRYPOINT, run).

-spec to_standalone_fun(Fun) -> StandaloneFun when
      Fun :: fun(),
      StandaloneFun :: standalone_fun().
%% @doc Extracts the given anonymous function
%%
%% This is the same as:
%% ```
%% khepri_fun:to_standalone_fun(Fun, #{}).
%% '''
%%
%% @param Fun the anonymous function to extract
%%
%% @returns a standalone function record or the same anonymous function if no
%% extraction was needed.

to_standalone_fun(Fun) ->
    to_standalone_fun(Fun, #{}).

-spec to_standalone_fun(Fun, Options) -> StandaloneFun when
      Fun :: fun(),
      Options :: options(),
      StandaloneFun :: standalone_fun().
%% @doc Extracts the given anonymous function
%%
%% @param Fun the anonymous function to extract
%% @param Options a map of options
%%
%% @returns a standalone function record or the same anonymous function if no
%% extraction was needed.

to_standalone_fun(Fun, Options) ->
    {StandaloneFun, _State} = to_standalone_fun1(Fun, Options),
    StandaloneFun.

-spec to_standalone_fun1(Fun, Options) -> {StandaloneFun, State} when
      Fun :: fun(),
      Options :: options(),
      StandaloneFun :: standalone_fun(),
      State :: #state{}.
%% @private
%% @hidden

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
%% @private
%% @hidden

to_standalone_fun2(Fun, State) ->
    case get_cached_standalone_fun(State) of
        #standalone_fun{} = StandaloneFunWithoutEnv ->
            %% We need to set the environment for this specific call of the
            %% anonymous function in the returned `#standalone_fun{}'.
            {Env, State1} = to_standalone_env(State),
            StandaloneFun = StandaloneFunWithoutEnv#standalone_fun{env = Env},
            {StandaloneFun, State1};
        fun_kept ->
            {Fun, State};
        undefined ->
            to_standalone_fun3(Fun, State)
    end.

-spec to_standalone_fun3(Fun, State) -> {StandaloneFun, State} when
      Fun :: fun(),
      State :: #state{},
      StandaloneFun :: standalone_fun().
%% @private
%% @hidden

to_standalone_fun3(
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
                       _ = code:ensure_loaded(Module),
                       case erlang:function_exported(Module, Name, Arity) of
                           true ->
                               should_process_function(
                                 Module, Name, Arity, undefined, State);
                           false ->
                               throw({call_to_unexported_function,
                                      {Module, Name, Arity}})
                       end
               end,
    case ShouldProcess of
        true ->
            State2 = pass1(State1),

            {Env, State3} = to_standalone_env(State2),

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

                    #state{literal_funs = LiteralFuns0} = State3,
                    LiteralFuns = maps:values(LiteralFuns0),

                    Asm = pass2(State3),
                    {GeneratedModuleName, Beam} = compile(Asm),

                    StandaloneFun = #standalone_fun{
                                       module = GeneratedModuleName,
                                       beam = Beam,
                                       arity = Arity,
                                       literal_funs = LiteralFuns,
                                       env = Env},
                    cache_standalone_fun(State3, StandaloneFun),
                    {StandaloneFun, State3};
                false ->
                    cache_standalone_fun(State3, fun_kept),
                    {Fun, State3}
            end;
        false ->
            process_errors(State1),
            cache_standalone_fun(State1, fun_kept),
            {Fun, State1}
    end.

-spec to_embedded_standalone_fun(Fun, State) -> {StandaloneFun, State} when
      Fun :: fun(),
      State :: #state{},
      StandaloneFun :: standalone_fun().
%% @private
%% @hidden

to_embedded_standalone_fun(
  Fun,
  #state{options = Options,
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
    {StandaloneFun, State1}.

-spec standalone_fun_cache_key(State) -> Key when
      State :: #state{},
      Key :: {?MODULE,
              standalone_fun_cache_key,
              {module(), atom(), arity()},
              binary()}.
%% @doc Computes the standalone function cache key.
%%
%% To identify a standalone function in the cache, we base the key on:
%% <ul>
%% <li>the anonymous function's module, function name and arity</li>
%% <li>the checksum of the module holding that function</li>
%% </ul>
%%
%% @private

standalone_fun_cache_key(
  #state{fun_info = #{module := Module,
                      name := Name,
                      arity := Arity,
                      type := local,
                      new_uniq := Checksum},
         options = Options}) ->
    standalone_fun_cache_key(Module, Name, Arity, Checksum, Options);
standalone_fun_cache_key(
  #state{fun_info = #{module := Module,
                      name := Name,
                      arity := Arity,
                      type := external},
         options = Options}) ->
    Checksum = Module:module_info(md5),
    standalone_fun_cache_key(Module, Name, Arity, Checksum, Options).

standalone_fun_cache_key(Module, Name, Arity, Checksum, Options) ->
    %% We also include the options in the cache key because different options
    %% could affect the created standalone function.
    {?MODULE, standalone_fun_cache, {Module, Name, Arity}, Checksum, Options}.

-spec get_cached_standalone_fun(State) -> Ret when
      State :: #state{},
      Ret :: StandaloneFun | fun_kept | undefined,
      StandaloneFun :: standalone_fun().
%% @doc Returns the cached standalone function if found.
%%
%% @returns a `standalone_fun()' if a corresponding standalone function was
%% found in the cache, a `fun_kept' atom if the anonymous function didn't need
%% any processing and can be used directly, or `undefined' if there is no
%% corresponding entry in the cache.
%%
%% @private

get_cached_standalone_fun(
  #state{fun_info = #{module := Module}} = State)
  when Module =/= erl_eval ->
    Key = standalone_fun_cache_key(State),
    case persistent_term:get(Key, undefined) of
        #{standalone_fun := StandaloneFunWithoutEnv,
          checksums := Checksums,
          counters := Counters} ->
            %% We want to make sure that all the modules used by the anonymous
            %% function were not updated since it was stored in the cache.
            %% Therefore, they must have the same checksums has the ones
            %% stored in the cache. This list of modules also contain the
            %% modules holding the callbacks in specified in `Options'.
            %%
            %% The checksum of the module holding the anonymous function is
            %% already in the cache key however. Likewise for the actual
            %% options.
            SameModules = maps:fold(
                            fun
                                (Mod, Checksum, true) ->
                                    Checksum =:= Mod:module_info(md5);
                                (_Module, _Checksum, false) ->
                                    false
                            end, true, Checksums),

            if
                SameModules ->
                    counters:add(Counters, 1, 1),
                    StandaloneFunWithoutEnv;
                true ->
                    undefined
            end;
        #{fun_kept := true,
          counters := Counters} ->
            %% `fun_kept' means the anonymous function could be used directly;
            %% i.e. there was no need to create a standalone function.
            counters:add(Counters, 1, 1),
            fun_kept;
        undefined ->
            undefined
    end;
get_cached_standalone_fun(_State) ->
    %% We don't cache `erl_eval'-based anonymous functions currently.
    %%
    %% TODO: Can we cache them?
    undefined.

-spec cache_standalone_fun(StandaloneFun, State) -> ok when
      StandaloneFun :: standalone_fun() | fun_kept,
      State :: #state{}.
%% @private

cache_standalone_fun(
  #state{checksums = Checksums, options = Options} = State,
  StandaloneFun) ->
    %% We include the options in the cached value. This is useful when the
    %% callbacks change for the same anonymous function.
    Checksums1 = maps:fold(
                   fun
                       (_Key, Fun, Acc) when is_function(Fun) ->
                           Info = maps:from_list(erlang:fun_info(Fun)),
                           #{module := Module,
                             new_uniq := Checksum} = Info,
                           case Acc of
                               #{Module := KnownChecksum} ->
                                   ?assertEqual(KnownChecksum, Checksum),
                                   Acc;
                               _ ->
                                   Acc#{Module => Checksum}
                           end;
                       (_Key, _Value, Acc) ->
                           Acc
                   end, Checksums, Options),

    Key = standalone_fun_cache_key(State),

    %% Counters track the cache hits. They are only used by the testsuite
    %% currently.
    Counters = counters:new(1, [write_concurrency]),

    case StandaloneFun of
        #standalone_fun{} ->
            %% The standalone function is stored in the cache without its
            %% environment (the variable bindings in the anonymous function).
            %% They are given for a specific call of this function and may
            %% change for another call , even though the code is the same
            %% otherwise.
            %%
            %% The environment is set by the caller of
            %% `get_cached_standalone_fun()'.
            StandaloneFunWithoutEnv = StandaloneFun#standalone_fun{env = []},

            Value = #{standalone_fun => StandaloneFunWithoutEnv,
                      checksums => Checksums1,
                      options => Options,
                      counters => Counters},

            %% TODO: We need to add some memory management here to clear the
            %% cache if there are many different standalone functions.
            persistent_term:put(Key, Value);
        fun_kept ->
            Value = #{fun_kept => true,
                      counters => Counters},
            persistent_term:put(Key, Value)
    end,
    ok.

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
        Error                  -> handle_compilation_error(Asm, Error)
    end.

handle_compilation_error(
  Asm,
  {error,
   [{_GeneratedModuleName,
     [{_, beam_validator, ValidationFailure} | _Rest]}],
   []} = Error) ->
    handle_validation_error(Asm, ValidationFailure, Error);
handle_compilation_error(
  Asm,
  %% Same as above, but returned by Erlang 23's compiler instead of Erlang 24+.
  {error,
   [{_GeneratedModuleName,
     [{beam_validator, ValidationFailure} | _Rest]}],
   []} = Error) ->
    handle_validation_error(Asm, ValidationFailure, Error);
handle_compilation_error(Asm, Error) ->
    throw({compilation_failure, Error, Asm}).

handle_validation_error(
  Asm,
  {{_, Name, Arity},
   {{Call, _Arity, {f, EntryLabel}},
    CallIndex,
    no_bs_start_match2}},
  Error) when Call =:= call orelse Call =:= call_only ->
    {_GeneratedModuleName,
     _Exports,
     _Attributes,
     Functions,
     _Labels} = Asm,
    #function{code = Instructions} = find_function(Functions, Name, Arity),
    Comments = find_comments_in_branch(Instructions, CallIndex),
    Location = {'after', {label, EntryLabel}},
    add_comments_and_retry(Asm, Error, EntryLabel, Location, Comments);
handle_validation_error(
  Asm,
  {FailingFun,
   {{bs_start_match4, _Fail, _, Var, Var} = FailingInstruction,
    _,
    {bad_type, {needed, NeededType}, {actual, any}}}},
  Error) ->
    VarInfo = {var_info, Var, [{type, NeededType}]},
    Comments = [{'%', VarInfo}],
    Location = {before, FailingInstruction},
    add_comments_and_retry(Asm, Error, FailingFun, Location, Comments);
handle_validation_error(Asm, _ValidationFailure, Error) ->
    throw({compilation_failure, Error, Asm}).

%% Looks up the comments for all variables within a branch up as they
%% appear at the given instruction index.
%%
%% For example, consider the instructions for this function:
%%
%%     ...
%%     {label, 4},
%%     {'%', {var_info, {x, 0}, [accepts_match_context]}},
%%     {bs_start_match4, {atom, no_fail}, 1, {x, 0}, {x, 0}},
%%     {test, bs_match_string, {f, 5}, [{x, 0}, 8, {string, <<".">>}]},
%%     {move, {x, 0}, {x, 1}},
%%     {move, nil, {x, 0}},
%%     {call_only, 2, {f, 7},
%%     {label, 5},
%%     ...
%%
%% When given these instructions and the index for the `call_only'
%% instruction, this function should return:
%%
%%     [{'%', {var_info, {x, 1}, [accepts_match_context]}}]
%%
%% Notice that this comment applies to `{x, 1}' instead of `{x, 0}' as it
%% appears in the original comment, since this is the typing of the variables
%% at the time of the `call_only' instruction.
%%
%% To construct these comments, we fold through the instructions in order
%% and track the comments. There are some special-case instructions to
%% consider - `label/1' and `move/2'.
%%
%% A `label/1' instruction begins a branch within the instructions. Each
%% branch has independent typing, so any comments that appear between two
%% labels only apply to the variables within that branch. So in this function,
%% `label/1' clears any types we've gathered. In the above example
%% instructions, we only care about the typing between `{label, 4}' and
%% `{label, 5}'.
%%
%% `move/2' moves the some value from a source register or literal `Src' to
%% a destination register `Dst'. When handling a `move/2', if any comments
%% exist for `Src', we move the comments for `Src' to `Dst'. If any comments
%% exist for `Dst`, they are discarded. In the example instructions,
%% `{move, {x, 0}, {x, 1}}' moves the comment for `{x, 0}' to `{x, 1}'.
find_comments_in_branch(Instructions, Index) ->
    %% `beam_validator' instruction counter is 1-indexed
    find_comments_in_branch(Instructions, Index, 1, #{}).

find_comments_in_branch(
  _Instructions, Index, Index, VarInfos) ->
    [{'%', {var_info, Var, Info}} || {Var, Info} <- maps:to_list(VarInfos)];
find_comments_in_branch(
  [{'%', {var_info, Var, Info}} | Rest], Index, Counter, VarInfos) ->
    VarInfos1 = maps:put(Var, Info, VarInfos),
    find_comments_in_branch(Rest, Index, Counter + 1, VarInfos1);
find_comments_in_branch(
  [{move, Src, Dst} | Rest], Index, Counter, VarInfos) ->
    VarInfos1 = case VarInfos of
                    #{Src := Info} -> maps:put(Dst, Info, VarInfos);
                    _              -> maps:remove(Dst, VarInfos)
                end,
    VarInfos2 = maps:remove(Src, VarInfos1),
    find_comments_in_branch(Rest, Index, Counter + 1, VarInfos2);
find_comments_in_branch(
  [{label, _Label} | Rest], Index, Counter, _VarInfos) ->
    %% Each branch (separated by labels) has independent typing
    find_comments_in_branch(Rest, Index, Counter + 1, #{});
find_comments_in_branch(
  [_Instruction | Rest], Index, Counter, VarInfos) ->
    find_comments_in_branch(
      Rest, Index, Counter + 1, VarInfos).

add_comments_and_retry(
  Asm, Error, FailingFun, Location, Comments) ->
    {GeneratedModuleName,
     Exports,
     Attributes,
     Functions,
     Labels} = Asm,
    try
        Functions1 = add_comments_to_function(
                       Functions, FailingFun, Location, Comments, []),
        Asm1 = {GeneratedModuleName,
                Exports,
                Attributes,
                Functions1,
                Labels},
        compile(Asm1)
    catch
        throw:duplicate_annotations ->
            throw({compilation_failure, Error, Asm})
    end.

add_comments_to_function(
  [#function{name = Name, arity = Arity, code = Code} = Function | Rest],
  {_GeneratedModuleName, Name, Arity},
  Location, Comments, Result) ->
    Code1 = add_comments_to_code(Code, Location, Comments),
    Function1 = Function#function{code = Code1},
    lists:reverse(Result) ++ [Function1 | Rest];
add_comments_to_function(
  [#function{entry = EntryLabel, code = Code} = Function | Rest],
  EntryLabel, Location, Comments, Result) ->
    Code1 = add_comments_to_code(Code, Location, Comments),
    Function1 = Function#function{code = Code1},
    lists:reverse(Result) ++ [Function1 | Rest];
add_comments_to_function(
  [Function | Rest], FailingFun, Location, Comments, Result) ->
    add_comments_to_function(
      Rest, FailingFun, Location, Comments, [Function | Result]).

add_comments_to_code(Code, Location, Comments) ->
    add_comments_to_code(Code, Location, Comments, []).

add_comments_to_code(
  [Instruction | Rest], {before, Instruction}, Comments, Result) ->
    {ExistingComments, Result1} = split_comments(Result),
    Comments1 = merge_comments(Comments, ExistingComments),
    lists:reverse(Result1) ++ Comments1 ++ [Instruction | Rest];
add_comments_to_code(
  [Instruction | Rest], {'after', Instruction}, Comments, Result) ->
    {ExistingComments, Rest1} = split_comments(Rest),
    Comments1 = merge_comments(Comments, ExistingComments),
    lists:reverse(Result) ++ [Instruction | Comments1] ++ Rest1;
add_comments_to_code(
  [Instruction | Rest], Location, Comments, Result) ->
    add_comments_to_code(Rest, Location, Comments, [Instruction | Result]).

split_comments(Instructions) ->
    split_comments(Instructions, []).

split_comments([{'%', _} = Comment | Rest], Comments) ->
    split_comments(Rest, [Comment | Comments]);
split_comments(Rest, Comments) ->
    {lists:reverse(Comments), Rest}.

merge_comments(Comments, ExistingComments) ->
    ExistingCommentsMap = maps:from_list(
                            [{Var, Info} ||
                             {'%', {var_info, Var, Info}} <-
                             ExistingComments]),
    lists:map(fun({'%', {var_info, Var, Info}} = Annotation) ->
        case ExistingCommentsMap of
          #{Var := Info} ->
              throw(duplicate_annotations);
          #{Var := ExistingInfo} ->
              {'%', {var_info, Var, Info ++ ExistingInfo}};
          _ ->
              Annotation
        end
    end, Comments).

-spec exec(StandaloneFun, Args) -> Ret when
      StandaloneFun :: standalone_fun(),
      Args :: [any()],
      Ret :: any().
%% @doc Executes a previously extracted anonymous function.
%%
%% This is the equivalent of {@link erlang:apply/2} but it supports extracted
%% anonymous functions.
%%
%% The list of `Args' must match the arity of the anonymous function.
%%
%% @param StandaloneFun the extracted function as returned by {@link
%% to_standalone_fun/2}.
%% @param Args the list of arguments to pass to the extracted function.
%%
%% @returns the return value of the extracted function.

exec(
  #standalone_fun{module = Module,
                  arity = Arity,
                  literal_funs = LiteralFuns,
                  env = Env} = StandaloneFun,
  Args) when length(Args) =:= Arity ->
    load_standalone_fun(StandaloneFun),
    %% We also need to load any literal functions referenced by the standalone
    %% function and extracted with it. The assembly code already references
    %% them.
    lists:foreach(
      fun(LiteralFun) -> load_standalone_fun(LiteralFun) end,
      LiteralFuns),
    Env1 = to_actual_arg(Env),
    erlang:apply(Module, ?SF_ENTRYPOINT, Args ++ Env1);
exec(#standalone_fun{} = StandaloneFun, Args) ->
    exit({badarity, {StandaloneFun, Args}});
exec(Fun, Args) ->
    erlang:apply(Fun, Args).

-spec load_standalone_fun(StandaloneFun) -> ok when
      StandaloneFun :: standalone_fun().
%% @private

load_standalone_fun(#standalone_fun{module = Module, beam = Beam}) ->
    case code:is_loaded(Module) of
        false ->
            {module, _} = code:load_binary(Module, ?MODULE_STRING, Beam),
            ok;
        _ ->
            ok
    end;
load_standalone_fun(Fun) when is_function(Fun) ->
    ok.

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
    pass1_process_instructions(Instructions, State, []).

%% The first group of clauses of this function patch incorrectly decoded
%% instructions. These clauses recurse after fixing the instruction to enter
%% the other groups of clauses.
%%
%% The second group of clauses:
%%   1. ensures the instruction is known and allowed,
%%   2. records all calls that need their code to be copied and
%%   3. records jump labels.
%%
%% The third group of clauses infers type information and match contexts
%% and adds comments to satisfy the compiler's validator pass.

%% First group.

pass1_process_instructions(
  [{arithfbif, Operation, Fail, Args, Dst} | Rest],
  State,
  Result) ->
    %% `beam_disasm' did not decode this instruction correctly. `arithfbif'
    %% should be translated into a `bif'.
    Instruction = {bif, Operation, Fail, Args, Dst},
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{bs_append, _, _, _, _, _, _, {field_flags, FF}, _} = Instruction0 | Rest],
  State,
  Result)
  when is_integer(FF) ->
    %% `beam_disasm' did not decode this instruction's field flags.
    Instruction = decode_field_flags(Instruction0, 8),
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{bs_create_bin,
    [{{f, _} = Fail, {u, Heap}, {u, Live}, {u, Unit}, Dst, _, _N, List}]}
   | Rest],
  State,
  Result) when is_list(List) ->
    %% `beam_disasm' decoded the instruction's arguments as a tuple inside a
    %% list. They should be part of the instruction's tuple. Also, various
    %% arguments are not wrapped/unwrapped correctly.
    List1 = fix_create_bin_list(List, State),
    Instruction = {bs_create_bin, Fail, Heap, Live, Unit, Dst, {list, List1}},
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{bs_private_append, _, _, _, _, {field_flags, FF}, _} = Instruction0 | Rest],
  State,
  Result)
  when is_integer(FF) ->
    %% `beam_disasm' did not decode this instruction's field flags.
    Instruction = decode_field_flags(Instruction0, 6),
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{BsInit, _, _, _, _, {field_flags, FF}, _} = Instruction0 | Rest],
  State,
  Result)
  when (BsInit =:= bs_init2 orelse BsInit =:= bs_init_bits) andalso
       is_integer(FF) ->
    %% `beam_disasm' did not decode this instruction's field flags.
    Instruction = decode_field_flags(Instruction0, 6),
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{BsPutSomething, _, _, _, {field_flags, FF}, _} = Instruction0 | Rest],
  State,
  Result)
  when (BsPutSomething =:= bs_put_binary orelse
        BsPutSomething =:= bs_put_integer) andalso
       is_integer(FF) ->
    %% `beam_disasm' did not decode this instruction's field flags.
    Instruction = decode_field_flags(Instruction0, 5),
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{bs_start_match3, Fail, Bin, {u, Live}, Dst} | Rest],
  State,
  Result) ->
    %% `beam_disasm' did not decode this instruction correctly. We need to
    %% patch it to:
    %%   1. add `test' as the first element in the tuple,
    %%   2. swap `Bin' and `Live',
    %%   3. put `Bin' in a list and
    %%   4. store `Live' as an integer.
    Instruction = {test, bs_start_match3, Fail, Live, [Bin], Dst},
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{bs_start_match4, Fail, {u, Live}, Src, Dst} | Rest],
  State,
  Result) ->
    %% `beam_disasm' did not decode this instruction correctly. We need to
    %% patch it to store `Live' as an integer.
    Instruction = {bs_start_match4, Fail, Live, Src, Dst},
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{call_fun2,
    {atom, safe},
    Arity,
    {tr, FunReg, {{t_fun, _Arity, _Domain, _Range} = Type, _, _}}} | Rest],
  State,
  Result) ->
    %% `beam_disasm' did not decode this instruction correctly. The
    %% type in the type-tagged record is wrapped with extra information
    %% we discard.
    Instruction = {call_fun2, {atom, safe}, Arity, {tr, FunReg, Type}},
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{test, BsGetSomething,
    Fail, [Ctx, Live, Size, Unit, {field_flags, FF} = FieldFlags0, Dst]}
   | Rest],
  State,
  Result)
  when (BsGetSomething =:= bs_get_integer2 orelse
        BsGetSomething =:= bs_get_binary2) andalso
       is_integer(FF) ->
    %% `beam_disasm' did not decode this instruction correctly. We need to
    %% patch it to move `Live' before the list. We also need to decode field
    %% flags.
    FieldFlags = decode_field_flags(FieldFlags0),
    Instruction = {test, BsGetSomething,
                   Fail, Live, [Ctx, Size, Unit, FieldFlags], Dst},
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{BsGetSomething, Ctx, Dst, {u, Live}} | Rest],
  State,
  Result)
  when BsGetSomething =:= bs_get_position orelse
       BsGetSomething =:= bs_get_tail ->
    %% `beam_disasm' did not decode this instruction correctly. We need to
    %% patch it to store `Live' as an integer.
    Instruction = {BsGetSomething, Ctx, Dst, Live},
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{test, bs_match_string, Fail, [Ctx, Stride, String]} | Rest],
  State,
  Result) when is_binary(String) ->
    %% `beam_disasm' did not decode this instruction correctly. We need to
    %% patch it to put `String' inside a tuple.
    Instruction = {test, bs_match_string,
                   Fail, [Ctx, Stride, {string, String}]},
    pass1_process_instructions([Instruction | Rest], State, Result);
pass1_process_instructions(
  [{raise, Fail, Args, Dst} | Rest],
  State,
  Result) ->
    %% `beam_disasm` did not decode this instruction correctly. `raise'
    %% should be translated into a `bif'.
    Instruction = {bif, raise, Fail, Args, Dst},
    pass1_process_instructions([Instruction | Rest], State, Result);

%% Second group.

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
    %% `beam_disasm' seems to put `line' before `label', but the compiler is
    %% not pleased with that. Let's make sure the `label' appears first in the
    %% final assembly form.
    Result1 = case Result of
                  [{line, _} = Line | R] -> [Line, Instruction | R];
                  _                      -> [Instruction | Result]
              end,
    pass1_process_instructions(Rest, State1, Result1);
pass1_process_instructions(
  [{line, Index} | Rest],
  #state{lines_in_progress = Lines} = State,
  Result) ->
    case Lines of
        #lines{items = Items, names = Names} ->
            %% We could decode the "Line" beam chunk which contains the mapping
            %% between `Index' in the instruction decoded by `beam_disasm' and
            %% the actual location (filename + line number). Therefore we can
            %% generate the correct `line' instruction.
            #line{name_index = NameIndex,
                  location = Location} = lists:nth(Index + 1, Items),
            Name = lists:nth(NameIndex + 1, Names),
            Line = {line, [{location, Name, Location}]},
            pass1_process_instructions(Rest, State, [Line | Result]);
        undefined ->
            %% Drop this instruction as we don't have the "Line" beam chunk to
            %% decode it.
            pass1_process_instructions(Rest, State, Result)
    end;
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
  [{move, {literal, Fun}, Reg} = Instruction | Rest],
  State,
  Result) when is_function(Fun) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),

    %% This `move' instruction references a lambda: we must extract it like
    %% lambas present in the environment. We keep track of all extracted
    %% literal funs in the `literal_funs' field of the state record. This is
    %% used to create the final `#standalone_fun{}' at the end.
    %%
    %% Note that `literal_funs` can contain both `#standalone_fun{}' records
    %% and lambdas. The latter is possible if the function doesn't need
    %% extraction.
    #state{literal_funs = LiteralFuns} = State1,
    State3 = case LiteralFuns of
                 #{Fun := _} ->
                     State1;
                 _ ->
                     {StandaloneFun, State2} = to_embedded_standalone_fun(
                                                 Fun, State1),
                     LiteralFuns1 = LiteralFuns#{Fun => StandaloneFun},
                     State2#state{literal_funs = LiteralFuns1}
             end,

    %% We can now get the result of the extraction and recreate the `move'
    %% instruction.
    #state{literal_funs = #{Fun := StandaloneFun1}} = State3,
    Fun1 = case StandaloneFun1 of
               #standalone_fun{module = Module, arity = Arity} ->
                   %% The lambda was extracted. Here we simply construct the
                   %% reference to the entrypoint function in the generated
                   %% module. It doesn't matter that the module is not loaded.
                   fun Module:?SF_ENTRYPOINT/Arity;
               _ when is_function(StandaloneFun1) ->
                   %% The function didn't require any extraction.
                   ?assertEqual(Fun, StandaloneFun1),
                   Fun
           end,
    Instruction1 = {move, {literal, Fun1}, Reg},
    pass1_process_instructions(Rest, State3, [Instruction1 | Result]);

%% Third group.
pass1_process_instructions(
  [{get_tuple_element, Src, Element, _Dest} = Instruction | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    Src1 = fix_type_tagged_beam_register(Src),
    Instruction1 = setelement(2, Instruction, Src1),

    Reg = get_reg_from_type_tagged_beam_register(Src1),
    Type = {t_tuple, Element + 1, false, #{}},
    VarInfo = {var_info, Reg, [{type, Type}]},
    Comment = {'%', VarInfo},

    pass1_process_instructions(Rest, State1, [Instruction1, Comment | Result]);
pass1_process_instructions(
  [{select_tuple_arity, Src, _, _} = Instruction | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    Src1 = fix_type_tagged_beam_register(Src),
    Instruction1 = setelement(2, Instruction, Src1),

    Reg = get_reg_from_type_tagged_beam_register(Src1),
    Type = {t_tuple, 0, false, #{}},
    VarInfo = {var_info, Reg, [{type, Type}]},
    Comment = {'%', VarInfo},
    pass1_process_instructions(Rest, State1, [Instruction1, Comment | Result]);
pass1_process_instructions(
  [{get_map_elements, _Fail, Src, {list, List}} = Instruction | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    Src1 = fix_type_tagged_beam_register(Src),
    List1 = [fix_type_tagged_beam_register(I)
             || I <- List],
    Instruction1 = setelement(3, Instruction, Src1),
    Instruction2 = setelement(4, Instruction1, {list, List1}),

    Reg = get_reg_from_type_tagged_beam_register(Src1),
    Type = {t_map, any, any},
    VarInfo = {var_info, Reg, [{type, Type}]},
    Comment = {'%', VarInfo},
    pass1_process_instructions(Rest, State1, [Instruction2, Comment | Result]);
pass1_process_instructions(
  [{put_map_assoc, _Fail, Src, _Dst, _Live, {list, _}} = Instruction | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    Type = {t_map, any, any},
    VarInfo = {var_info, Src, [{type, Type}]},
    Comment = {'%', VarInfo},
    pass1_process_instructions(Rest, State1, [Instruction, Comment | Result]);
pass1_process_instructions(
  [{bs_start_match4, _Fail, _, Var, Var} = Instruction | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    VarInfo = {var_info, Var, [accepts_match_context]},
    Comment = {'%', VarInfo},
    pass1_process_instructions(Rest, State1, [Instruction, Comment | Result]);
pass1_process_instructions(
  [{test, bs_start_match3, _Fail, _, [Var], _Dst} = Instruction | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    VarInfo = {var_info, Var, [accepts_match_context]},
    Comment = {'%', VarInfo},
    pass1_process_instructions(Rest, State1, [Instruction, Comment | Result]);
pass1_process_instructions(
  [{test, test_arity, _Fail, [Var, Arity]} = Instruction | Rest],
  State,
  Result) ->
    State1 = ensure_instruction_is_permitted(Instruction, State),
    Type = {t_tuple, Arity, false, #{}},
    VarInfo = {var_info, Var, [{type, Type}]},
    Comment = {'%', VarInfo},
    pass1_process_instructions(Rest, State1, [Instruction, Comment | Result]);

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
%% Looks up the function from the given module with the given arity (if
%% defined), disassembling the module if necessary

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
    %%
    %% FIXME: Can we compile to assembly form using 'S' instead?
    #beam_file_ext{code = Functions} = erl_eval_fun_to_asm(
                                         Module, Name, Arity, Env),
    {find_function(Functions, Name, Arity), State};
lookup_function(Module, Name, Arity, State) ->
    {#beam_file_ext{code = Functions}, State1} = disassemble_module(Module, State),
    {find_function(Functions, Name, Arity), State1}.

-spec find_function([Function], Name, Arity) -> Function when
      Function :: #function{},
      Name :: atom(),
      Arity :: non_neg_integer() | undefined.
%% Finds a function in a list of functions by its name and arity

find_function(
  [#function{name = Name, arity = Arity} = Function | _],
  Name, Arity) when is_integer(Arity) ->
    Function;
find_function(
  [#function{name = Name} = Function | _],
  Name, undefined) ->
    Function;
find_function(
  [_ | Rest],
  Name, Arity) ->
    find_function(Rest, Name, Arity).

-spec erl_eval_fun_to_asm(Module, Name, Arity, Env) -> BeamFileRecord when
      Module :: module(),
      Name :: atom(),
      Arity :: arity(),
      Env :: any(),
      BeamFileRecord :: #beam_file_ext{}.
%% @private

erl_eval_fun_to_asm(Module, Name, Arity, [{_, Bindings, _, _, _, Clauses}])
  when Bindings =:= [] orelse %% Erlang is using a list for bindings,
       Bindings =:= #{} ->    %% but Elixir is using a map.
    %% Erlang starting from 25.
    erl_eval_fun_to_asm1(Module, Name, Arity, Clauses);
erl_eval_fun_to_asm(Module, Name, Arity, [{Bindings, _, _, Clauses}])
  when Bindings =:= [] orelse %% Erlang is using a list for bindings,
       Bindings =:= #{} ->    %% but Elixir is using a map.
    %% Erlang up to 24.
    erl_eval_fun_to_asm1(Module, Name, Arity, Clauses).

erl_eval_fun_to_asm1(Module, Name, Arity, Clauses) ->
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
      BeamFileRecord :: #beam_file_ext{}.

-define(ASM_CACHE_KEY(Module, Checksum),
        {?MODULE, asm_cache, Module, Checksum}).

disassemble_module(Module, #state{checksums = Checksums} = State) ->
    case Checksums of
        #{Module := Checksum} ->
            {#beam_file_ext{lines = Lines,
                            strings = Strings} = BeamFileRecord,
             Checksum} = disassemble_module1(Module, Checksum),

            State1 = State#state{lines_in_progress = Lines,
                                 strings_in_progress = Strings},
            {BeamFileRecord, State1};
        _ ->
            {#beam_file_ext{lines = Lines,
                            strings = Strings} = BeamFileRecord,
             Checksum} = disassemble_module1(Module, undefined),
            ?assert(is_binary(Checksum)),

            Checksums1 = Checksums#{Module => Checksum},
            State1 = State#state{checksums = Checksums1,
                                 lines_in_progress = Lines,
                                 strings_in_progress = Strings},
            {BeamFileRecord, State1}
    end.

disassemble_module1(Module, Checksum) when is_binary(Checksum) ->
    Key = ?ASM_CACHE_KEY(Module, Checksum),
    case persistent_term:get(Key, undefined) of
        #beam_file_ext{} = BeamFileRecord ->
            {BeamFileRecord, Checksum};
        undefined ->
            {Module, Beam, _} = get_object_code(Module),
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
    {Module, Beam, _} = get_object_code(Module),
    {ok, {Module, Checksum}} = beam_lib:md5(Beam),
    BeamFileRecord = do_disassemble_and_cache(Module, Checksum, Beam),
    {BeamFileRecord, Checksum}.

-ifdef(TEST).
-define(OBJECT_CODE_KEY(Module), {?MODULE, object_code, Module}).

override_object_code(Module, Beam) ->
    Key = ?OBJECT_CODE_KEY(Module),
    persistent_term:put(Key, Beam),
    ok.

get_object_code(Module) ->
    Key = ?OBJECT_CODE_KEY(Module),
    case persistent_term:get(Key, undefined) of
        undefined -> do_get_object_code(Module);
        Beam      -> {Module, Beam, ""}
    end.
-else.
get_object_code(Module) ->
    do_get_object_code(Module).
-endif.

do_get_object_code(Module) ->
    case code:get_object_code(Module) of
        {Module, Beam, Filename} -> {Module, Beam, Filename};
        error                    -> throw({module_not_found, Module})
    end.

do_disassemble_and_cache(Module, Checksum, Beam) ->
    Key = ?ASM_CACHE_KEY(Module, Checksum),
    BeamFileRecordExt = do_disassemble(Beam),
    persistent_term:put(Key, BeamFileRecordExt),
    BeamFileRecordExt.

do_disassemble(Beam) ->
    BeamFileRecord = beam_disasm:file(Beam),
    #beam_file{
       module = Module,
       labeled_exports = LabeledExports,
       attributes = Attributes,
       compile_info = CompileInfo,
       code = Code} = BeamFileRecord,
    Lines = get_and_decode_line_chunk(Module, Beam),
    Strings = get_and_decode_string_chunk(Module, Beam),
    BeamFileRecordExt = #beam_file_ext{
                           module = Module,
                           labeled_exports = LabeledExports,
                           attributes = Attributes,
                           compile_info = CompileInfo,
                           code = Code,
                           lines = Lines,
                           strings = Strings},
    BeamFileRecordExt.

%% The "Line" beam chunk decoding is based on the equivalent C code in ERTS.
%% See: erts/emulator/beam/beam_file.c, parse_line_chunk().
%%
%% The opposite encoding function is inside the compiler.
%% See: compiler/src/beam_asm.erl, build_line_table().

-define(CHAR_BIT, 8).
-define(sizeof_Sint16, 2).
-define(sizeof_Sint32, 4).
-define(sizeof_SWord, 4).

%% See erts/emulator/beam/big.h
%% Here, we assume a 64bit architecture with 4-byte integers.
-define(_IS_SSMALL32(X), true).
-define(IS_SSMALL(X), ?_IS_SSMALL32(X)).

get_and_decode_line_chunk(Module, Beam) ->
    case beam_lib:chunks(Beam, ["Line"]) of
        {ok, {Module, [{"Line", Chunk}]}} ->
            decode_line_chunk(Module, Chunk);
        _ ->
            undefined
    end.

decode_line_chunk(Module, Chunk) ->
    decode_line_chunk_version(Module, Chunk).

decode_line_chunk_version(
  Module,
  <<Version:32/integer,
    Rest/binary>>)
  when Version =:= 0 ->
    %% The original C code makes an assertion that the version is 0, thus the
    %% guard expression above.
    decode_line_chunk_counts_and_flags(Module, Rest).

decode_line_chunk_counts_and_flags(
  Module,
  <<_Flags:32/integer,
    _InstrCount:32/integer,
    ItemCount:32/integer,
    NameCount:32/integer,
    Rest/binary>>) ->
    UndefinedLocation = #line{name_index = 0,
                              location = 0},
    ModuleFilename = atom_to_list(Module) ++ ".erl",
    NameCount1 = NameCount + 1,
    ItemCount1 = ItemCount + 1,
    LocationSize = if
                       NameCount1 > 1 -> ?sizeof_Sint32;
                       true           -> ?sizeof_Sint16
                   end,
    Lines = #lines{item_count = ItemCount1,
                   items = [UndefinedLocation],
                   name_count = NameCount1,
                   names = [ModuleFilename],
                   location_size = LocationSize},
    NameIndex = 0,
    I = 1,
    decode_line_chunk_items(Rest, I, NameIndex, Lines).

decode_line_chunk_items(
  Rest, I, NameIndex,
  #lines{item_count = ItemCount,
         items = Items,
         location_size = LocationSize} = Lines)
  when I < ItemCount ->
    {Tag, Rest1} = read_tagged(Rest),
    case Tag of
        #tag{tag = tag_a, word_value = WordValue} ->
            NameIndex1 = WordValue,
            decode_line_chunk_items(Rest1, I, NameIndex1, Lines);
        #tag{tag = tag_i, size = 0, word_value = WordValue}
          when WordValue >= 0 ->
            LocationSize1 = if
                                WordValue > 16#FFFF -> ?sizeof_Sint32;
                                true                -> LocationSize
                            end,
            Item = #line{name_index = NameIndex,
                         location = WordValue},
            Lines1 = Lines#lines{items = Items ++ [Item],
                                 location_size = LocationSize1},
            decode_line_chunk_items(Rest1, I + 1, NameIndex, Lines1)
    end;
decode_line_chunk_items(
  Rest, I, _NameIndex,
  #lines{item_count = ItemCount} = Lines) when I =:= ItemCount ->
    decode_line_chunk_names(Rest, 1, Lines).

decode_line_chunk_names(
  <<NameLength:16/integer, Name:NameLength/binary, Rest/binary>>,
  I,
  #lines{name_count = NameCount,
         names = Names} = Lines)
  when I < NameCount ->
    Name1 = unicode:characters_to_list(Name),
    Names1 = Names ++ [Name1],
    Lines1 = Lines#lines{names = Names1},
    decode_line_chunk_names(Rest, I + 1, Lines1);
decode_line_chunk_names(<<>>, I, #lines{name_count = NameCount} = Lines)
  when I =:= NameCount ->
    Lines.

get_and_decode_string_chunk(Module, Beam) ->
    case beam_lib:chunks(Beam, ["StrT"]) of
        {ok, {Module, [{"StrT", Chunk}]}} ->
            %% There is nothing to decode: the chunk is made of concatenated
            %% binaries. The instruction knows the offset inside the chunk and
            %% the length of the binary to extract.
            Chunk;
        _ ->
            undefined
    end.

%% See: erts/emulator/beam/beam_file.c, beamreader_read_tagged().

read_tagged(
  <<LenCode:8/unsigned-integer,
    Rest/binary>>) ->
    Tag = decode_tag(LenCode band 16#07),
    if
        LenCode band 16#08 =:= 0 ->
            WordValue = LenCode bsr 4,
            Size = 0,
            {#tag{tag = Tag,
                  word_value = WordValue,
                  size = Size},
             Rest};
        LenCode band 16#10 =:= 0 ->
            <<ExtraByte:8/unsigned-integer, Rest1/binary>> = Rest,
            WordValue = ((LenCode bsr 5) bsl 8) bor ExtraByte,
            Size = 0,
            {#tag{tag = Tag,
                  word_value = WordValue,
                  size = Size},
             Rest1};
        true ->
            LenCode1 = LenCode bsr 5,
            {Count, Rest1} = if
                                 LenCode1 < 7 ->
                                     SizeBase = 2,
                                     {LenCode1 + SizeBase, Rest};
                                 true ->
                                     SizeBase = 9,
                                     {#tag{tag = tag_u,
                                           word_value = UnpackedSize},
                                      R1} = read_tagged(Rest),
                                     {UnpackedSize + SizeBase, R1}
                             end,
            <<Data:Count/binary, Rest2/binary>> = Rest1,
            case unpack_varint(Count, Data) of
                WordValue when is_integer(WordValue) andalso Tag =:= tag_i ->
                    Shift = ?CHAR_BIT * (?sizeof_SWord - Count),
                    SignExtendedValue = (WordValue bsl Shift) bsr Shift,
                    if
                        %% This first clause is true at compile-time.
                        ?IS_SSMALL(SignExtendedValue) ->
                            {#tag{tag = Tag,
                                  word_value = SignExtendedValue,
                                  size = 0},
                             Rest2}
                    end;
                WordValue when is_integer(WordValue) andalso WordValue >= 0 ->
                    {#tag{tag = Tag,
                          word_value = WordValue,
                          size = 0},
                     Rest2};
                false ->
                    {#tag{tag = tag_o,
                          ptr_value = Data,
                          size = Count},
                     Rest2}
            end
    end.

decode_tag(0) -> tag_u;
decode_tag(1) -> tag_i;
decode_tag(2) -> tag_a;
decode_tag(3) -> tag_x;
decode_tag(4) -> tag_y;
decode_tag(5) -> tag_f;
decode_tag(6) -> tag_h;
decode_tag(7) -> tag_z.

unpack_varint(Size, Data) ->
    if
        Size =< ?sizeof_SWord -> do_unpack_varint(0, Size, Data, 0);
        true                  -> false
    end.

do_unpack_varint(I, Size, <<Byte:8/unsigned-integer, Rest/binary>>, Res)
  when I < Size ->
    Res1 = (Byte bsl (Size - I - 1) * ?CHAR_BIT) bor Res,
    do_unpack_varint(I + 1, Size, Rest, Res1);
do_unpack_varint(I, I = _Size, <<>> = _Rest, Res) ->
    Res.

%% The field flags, which correspond to `Var/signed', `Var/unsigned',
%% `Var/little', `Var/big' and `Var/native' in the bitstring syntax, need to
%% be decoded here. It's the opposite to:
%%   https://github.com/erlang/otp/blob/OTP-24.2/lib/compiler/src/beam_asm.erl#L486-L493
%%
%% The field flags bit field becomes a sublist of [signed, little, native].

decode_field_flags(Instruction, Pos) when is_tuple(Instruction) ->
    FieldFlags0 = element(Pos, Instruction),
    FieldFlags1 = decode_field_flags(FieldFlags0),
    setelement(Pos, Instruction, FieldFlags1).

-spec decode_field_flags(FieldFlagsBitFieldsTuple | FieldFlagsBitField) ->
    FieldFlagsTuple | FieldFlags when
      FieldFlagsBitFieldsTuple :: {field_flags, FieldFlagsBitField},
      FieldFlagsBitField :: non_neg_integer(),
      FieldFlagsTuple :: {field_flags, FieldFlags},
      FieldFlags :: [FieldFlag],
      FieldFlag :: little | signed | native.

decode_field_flags(0) ->
    [];
decode_field_flags(FieldFlags) when is_integer(FieldFlags) ->
    lists:filtermap(
      fun
          (little) -> (FieldFlags band 16#02) == 16#02;
          (signed) -> (FieldFlags band 16#04) == 16#04;
          (native) -> (FieldFlags band 16#10) == 16#10
      end, [signed, little, native]);
decode_field_flags({field_flags, FieldFlagsBitField}) ->
    FieldFlags = decode_field_flags(FieldFlagsBitField),
    {field_flags, FieldFlags}.

fix_create_bin_list(
  [{atom, string} = Type, Seg, Unit, Flags, {u, Offset} = _Val, Size
   | Args],
  #state{strings_in_progress = Strings} = State) ->
    Seg1 = fix_integer(Seg),
    Unit1 = fix_integer(Unit),
    Size1 = {integer, Length} = fix_integer(Size),
    ?assertNotEqual(undefined, Strings),
    Binary = binary:part(Strings, {Offset, Length}),
    Val = {string, Binary},
    [Type, Seg1, Unit1, Flags, Val, Size1 | fix_create_bin_list(Args, State)];
fix_create_bin_list(
  [Type, Seg, Unit, Flags, Val, Size
   | Args],
  State) ->
    Seg1 = fix_integer(Seg),
    Unit1 = fix_integer(Unit),
    Val1 = fix_integer(Val),
    Size1 = fix_integer(Size),
    [Type, Seg1, Unit1, Flags, Val1, Size1 | fix_create_bin_list(Args, State)];
fix_create_bin_list([], _State) ->
    [].

fix_integer({u, U}) -> U;
fix_integer({i, I}) -> {integer, I};
fix_integer(Other)  -> Other.

fix_type_tagged_beam_register({tr, Reg, {Type, _, _}}) -> {tr, Reg, Type};
fix_type_tagged_beam_register(Other)                   -> Other.

get_reg_from_type_tagged_beam_register({tr, Reg, _}) -> Reg;
get_reg_from_type_tagged_beam_register(Reg)          -> Reg.

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
    GeneratedModuleName = gen_module_name(State),
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
  {bif, _, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 3, State);
pass2_process_instruction(
  {bs_add, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 2, State);
pass2_process_instruction(
  {bs_append, _, _, _, _, _, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 2, State);
pass2_process_instruction(
  {bs_create_bin, _, _, _, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 2, State);
pass2_process_instruction(
  {bs_init2, _, _, _, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 2, State);
pass2_process_instruction(
  {bs_private_append, _, _, _, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 2, State);
pass2_process_instruction(
  {BsPutSomething, _, _, _, _, _} = Instruction, State)
  when BsPutSomething =:= bs_put_binary orelse
       BsPutSomething =:= bs_put_integer ->
    replace_label(Instruction, 2, State);
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
  {'catch', _, _} = Instruction, State) ->
    replace_label(Instruction, 3, State);
pass2_process_instruction(
  {func_info, _ModRepr, _NameRepr, Arity},
  #state{generated_module_name = GeneratedModuleName,
         function_in_progress = Name}) ->
    ModRepr = {atom, GeneratedModuleName},
    NameRepr = {atom, Name},
    {func_info, ModRepr, NameRepr, Arity};
pass2_process_instruction(
  {get_map_elements, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 2, State);
pass2_process_instruction(
  {jump, _} = Instruction, State) ->
    replace_label(Instruction, 2, State);
pass2_process_instruction(
  {loop_rec, _, _} = Instruction, State) ->
    replace_label(Instruction, 2, State);
pass2_process_instruction(
  {Select, _, _, {list, Cases}} = Instruction,
  #state{mfa_in_progress = {Module, _, _},
         label_map = LabelMap} = State)
  when Select =:= select_val orelse Select =:= select_tuple_arity ->
    Cases1 = [case Case of
                  {f, OldLabel} ->
                      NewLabel = maps:get({Module, OldLabel}, LabelMap),
                      {f, NewLabel};
                  _ ->
                      Case
              end || Case <- Cases],
    Instruction1 = replace_label(Instruction, 3, State),
    setelement(4, Instruction1, {list, Cases1});
pass2_process_instruction(
  {test, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 3, State);
pass2_process_instruction(
  {test, _, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 3, State);
pass2_process_instruction(
  {test, _, _, _, _, _} = Instruction, State) ->
    replace_label(Instruction, 3, State);
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
  {'try', _, _} = Instruction, State) ->
    replace_label(Instruction, 3, State);
pass2_process_instruction(
  {wait_timeout, _, _} = Instruction, State) ->
    replace_label(Instruction, 2, State);
pass2_process_instruction(
  Instruction,
  _State) ->
    Instruction.

replace_label(
  Instruction, Pos,
  #state{mfa_in_progress = {Module, _, _},
         label_map = LabelMap}) ->
    case element(Pos, Instruction) of
        {f, 0} ->
            %% The `0' label is an exception label in the compiler, used to
            %% trigger an exception when branching. It should remain unchanged
            %% here, for more information see:
            %%   https://github.com/erlang/otp/blob/d955dc663a6d5dd03ab3360f9dd3dc0f439c7ef5/lib/compiler/src/beam_validator.erl#L26-L32
            Instruction;
        {f, OldLabel} ->
            NewLabel = maps:get({Module, OldLabel}, LabelMap),
            setelement(Pos, Instruction, {f, NewLabel});
        nofail ->
            Instruction
    end.

-spec gen_module_name(State) -> Module when
      State :: #state{},
      Module :: module().

gen_module_name(#state{fun_info = Info, functions = Functions}) ->
    #{module := Module,
      name := Name} = Info,
    Checksum = erlang:phash2(Functions),
    InternalName = lists:flatten(
                     io_lib:format(
                       "kfun__~s__~s__~b", [Module, Name, Checksum])),
    list_to_atom(InternalName).

-spec gen_function_name(Module, Name, Arity, State) -> Name when
      Module :: module(),
      Name :: atom(),
      Arity :: arity(),
      State :: #state{}.

gen_function_name(
  Module, Name, Arity,
  #state{entrypoint = {Module, Name, Arity}}) ->
    ?SF_ENTRYPOINT;
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
%% @doc Converts the fun environment to a standalone term.
%%
%% For "regular" lambdas, variables declared outside of the function body are
%% put in this `env'. We need to process them in case they reference other
%% lambdas for instance. We keep the end result to store it alongside the
%% generated module, but not inside the module to avoid an increase in the
%% number of identical modules with different environment.
%%
%% However for `erl_eval' functions created from lambdas, the env contains the
%% parsed source code of the function. We don't need to interpret it.
%%
%% TODO: `to_standalone_env()' uses `to_standalone_fun1()' to extract and
%% compile lambdas passed as arguments. It means they are fully compiled even
%% if `is_standalone_fun_still_needed()' returns false later. This is a waste
%% of resources and this function can probably be split into two parts to
%% allow the environment to be extracted before and compiled after, once we
%% are sure we need to create the final standalone fun.

to_standalone_env(#state{fun_info = #{module := Module,
                                      type := Type,
                                      env := Env},
                         options = Options} = State)
  when Module =/= erl_eval orelse Type =/= local ->
    State1 = State#state{options = maps:remove(
                                     is_standalone_fun_still_needed,
                                     Options)},
    {Env1, State2} = to_standalone_arg(Env, State1),
    State3 = State2#state{options = Options},
    {Env1, State3};
to_standalone_env(State) ->
    {[], State}.

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
to_standalone_arg(Fun, State) when is_function(Fun) ->
    to_embedded_standalone_fun(Fun, State);
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
