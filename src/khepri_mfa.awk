# vim:sw=4:et:

BEGIN {
    print "%% This Source Code Form is subject to the terms of the Mozilla Public";
    print "%% License, v. 2.0. If a copy of the MPL was not distributed with this";
    print "%% file, You can obtain one at https://mozilla.org/MPL/2.0/.";
    print "%%";
    print "%% Copyright © 2024 Broadcom. All Rights Reserved. The term \"Broadcom\" refers to Broadcom Inc. and/or its subsidiaries.";
    print "%%";
    print "";
    print "-module(khepri_mfa).";
    print "";
    print "-include(\"src/khepri_error.hrl\").";
    print "";
    print "-export([to_fun/2]).";
    print "";

    print "-spec to_fun(MFA, Arity) -> Fun when";
    print "      MFA :: khepri:mod_func_args(),";
    print "      Arity :: arity(),";
    print "      Fun :: fun().";
    print "%% @private";
    print "";

    print "to_fun({Mod, Func, Args} = MFA, Arity) ->";
    print "    case {Args, Arity} of";

    # Max arguments supported by Horus extraction.
    max = 10;

    for (i = 0; i <= max; i++) {
        for (j = 0; j <= max; j++) {
            if (i + j > max)
                continue;

            printf "        {[";
            for (n = 1; n <= i; n++) {
                if (n == 1) {
                    printf "Arg" n;
                } else {
                    printf ", Arg" n;
                }
            }
            print "], " j "} ->";

            printf "            fun(";
            for (n = i + 1; n <= i + j; n++) {
                if (n == i + 1) {
                    printf "Arg" n;
                } else {
                    printf ", Arg" n;
                }
            }
            print ") ->";
            printf "                    Mod:Func(";
            for (n = 1; n <= i + j; n++) {
                if (n == 1) {
                    if (i + j < 8) {
                        printf "Arg" n;
                    } else {
                        printf "\n                      Arg" n;
                    }
                } else if (n == 10) {
                    printf ",\n                      Arg" n;
                } else {
                    printf ", Arg" n;
                }
            }
            print ")";
            print "            end;";
        }
        print "";
    }

    print "        _ ->";
    print "            ?khepri_misuse(";
    print "               tx_mfa_with_arity_too_great,";
    print "               #{mfa => MFA,";
    print "                 arity => Arity})";
    print "    end.";
}
