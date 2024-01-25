# vim:sw=4:et:

BEGIN {
    print "%% This Source Code Form is subject to the terms of the Mozilla Public";
    print "%% License, v. 2.0. If a copy of the MPL was not distributed with this";
    print "%% file, You can obtain one at https://mozilla.org/MPL/2.0/.";
    print "%%";
    print "%% Copyright © 2024 Broadcom. All Rights Reserved. The term \"Broadcom\" refers to Broadcom Inc. and/or its subsidiaries.";
    print "%%";
    print "";
    print "-module(mfa_to_fun).";
    print "";
    print "-include_lib(\"eunit/include/eunit.hrl\").";
    print "";
    print "-include(\"src/khepri_error.hrl\").";
    print "-include(\"src/khepri_mfa.hrl\").";
    print "";

    # Max arguments supported by Horus extraction.
    max = 10;

    for (i = 0; i <= max; i++) {
        if (i == 0) {
            print "-export([args_to_list/" i ",";
        } else if (i < max) {
            print "         args_to_list/" i ",";
        } else {
            print "         args_to_list/" i "]).";
        }
    }

    print "";
    print "-dialyzer([{nowarn_function, [mfa_to_fun_0_11_test/0]}]).";

    for (i = 0; i <= max + 1; i++) {
        for (j = 0; j <= max + 1; j++) {
            if (i + j > max + 1)
                continue;

            print "";
            print "mfa_to_fun_" i "_" j "_test() ->";

            printf "    MFA = {?MODULE, args_to_list, [";
            for (n = 1; n <= i; n++) {
                if (n == 1) {
                    printf "arg" n;
                } else if (n == 8) {
                    printf ",\n                                   arg" n;
                } else {
                    printf ", arg" n;
                }
            }
            print "]},";

            if (i + j <= max) {
                print "    Fun = khepri_mfa:to_fun(MFA, " j "),";
                print "    ?assert(is_function(Fun, " j ")),";

                print "    ?assertEqual(";
                printf "       args_to_list(";
                for (n = 1; n <= i + j; n++) {
                    if (n == 1) {
                        printf "arg" n;
                    } else if (n == 10) {
                        printf ",\n                    arg" n;
                    } else {
                        printf ", arg" n;
                    }
                }
                print "),";
                printf "       Fun(";
                for (n = i + 1; n <= i + j; n++) {
                    if (n == i + 1) {
                        printf "arg" n;
                    } else {
                        printf ", arg" n;
                    }
                }
                print ")).";
            } else {
                print "    ?assertError(";
                print "      ?khepri_exception(";
                print "        tx_mfa_with_arity_too_great,";
                print "        #{mfa := MFA,";
                print "          arity := " j "}),";
                print "      khepri_mfa:to_fun(MFA, " j ")).";
            }
        }
    }

    for (i = 0; i <= max; i++) {
        print "";
        printf "args_to_list(";
        for (n = 1; n <= i; n++) {
            if (n == 1) {
                printf "Arg" n;
            } else {
                printf ", Arg" n;
            }
        }
        print ") ->";

        printf "    [";
        for (n = 1; n <= i; n++) {
            if (n == 1) {
                printf "Arg" n;
            } else {
                printf ", Arg" n;
            }
        }
        print "].";
    }
}
