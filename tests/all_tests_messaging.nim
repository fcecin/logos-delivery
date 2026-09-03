## The layers above the kernel: the Messaging API, the messaging client, the
## reliable channels. The kernel suites stay in all_tests_waku.

# These suites were in all_tests_waku. The refc runtime holds 3500 global
# markers (one per module-level global with managed memory, and each `let`
# or `var` in a plain `test` body is one), and all_tests_waku with these
# suites registered 3535. Measure with, in nimcache/debug/<binary>:
#   grep -o 'nimRegisterGlobalMarker(TM' *.c | wc -l

# Waku API tests
import ./api/test_all

# Messaging API tests
import ./messaging/test_all

# Reliable Channel API tests
import ./channels/test_all
