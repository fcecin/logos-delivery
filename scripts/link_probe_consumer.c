/* Consumer fixture for scripts/link_probe.sh: references the public API the
 * way an application does, so the artifact under test must define it. Built
 * with PROBE_CONSUMER=<dir holding liblogosdelivery.h>. Never executed. */
#include <stdint.h>
#include "liblogosdelivery.h"

/* Address-taken into a volatile table: no optimisation level drops them. */
static void *volatile api_refs[] = {
    (void *)logosdelivery_version,
    (void *)logosdelivery_add_event_listener,
    (void *)logosdelivery_remove_event_listener,
};

int main(void)
{
  return api_refs[0] == (void *)0;
}
