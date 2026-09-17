{.push raises: [].}

import metrics

declarePublicCounter logos_delivery_mix_bootnode_resolve_failures,
  "number of mix bootstrap node names that did not resolve at mount and were dropped"
