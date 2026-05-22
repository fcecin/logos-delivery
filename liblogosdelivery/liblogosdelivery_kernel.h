// Low-level library interfaces
// NOTE: This interface is unsupported and may be changed at any time
#pragma once
#ifndef __liblogosdelivery_kernel__
#define __liblogosdelivery_kernel__

#include "liblogosdelivery_common.h"

#ifdef __cplusplus
extern "C"
{
#endif

  // Creates a new Waku node from a JSON WakuNodeConf blob.
  // Returns an opaque handle (NULL on failure). Configuration field names match
  // Nim identifiers from WakuNodeConf (case-insensitive; unknown fields rejected).
  void *waku_new(const char *configJson,
                 FFICallBack callback,
                 void *userData);

  // Starts the Waku node.
  int waku_start(void *ctx,
                 FFICallBack callback,
                 void *userData);

  // Stops the Waku node.
  int waku_stop(void *ctx,
                FFICallBack callback,
                void *userData);

  // Subscribes the relay mesh to a shard (pubsub topic). A shard stays
  // subscribed while a direct shard subscription OR any content-topic interest
  // holds it.
  int waku_relay_subscribe_shard(void *ctx,
                           FFICallBack callback,
                           void *userData,
                           const char *pubsubTopic);

  // Removes the direct shard subscription. The pubsub topic is only torn down
  // if no content-topic interest still holds it.
  int waku_relay_unsubscribe_shard(void *ctx,
                             FFICallBack callback,
                             void *userData,
                             const char *pubsubTopic);

  // Subscribes to a content topic. pubsubTopic is the optional shard: pass an
  // empty string ("") to derive it via auto-sharding; under static/manual
  // sharding a non-empty shard must be supplied.
  int waku_relay_subscribe_content_topic(void *ctx,
                                   FFICallBack callback,
                                   void *userData,
                                   const char *contentTopic,
                                   const char *pubsubTopic);

  // Unsubscribes from a content topic. pubsubTopic is the optional shard, same
  // convention as waku_relay_subscribe_content_topic.
  int waku_relay_unsubscribe_content_topic(void *ctx,
                                     FFICallBack callback,
                                     void *userData,
                                     const char *contentTopic,
                                     const char *pubsubTopic);

  // Destroys a Waku node previously created with waku_new.
  int waku_destroy(void *ctx,
                   FFICallBack callback,
                   void *userData);

#ifdef __cplusplus
}
#endif

#endif /* __liblogosdelivery_kernel__ */
