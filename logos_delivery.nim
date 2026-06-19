## Main module for using nwaku as a Nimble library
##
## This module re-exports the public API for creating and managing Waku nodes
## when using nwaku as a library dependency.

import logos_delivery/waku/api
export api

import logos_delivery/waku/factory/waku
export waku

import logos_delivery/api/logos_delivery_interface
export logos_delivery_interface

import logos_delivery/logos_delivery

import brokers/api_library # registerBrokerLibrary

# `git_version` is exported as a `{.strdefine.}` by several modules in the graph
# (waku.nim, waku_node.nim, nim-ffi), so it's ambiguous unqualified. Pin to
# waku's and expose an unambiguous local const for registerBrokerLibrary; the
# build injects `-d:git_version="$(git describe …)"`.
const ldGitVersion = waku.git_version

registerBrokerLibrary:
  name:
    "logosdelivery"
  version:
    ldGitVersion
  mainClass:
    LogosDeliveryInterface
  initializeRequest:
    StartAsClient
  shutdownRequest:
    Shutdown
