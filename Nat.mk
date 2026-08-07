# Copyright (c) 2022 Status Research & Development GmbH. Licensed under
# either of:
# - Apache License, version 2.0
# - MIT license
# at your option. This file may not be copied, modified, or distributed except
# according to those terms.

###########################
## nat-libs (nimbledeps) ##
###########################
# Builds miniupnpc and libnatpmp from the package installed by nimble under
# nimbledeps/pkgs2/. Invoked via $(NIMBLEDEPS_STAMP) / build-deps.
#
# NAT_TRAVERSAL_NIMBLEDEPS_DIR is evaluated at parse time, so targets that
# depend on it must be invoked via a recursive $(MAKE) call so the sub-make
# re-evaluates the variable after nimble setup has populated nimbledeps/.
#
# `ls -dt` (sort by modification time, newest first) is used to pick the
# latest installed version and is portable across Linux, macOS, and
# Windows (MSYS/MinGW).

NAT_TRAVERSAL_NIMBLEDEPS_DIR := $(shell ls -dt $(CURDIR)/nimbledeps/pkgs2/nat_traversal-* 2>/dev/null | head -1)

NAT_UNAME_M := $(shell uname -m)
ifeq ($(NAT_UNAME_M),x86_64)
  PORTABLE_NAT_MARCH := -mssse3
else
  PORTABLE_NAT_MARCH :=
endif

.PHONY: rebuild-nat-libs-nimbledeps

rebuild-nat-libs-nimbledeps:
ifeq ($(NAT_TRAVERSAL_NIMBLEDEPS_DIR),)
	$(error No nat_traversal package found under nimbledeps/pkgs2/ — run 'make build-deps' first)
endif
	@echo "Rebuilding nat-libs from $(NAT_TRAVERSAL_NIMBLEDEPS_DIR)"
ifeq ($(OS), Windows_NT)
	+ [ -e "$(NAT_TRAVERSAL_NIMBLEDEPS_DIR)/vendor/miniupnp/miniupnpc/libminiupnpc.a" ] || \
		PATH=".;$${PATH}" "$(MAKE)" -C "$(NAT_TRAVERSAL_NIMBLEDEPS_DIR)/vendor/miniupnp/miniupnpc" \
		-f Makefile.mingw CC=$(CC) CFLAGS="-Os -fPIC" libminiupnpc.a $(HANDLE_OUTPUT)
	+ "$(MAKE)" -C "$(NAT_TRAVERSAL_NIMBLEDEPS_DIR)/vendor/libnatpmp-upstream" \
		OS=mingw CC=$(CC) \
		CFLAGS="-Wall -Wno-cpp -Os -fPIC -DWIN32 -DNATPMP_STATICLIB -DENABLE_STRNATPMPERR -DNATPMP_MAX_RETRIES=4 $(CFLAGS)" \
		libnatpmp.a $(HANDLE_OUTPUT)
else
# Drop the archives nat_traversal's own nimble install hook already produced
# (with a different CC) before nimble copied the package into nimbledeps/pkgs2/.
#
# Both vendored Makefiles archive with `$?` — *only the out-of-date
# prerequisites* — and on macOS the rule is `libtool -static -o $@ $?`, which
# CREATES a fresh archive instead of updating one. nimble's install copy does
# not preserve mtimes, so build/miniupnpcstrings.h can land newer than only
# some of the build/*.d files; `$(DEPS): $(BUILDINCLUDES)` regenerates that
# subset, a matching subset of objects recompiles, and the archive is rewritten
# containing ONLY those. The rest silently vanish and the link fails with
# undefined _UPNP_* / _connecthostport / _soapPostSubmit / _addr_is_reserved.
#
# GNU make treats every prerequisite as out of date when the target is missing,
# so deleting the archive first makes `$?` expand to the full object list again.
# Linux is immune (`ar crs` merges into the existing archive) but the removal is
# harmless there — it re-archives existing objects without recompiling.
	@rm -f "$(NAT_TRAVERSAL_NIMBLEDEPS_DIR)/vendor/miniupnp/miniupnpc/build/libminiupnpc.a" \
	       "$(NAT_TRAVERSAL_NIMBLEDEPS_DIR)/vendor/libnatpmp-upstream/libnatpmp.a"
	+ "$(MAKE)" -C "$(NAT_TRAVERSAL_NIMBLEDEPS_DIR)/vendor/miniupnp/miniupnpc" \
		CC=$(CC) CFLAGS="-Os -fPIC $(PORTABLE_NAT_MARCH)" build/libminiupnpc.a $(HANDLE_OUTPUT)
	+ "$(MAKE)" CFLAGS="-Wall -Wno-cpp -Os -fPIC $(PORTABLE_NAT_MARCH) -DENABLE_STRNATPMPERR -DNATPMP_MAX_RETRIES=4 $(CFLAGS)" \
		-C "$(NAT_TRAVERSAL_NIMBLEDEPS_DIR)/vendor/libnatpmp-upstream" \
		CC=$(CC) libnatpmp.a $(HANDLE_OUTPUT)
endif