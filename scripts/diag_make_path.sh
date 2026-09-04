#!/usr/bin/env bash
# Diagnostic. A prerequisite creates prog in a directory the Makefile put
# first on the exported PATH. Which prog does the recipe run?
set -u
T=$(mktemp -d)
mkdir -p "$T/old/bin"
printf '#!/bin/sh\necho "OLD prog ran"\n' > "$T/old/bin/prog"
chmod +x "$T/old/bin/prog"
cat > "$T/Makefile" <<'MK'
export PATH := $(CURDIR)/new/bin:$(PATH)
.PHONY: all install
all: | install
	@echo "recipe PATH: $$PATH"
	@echo "shell command -v prog: $$(command -v prog)"
	prog
	sh -c prog
install:
	mkdir -p new/bin
	cp old/bin/prog new/bin/prog
	sed -i.bak 's/OLD/NEW/' new/bin/prog
	rm -f new/bin/prog.bak
	chmod +x new/bin/prog
MK
cd "$T" || exit 1
echo "make: $(command -v make), $(make --version | head -1)"
echo "MAKEFLAGS=${MAKEFLAGS:-<unset>}"
echo "== new/bin absent at make start, created by the prerequisite"
PATH="$T/old/bin:$PATH" make all
echo "== same, with -j2 on the command line"
rm -rf new
PATH="$T/old/bin:$PATH" make -j2 all
echo "== same, serial (MAKEFLAGS cleared)"
rm -rf new
PATH="$T/old/bin:$PATH" env -u MAKEFLAGS make all
echo "== new/bin/prog already present before make starts"
PATH="$T/old/bin:$PATH" make all
