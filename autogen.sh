#!/bin/sh

set -x
for f in AUTHORS NEWS README ChangeLog; do
    [ -e "$f" ] || touch "$f"
done
aclocal &&
automake --foreign --add-missing --copy &&
autoconf &&
./configure
