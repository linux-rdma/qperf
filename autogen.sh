#!/bin/sh

set -x
touch AUTHORS NEWS README ChangeLog
aclocal
automake --foreign --add-missing --copy
autoconf
