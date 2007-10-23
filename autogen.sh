#!/bin/sh
#
touch AUTHORS NEWS README ChangeLog
aclocal &&
    automake --add-missing &&
    autoconf
