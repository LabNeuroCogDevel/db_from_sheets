#!/usr/bin/env bash
set -euo pipefail
trap 'e=$?; [ $e -ne 0 ] && echo "$0 exited in error"' EXIT
SCRIPTDIR="$(cd $(dirname "$0"); pwd)"

#
# find where vscore is missing and update if we can find in cal_events.csv
#
# 20191216WF - init
#

psql lncddb lncd -tc "select googleuri from visit where vscore is null and vtimestamp > '20190101'"|
   xargs -I{} -n1 grep {} txt/cal_events.csv|
   cut -d, -f10- | sed 's/,/\t/'|
   perl -F"\t" -slane '@a=split/-/,$F[1]; $F[0]=~s/"//g; print "$F[0]\t$1" if $a[3] =~ m/^\s*(\d\.?\d?)/'|
   while read g s; do
      echo "update visit set vscore = $s where googleuri like \'$g\';"
   done|
   xargs -I{} -n1 echo 'psql lncddb lncd -c "{}"'
