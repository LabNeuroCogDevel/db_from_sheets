#!/usr/bin/env bash
set -e

#
# get all calendar events since 2015
# until yesterday 
# try not to download everything every time (get max of previous fetch)
#
#gcal(){ gcalcli --calendar "Luna Lab" agenda --detail longurl --tsv $@; }
gcal(){ gcalcli --calendar "Luna Lab" agenda --detail id --tsv $@; }
caltsv=txt/all_gcal_events.tsv
[ ! -s  $caltsv ] && 
 gcal 2015-01-01 $(date +%F) | sort -nr > $caltsv

max=$(sort -nr $caltsv |awk -F"\t" '{print $1;exit}')
echo querying from $max onward, applying to $(wc -l $caltsv)
(
 gcal $max $(date -d yesterday +%F)
 cat $caltsv;
) | sort -nr|uniq  > $caltsv.tmp
mv $caltsv.tmp $caltsv

echo finished, now have $(wc -l $caltsv)
echo making csv
./parse_gcal.R 
