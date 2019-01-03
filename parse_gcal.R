#!/usr/bin/env Rscript
library(dplyr)
library(stringr)
library(lubridate)
#
# parse google calendar -- called by 01_getCalendar.bash
# (from gcalcli output, also in 01_getCalendar.bash)
#

calv <-
    read.table("txt/all_gcal_events.tsv",sep="\t",header=F) %>%
    `names<-`(c("sdate","stime","edate","etime","desc")) %>%
    mutate(sdate=ymd_hm(paste(sdate,stime)),
           edate=ymd_hm(paste(edate,etime)),
           durhr=as.numeric(edate-sdate)/60,
           vscore=str_extract(desc, "[0-9.]+ ?$")%>%as.numeric,
           initials=str_extract(desc, "\\([A-Z]{2}\\)") %>% gsub("[()]","",.),
           study=str_extract(desc%>%toupper,"(7T|COG|PET)"),
           vtype=str_extract(desc%>%tolower,"(behav|scan|eeg)"),
           age=str_extract(desc,"(?<=[ -])\\d+ ?(?=yo)") %>% as.numeric,
           visitno=str_extract(desc,"(?<= x)\\d+(?= )") %>% as.numeric
           ) %>%
    select(study,visitno,vtype,initials,age,sdate,durhr,vscore,desc)
write.csv(calv,"txt/cal_events.csv")
