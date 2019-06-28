#!/usr/bin/env Rscript
suppressPackageStartupMessages({
library(dplyr)
library(stringr)
library(lubridate)
})
#
# parse google calendar -- called by 01_getCalendar.bash
# (from gcalcli output, also in 01_getCalendar.bash)
#

allcal <- read.table("txt/all_gcal_events.tsv",
                     sep="\t", header=F, comment.char="",
                     quote=NULL)
cat("have ", nrow(allcal), "rows going into R")

# 2018-02-16	13:00	2018-02-16	16:30	7T x1 Scan - 19 yof (AI) - JF - 4.5
# "7T",1,"scan","AI",19,2018-02-16 13:00:00,3.5,4.5,"7T x1 Scan - 19 yof (AI) - JF - 4.5"
calv <-
   allcal %>%
    `names<-`(c("sdate", "stime", "edate", "etime", "eid", "desc")) %>%
    mutate(sdate=ymd_hm(paste(sdate, stime)),
           edate=ymd_hm(paste(edate, etime)),
           durhr=as.numeric(edate-sdate)/60,
           vscore=str_extract(desc, "[0-9.]+ ?$")%>%as.numeric,
           initials=str_extract(desc, "\\([A-Z]{2}\\)") %>% gsub("[()]", "", .),
           study=str_extract(desc%>%toupper, "(7T|COG|PET|P5|SZ|SCHIZOPHRENIA)"),
           study=ifelse(study %in% c("SZ", "SCHIZOPHRENIA"), "P5", study), # Sz is P5
           vtype=str_extract(desc%>%tolower, "(behav|scan|eeg)"),
           age=str_extract(desc, "(?<=[ -])\\d+ ?(?=yo)") %>% as.numeric,
           sex=str_extract(desc%>%tolower, "(?<=yo)(m|f)"),
           visitno=str_extract(desc, "(?<= x)\\d+(?= )") %>% as.numeric
           ) %>%
    select(study, visitno, vtype, initials,
           age, sex, sdate, durhr, vscore, eid, desc)


cat(" and ", nrow(calv), "going out\n")
write.csv(calv, "txt/cal_events.csv", row.names=F)


## get calendar info -- buggy, use `gcalcli`
# devtools::install_github("jdeboer/gcalendar")
#library(gcalendar)
#creds <- GoogleApiCreds(appCreds = "secret.json")
#calve <- gCalendar$new(creds=creds, id="lunalncd@gmail.com")$events
