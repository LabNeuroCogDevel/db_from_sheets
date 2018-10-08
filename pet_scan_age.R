#!/usr/bin/env Rscript

#
# give list of luna_date, return ages
#
suppressMessages({
library(dplyr)
library(tidyr)
library(lubridate)
})
gsheet_top <- "https://docs.google.com/spreadsheets/d/e/2PACX-1vT32ku-_l9DKLSYi8lHdeWCFolyjZy8V5NaXbYdb7vY6WWA9JFyo-9_XeCB4NvSofN3EaPEWwru51PU/pub?gid=0&single=true&output=tsv"
idlist <- commandArgs(T)
#idlist <- c("11543_20160507", "11577_20180407", "11535_20160520", "11558_20160708", "11558_20180411", "11560_20160712", "11577_20160910", "11574_20160901", "11592_20170126")
# read command args into dataframe
d.in <-
   data.frame(ld8=idlist) %>%
   separate(ld8, c("ID", "d8")) %>%
   mutate(visitdate=ymd(d8)) %>%
   unique

# get online sheet
d.sheet <-
   read.table(gsheet_top, sep="\t", header=T) %>%
  mutate(dob=mdy(dob)) %>% select(ID, dob) %>% unique

d <- merge(d.in, d.sheet, by="ID") %>%
   mutate(age=round(as.numeric(visitdate-dob)/365.25,2))

cat(paste(d$ID,d$d8,d$age, sep="\t",collapse="\n"), "\n")
