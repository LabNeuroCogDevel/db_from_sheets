#!/usr/bin/env Rscript
cat("loading packages, connect to db\n")
suppressPackageStartupMessages({
 library(dplyr)
 library(dbplyr)
 library(lubridate)
 library(stringr)
})

source("db_insert.R")


cat("P5\n")
# get p5 data
p5 <-
   readxl::read_xlsx("sheets/P5.xlsx", sheet="P5 Completed") %>%
   select(id=`Luna ID`, dob=`DOB`,
         fname=`First Name`, lname=`Last Name`,
         adddate=`fMRI Date`, sex=`Sex`) %>%
   mutate(id=as.character(id),
          dob=lubridate::ymd(dob),
          hand="U")

# add id's to DB
add_id_to_db(con, p5)



cat("PET\n")
pet <-
   readxl::read_xlsx("sheets/PET.xlsx", sheet="Completed") %>%
   select(id=`Luna ID`, dob=`DOB`,
         fname=`First Name`, lname=`Last Name`,
         adddate=`x1 Beh Date`, sex=`Sex`) %>%
   filter(!is.na(fname)) %>%
   mutate(id=as.character(id),
          dob=dob,
          adddate=adddate,
          hand="U")

add_id_to_db(con, pet)

###### 7T 
cat("7T\n")
sevent_xlsx <- readxl::read_xlsx("sheets/7T.xlsx", sheet="Enrolled")

## insert people
idcols_7t <- function(sevent_xlsx) {
   sevent_xlsx %>%
    select(id=`Luna ID`, dob=DOB,
           fname=`First Name`, lname=`Last Name`,
           adddate=`Date Enrolled`, sex=`Gender`, source=Source) %>%
    mutate(id=as.character(id),
           dob=lubridate::ymd(dob),
           adddate= xlsx_date(adddate),
           hand="U") %>%
    filter(!is.na(id))
}
aux_id_7t <- function(d, id_colname, etype){
   d<-as.data.frame(d)
   d$id <- d[, id_colname]
   if (! "edate" %in% names(d)) d$edate<-NA
   d  %>%
    select(lunaid=`Luna ID`, id, edate) %>%
    filter(grepl("^\\d{5}$", lunaid )) %>%
    mutate(lunaid=as.character(lunaid) ) %>%
    mutate(etype=etype)
}

#brnMch7T <-lapply(list(sevent_xlsx, sevenTxlsx_drop),  idcols_7t) %>% bind_rows
brnMch7T <-idcols_7t(sevent_xlsx)

add_id_to_db(con, brnMch7T)

# add QualitricsID for those that have a lunaid

aux_id_7t(sevent_xlsx, "Screening ID", "QaultricsID") %>%
   add_aux_id(con, .)

sevent_xlsx_drop <- readxl::read_xlsx("sheets/7T.xlsx", sheet="Dropped")
brn_mch7t_drop <-idcols_7t(sevent_xlsx_drop)
add_id_to_db(con, brn_mch7t_drop)
aux_id_7t(sevent_xlsx_drop, "Screening ID", "QaultricsID") %>%
   add_aux_id(con, .)

## get calendar info -- buggy, use `gcalcli`
# devtools::install_github("jdeboer/gcalendar")
#library(gcalendar)
#creds <- GoogleApiCreds(appCreds = "secret.json")
#calve <- gCalendar$new(creds=creds, id="lunalncd@gmail.com")$events

calv <-
   read.csv("txt/cal_events.csv") %>%
   mutate(sdate=ymd_hms(sdate))
cal <- calv %>%
    filter(study=="7T", !grepl("cancelled", desc)) %>%
    mutate(vtimestamp=format(sdate, "%Y-%m-%d"))
# cal[sample(1:nrow(cal))%>%head,]  %>% head  

insert_study("BrainMechR01", "BrainMechR01")

## visit 1 scan
extract_7t_date <- function(d, agevar, vdatevar) {
   d<-as.data.frame(d)
   d$vtimestamp <- d[, vdatevar] # cannot get !!vdatevar to work?
   d$age <- d[, agevar] # cannot get !!vdatevar to work?
   d %>%
       mutate(initials=paste0(substr(`First Name`, 0, 1),
                              substr(`Last Name`, 0, 1) %>%
                              gsub("[[\\(].*[]\\)]", "", .))%>%
                  toupper,
              vtimestamp=xlsx_date(vtimestamp, vdatevar) ) %>%
       select(id=`Luna ID`,
              age,
              vtimestamp,
              initials)
}

match_7t_cal <- function(d, cal, vtype) {
   m <-
     d %>%
     merge(cal %>% filter(vtype==!!vtype),
           all.x=T,
           by=c("vtimestamp", "initials")) %>%
     mutate(
              # use calendar (sdate) if we have (actual time of day)
              # but need vtimestamp and sdate to be same type (POSIXct)
              vtimestamp=paste0(vtimestamp, " 00:00:00"),
              vtimestamp=ifelse(!is.na(sdate), as.character(sdate), vtimestamp),
              vtimestamp=ymd_hms(vtimestamp),
              # id should be a 5 digit number
              id=as.numeric(id),
              # who ran the scan? e.g. " - JF - "
              ra = str_extract(desc, "- *(MM|LT|JL|JF|NR|KS)") %>%
                   gsub("[- ]*", "", .),
              study="BrainMechR01",
              cohort="Control",
              age=age.x)

     nmatch <- length(which(!is.na(m$sdate)))
     if (nrow(d) > 2*nmatch)
        stop("too few visit <-> calendar vtimestamp/sdate matches! ",
             nmatch, " out of ", nrow(d))

     return(m)
}
insert_7t <- function(d) {
  d %>%
     filter(!is.na(id), !is.na(vtype)) %>%
     add_visit
}

extract_7t_date(sevent_xlsx, "Age @ Y1 Behav", "Y1 Behav Date") %>%
     match_7t_cal(cal, "behav") %>%
     insert_7t
extract_7t_date(sevent_xlsx, "Age @ Y1 MRI", "Y1 MRI Date") %>%
   match_7t_cal(d, cal, "scan") %>%
   insert_7t
