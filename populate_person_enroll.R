#!/usr/bin/env Rscript

library(dplyr)
library(dbplyr)
# depends on valid ~/.pgpass
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), user="lncd", dbname="lncddb")

add_id_to_db <- function(con, d) {

   # shouldn't match any pid
   person_mashed <- with(d, paste(fname, lname, dob))
   have_pid <-
      tbl(con, "person") %>%
      filter(paste(fname, lname, dob) %in% person_mashed) %>%
      collect

   have_lunaid <-
      tbl(con, "enroll") %>%
      filter(etype %like% "LunaID", id %in% d$id) %>%
      collect

   # anyone in db without a lunaid?
   missing_id <-
      anti_join(have_pid, have_lunaid, by="pid") %>%
      select(pid, fname, lname, dob)  %>%
      merge(d, by=c("fname", "lname", "dob"))

   if (nrow(missing_id) > 0L){
      cat("have person but not lunaid in databae!?\n")
      cat("in sheet:\n")
      print(missing_id)
      cat("in db\n")
      tbl(con, "enroll") %>%
         inner_join(tbl(con, "person"), by="pid") %>%
         filter(pid %in% missing_id$pid ) %>%
         select(fname, lname, dob, pid, id, adddate,sex, hand) %>%
         collect %>% as.data.frame %>%
         print

   }

   # insert people we dont have
   people_to_add <- d %>%
      anti_join(have_lunaid, by="id") %>%
      anti_join(have_pid, by=c("fname", "lname"))
   # add people not in database (as either a person or lunaid)
   db_insert_into(con, "person", people_to_add %>% select(-id))


   to_enroll <- rbind(people_to_add, missing_id %>% select(-pid))

   enroll_mash <- with(to_enroll, paste(fname, lname, dob))
   if(length(enroll_mash) == 0L){
      warning("everyone is enrolled already!")
      return()
   }

   # 1. get db "person" id for everyone yet to be enrolled
   # 2. add back any missing id people
   # 3. remove duplicates
   pid <- tbl(con, "person") %>%
      select(pid, fname, lname, dob) %>%
      filter(paste(fname, lname, dob) %in% enroll_mash) %>%
      collect %>%
      merge(to_enroll, ., by=c("fname", "lname", "dob")) %>%
      # issues with insert created duplicates, unlike to happen again
      arrange(adddate) %>%
      filter(!duplicated(paste(fname, lname, sex)))

   # check db: have we already enrolled these people
   have_enroll <-
      tbl(con, "enroll") %>%
      filter(etype %like% "LunaID", pid %in% pid$pid) %>%
      collect

   # only add those who haven't
   to_add<-
      pid %>%
      anti_join(have_enroll, by="pid") %>%
      select(pid, edate=adddate, id) %>%
      mutate(etype="LunaID")

   if (nrow(to_add)>0L) db_insert_into(con, "enroll", to_add)
}


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
