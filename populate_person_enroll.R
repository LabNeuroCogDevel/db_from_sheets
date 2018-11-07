#!/usr/bin/env Rscript

library(dplyr)
library(dbplyr)
# depends on valid ~/.pgpass
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),host="arnold.wpic.upmc.edu", user="postgres", dbname="lncddb")

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
      cat("have person but not lunaid in database!?\n")
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

get_vids <- function(pid_df) {
    visit_mash <- with(pid_df, paste(pid, vtype, vtimestamp))
    have_vid <-
       tbl(con, "visit") %>%
       filter(paste(pid, vtype, vtimestamp) %in% visit_mash) %>%
       collect %>%
       merge(pid_df %>% select(pid,id),by="pid")
}
get_vstudy <- function(vids_df){
    mash <- with(vids_df, paste(vid, study))
    have_vid <-
       tbl(con, "visit_study") %>%
       filter(paste(vid, study) %in% mash) %>%
       collect %>%
       merge(vids_df,by=c("vid","study"))
}
missing_warn <- function(toadd,db,msg,column="id") {
    mis <- setdiff(unlist(toadd[,column]), db[,column])
    if(length(mis)>0L)
        warning("missing ", column, " in ", msg, " db: ", paste(sep=",",mis), "\n")
}

add_visit <- function(d) {
    # input
    # df(lunaid,vtype,vscore,vtimestamp,vistno,ra,study,cohort) 
    if(! all(names(d) %in%
             c('id','vtype','vscore','vtimestamp','visitno','ra','study','age','study')) )
        stop("add_visit not given dataframe will required colums")

    # format for db
    d$vtimestamp=format(d$vtimestamp,format="%Y-%m-%d %H:%M:%S",tz="UTC")
    if(!"vstatus" %in% names(d)) d$vstatus='checkedin'

    # get pid
    pid_df <- tbl(con,"enroll") %>%
        filter(etype %like% "LunaID") %>%
        select(pid,id,-etype) %>%
        merge(d,by="id")

    # warn about missing (lunaid (and maybe person) not in database)
    missing_warn(d,pid_df,"enroll")

    ## find vid or create vid
    vids_df <- get_vids(pid_df)
    to_add <- anti_join(pid_df, vids_df, by="pid")
    # add visit
    if(nrow(to_add)>0L){
        to_add %>%
            select(pid,vtype,vscore,vtimestamp,visitno,vstatus,age) %>%
            db_insert_into(con, "visit", .)
        # refetch all vids
        vids_df <- get_vids(pid_df)
    }
    missing_warn(pid_df,vids_df, "visit")
    

    # add visit_action
    #  vid,action=='checkedin' (vs sched), ra
    # add visit_study
    #  vid, study, cohort

    to_add_study <-
        get_vstudy(vids_df) %>%
        anti_join(vids_df,by=c("vid","study")) %>%
        # get study back
        merge(d, by=c("pid","vtimestamp","vtype"))

    to_add_study %>%
     select(vid, study, cohort) %>%
     db_insert_into(con, "visit_study", .)
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

###### 7T 
sevenTxlsx <- readxl::read_xlsx("sheets/7T.xlsx", sheet="Enrolled") 

## insert people
BrnMch7T <- sevenTxlsx %>%
    select(id=`Luna ID`, dob=DOB,
           fname=`First Name`, lname=`Last Name`,
           adddate=`Date Enrolled`, sex=`Gender`,source=Source) %>%
    mutate(id=as.character(id),
           dob=lubridate::ymd(dob),
           adddate=as.Date(as.numeric(adddate),origin="1899-12-30"),
           hand="U") %>%
    filter(!is.na(id))

add_id_to_db(con, BrnMch7T)

## get calendar info -- buggy, use `gcalcli`
# devtools::install_github("jdeboer/gcalendar")
#library(gcalendar)
#creds <- GoogleApiCreds(appCreds = "secret.json")
#calve <- gCalendar$new(creds=creds, id="lunalncd@gmail.com")$events


# okay if this fails -- only need to have inserted once will fail if tried again
data.frame(study="BrainMechR01", grantname="BrainMechR01") %>% 
    db_insert_into(con, "study", .)

BM7T_visit <-
    sevenTxlsx %>% 
    select(id=`Luna ID`,
           age=`Age @ Y1 Behav`,
           vtimestamp=`Y1 Behav Date`) %>%
    mutate(vtype="Behavioral", ra="db_add",vscore=NA, visitno=1, study="BrainMechR01")

