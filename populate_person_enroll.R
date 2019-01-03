#!/usr/bin/env Rscript
cat("loading packages, connect to db\n")
suppressPackageStartupMessages({
 library(dplyr)
 library(dbplyr)
 library(lubridate)
 library(stringr)
})



# if any cell in date column is not a date (e.g. "1/9 or 1/10")
# all dates will be given as days since 1899-12-30 as a string (e.g "43488")
xlsx_date <- function(x, msg=NULL) {
   if (is.null(msg)) msg <- substitute(x)
   if (is.character(x))
      x <- as.Date(as.numeric(x), origin="1899-12-30")
   # should be 12-30, date is off by one!?
   if (!any(na.omit(between(year(x), 2015, 2024))))
     stop(msg, ": xlsx date parse failure (", head(x),
          " ): outside of range 2015-2024")
   x <- format(x, "%Y-%m-%d")
}

# depends on valid ~/.pgpass
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                      host="arnold.wpic.upmc.edu",
                      user="postgres",
                      dbname="lncddb")

# okay if this fails -- only need to have inserted once
#will fail if tried again
insert_study <- function(study, grant) {
   studydf <- data.frame(study=study, grantname=grant)
   tryCatch( db_insert_into(con, "study", studydf),
            error=function(e)
               warning("failed to insert ", study, " study, probably okay")
           )
}
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
         select(fname, lname, dob, pid, id, adddate, sex, hand) %>%
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
get_vstudy <- function(vids_with_study){
    mash <- with(vids_with_study, paste(vid, study))
    have_vid <-
       tbl(con, "visit_study") %>%
       filter(paste(vid, study) %in% mash) %>%
       collect %>%
       merge(vids_with_study, by=c("vid", "study"))
}
missing_warn <- function(toadd, db, msg, column="id") {
    mis <- setdiff(unlist(toadd[, column]), db[, column])
    if (length(mis)>0L)
        warning("missing ", column, " in ", msg,
                " db: ", paste(sep=",", mis), "\n")
}

add_visit <- function(d) {
    # input
    # df(lunaid,vtype,vscore,vtimestamp,vistno,ra,study,cohort)
    req_cols <- c("id", "vtype", "vscore", "vtimestamp",
                  "visitno", "ra", "study", "age", "study",
                  "cohort")
    if (!all(req_cols %in% names(d)))
        stop("add_visit not given dataframe with required columns. Missing: ",
             paste(collapse=", ", setdiff(req_cols, names(d))))

    # subset to just what we care about
    d <- d[, names(d) %in% req_cols]

    # format for db
    d$vtimestamp <- format(d$vtimestamp, format="%Y-%m-%d %H:%M:%S", tz="UTC")
    if (!"vstatus" %in% names(d)) d$vstatus <- "checkedin"

    # get pid
    pid_df <- tbl(con, "enroll") %>%
        filter(etype %like% "LunaID") %>%
        select(pid, id, -etype) %>%
        merge(d, by="id")

    # warn about missing (lunaid (and maybe person) not in database)
    missing_warn(d, pid_df, "enroll")

    ## find vid or create vid
    vids_df <- get_vids(pid_df)
    to_add <- anti_join(pid_df, vids_df, by="pid")
    # add visit
    if(nrow(to_add)>0L){
        to_add %>%
            select(pid, vtype, vscore, vtimestamp, visitno, vstatus, age) %>%
            db_insert_into(con, "visit", .)
        # refetch all vids
        vids_df <- get_vids(pid_df)
    }
    missing_warn(pid_df, vids_df, "visit")

    # add visit_action
    #  vid,action=='checkedin' (vs sched), ra
    # add visit_study
    #  vid, study, cohort

    # add study and cohort back to vids_df
    db_tz <- tz(vids_df$vtimestamp[0]) # assume same timezone everywhere
    vids_with_study <-
       d %>% select(id, vtimestamp, study, cohort) %>%
       mutate(vtimestamp=ymd_hms(vtimestamp, tz=db_tz)) %>%
       merge(vids_df, by=c("id", "vtimestamp"))

    nmissing <- nrow(vids_df) - nrow(vids_with_study)
    if (nmissing > 0)
       stop("could not add study back to dataframe after geting visiti id (vid). ",
            "Missing ", nmissing, "/", nrow(vids_df), ": ",
            paste(collapse=",", setdiff(vids_df$id, vids_with_study$id)))

    already_have_study <- get_vstudy(vids_with_study)
    to_add_study <- anti_join(vids_with_study, already_have_study,  by="vid")

    to_add_study %>%
     select(vid, study, cohort) %>%
     db_insert_into(con, "visit_study", .)
}


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

#brnMch7T <-lapply(list(sevent_xlsx, sevenTxlsx_drop),  idcols_7t) %>% bind_rows
brnMch7T <-idcols_7t(sevent_xlsx)

add_id_to_db(con, brnMch7T)

sevent_xlsx_drop <- readxl::read_xlsx("sheets/7T.xlsx", sheet="Dropped")
brn_mch7t_drop <-idcols_7t(sevent_xlsx_drop)
add_id_to_db(con, brn_mch7t_drop)

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
