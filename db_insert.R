#
# database insert functions
#
cat("loading packages, connect to db\n")
suppressPackageStartupMessages({
 library(dplyr)
 library(dbplyr)
 library(lubridate)
 library(stringr)
 library(tidyr)
})

# depends on valid ~/.pgpass

get_con <- function() {
   # db_host <- "arnold.wpic.upmc.edu"
   db_host <- "localhost"
   DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                      host=db_host,
                      user="postgres",
                      dbname="lncddb")
}

read_gcal <- function(txt_file="txt/cal_events.csv") {
  calv <-
     read.csv(txt_file) %>%
     mutate(sdate=ymd_hms(sdate)) %>%
      filter(!grepl("cancelled", desc)) %>%
      mutate(vtimestamp=format(sdate, "%Y-%m-%d"),
             sex=sex%>%toupper,
             googleuri=eid)
}

get_cal_ra <- function(desc, RAs=c("KS", "JF", "JL", "JG", "MM", "LT", "AF", "NR")) {
   ra_regexp <- paste(sep="|", collapse="|", RAs)
   # get ra from desc
   ra_f <-  sprintf("(?<=- )(%s)",      ra_regexp) %>% str_extract(desc, .)
   ra_b <-  sprintf("(%s)(?=$| -|\\()", ra_regexp) %>% str_extract(desc, .)
   ra_a <-  sprintf("(%s)",             ra_regexp) %>% str_extract(desc, .)
   ra <- ifelse(is.na(ra_f), ra_b, ra_f)
   ra <- ifelse(is.na(ra), ra_a, ra)
   return(ra)
}

# get initials by remove () and [] and taking only first character
# as uppercase
rm_name_annotation <- function(name) gsub("[[\\(].*[]\\)]", "", name)
name_first_letter <- function(name) name %>% rm_name_annotation %>% substr(0, 1)
mk_initials <- function(fname, lname) {
      paste0(name_first_letter(fname), name_first_letter(lname)) %>%
      toupper
}
cal_vtype_fix <- function(vtype) {
   vtype <- as.character(vtype)
   vtype <- ifelse(vtype=="scan", "Scan", vtype)
   vtype <- ifelse(vtype=="behav", "Behavioral", vtype)
   return(as.character(vtype))
}

# if any cell in date column is not a date (e.g. "1/9 or 1/10")
# all dates will be given as days since 1899-12-30 as a string (e.g "43488")
xlsx_date <- function(x, msg=NULL, minyear=2015, maxyear=2024) {
   if (length(x) == 0L) return() # otherwise would error at bottom
   if (is.null(msg)) msg <- substitute(x)
   if (is.character(x)) {
      xn <- as.numeric(x)
      x <- as.Date(xn, origin="1899-12-30")

      # if number is really high, unix epoch instead of xlsx date
      # ifelse converts to numeric? so use this:
      e_i <- !is.na(xn) & xn>10^9
      if (any(e_i))
       x[e_i] <- as.Date(as.POSIXct(xn[e_i], origin="1970-01-01"))
   }

   if (!any(na.omit(between(lubridate::year(x), minyear, maxyear))))
     stop(msg, ": xlsx date parse failure (", head(x, n=3),
          " ...): outside of range ", minyear, "-", maxyear)
   x <- format(x, "%Y-%m-%d")
}


# only add to table what is not already there
# anti join based on idcols
add_new_only <- function(con, tname, d, idcols=names(d), add=T) {
   d_unique <- unique(d)
   ndup <- nrow(d) - nrow(d_unique)
   if (ndup > 0L ) {
      warning(tname, " input to add not unique. removing ", ndup, " dups")
      d <- d_unique
   }
   have_data <- tbl(con, tname) %>% select_(.dots=idcols) %>% collect
   need_data <- anti_join(d, have_data)
   if (nrow(need_data)>0L & add==T){
      cat("adding", nrow(need_data), "rows to", tname, "\n")
      db_insert_into(con, tname, need_data)
      # give back data as it was entered in db (probalby with id column)
      need_data <- inner_join(tbl(con, tname) %>% collect, need_data)
   }
   return(need_data)
}

# okay if this fails -- only need to have inserted once
#will fail if tried again.
# con is global
insert_study <- function(study, grant) {
   studydf <- data.frame(study=study, grantname=grant)
   tryCatch( db_insert_into(con, "study", studydf),
            error=function(e)
               warning("failed to insert ", study, " study, probably okay")
           )
}

# insert task
insert_task <- function(con, task, tdesc, modes=c('Scan'), measures=NA) {
   measures <- if(is.na(measures)) NA else as.character(jsonlite::toJSON(measures))
   modes <- jsonlite::toJSON(modes) %>% as.character
   tbldf <- data.frame(task=task, tdesc=tdesc,
                       modes=modes,
                       measures=measures)
   tryCatch( db_insert_into(con, "task", tbldf),
            error=function(e)
               warning("failed to insert ", task, " task, probably okay")
           )
}

# give: dataframe with lunaid, id, etype
# find pid from lunaid, add auxid
add_aux_id <- function(con, d) {
   # collect all now, avoid  "promise already under evaluation"
   all_enroll <- tbl(con, "enroll") %>%
      collect

   need_cols <- c("lunaid", "id", "etype", "edate")
   if (!all(names(d) %in% need_cols ))
      stop("missing needed columns for aux id insert: ",
           paste(collapse=", ", setdiff(need_cols, names(d))))
   have_pid <-
      all_enroll %>%
      filter(grepl("LunaID", etype)) %>%
      select(pid, lunaid=id) %>%
      inner_join(., d, by="lunaid", copy=T)

   if (nrow(have_pid) != nrow(d))
      warning("no lunaid in db for: ",
             paste(collapse=", ", setdiff(d$lunaid, have_pid$lunaid)))

   # dont add if already exists
   have_auxid <-
      all_enroll %>%
      filter(id %in% d$id)

   if (nrow(have_auxid) > 0)
      warning("already have aux enroll ", nrow(have_auxid),
              " of ", nrow(have_pid), " with known pids")

   # now we add the aux ids that are still missing
   to_add <-
      anti_join(have_pid, have_auxid, by="pid") %>%
      select(pid, id, etype, edate) %>%
      unique

   if (nrow(to_add)>0L) {
      print("adding aux ids:")
      print(to_add)
      db_insert_into(con, "enroll", to_add) %>%
      return()
   } else {
      warning("no ids to add!")
      return(F)
   }
}

# insert person, get pid, insert intoenroll
add_id_to_db <- function(con, d,  double_ok_list=NULL, missing_okay=NULL) {

   all_enroll <-
      tbl(con, "enroll") %>%
      inner_join(tbl(con, "person"), by="pid") %>%
      collect

   # shouldn't match any pid
   person_mashed <- with(d, paste(fname, lname, dob))
   have_pid <-
      tbl(con, "person") %>%
      filter(paste(fname, lname, dob) %in% person_mashed) %>%
      collect

   have_lunaid <-
      all_enroll %>%
      filter(grepl("LunaID", etype),
             id %in% d$id)

   # anyone in db without a lunaid?
   missing_id <-
      anti_join(have_pid, have_lunaid, by="pid") %>%
      select(pid, fname, lname, dob)  %>%
      merge(d, by=c("fname", "lname", "dob"))

   # 20210323
   # we have repeat: 7t: '11748; 11515', '11487' is '11478' ?!
   if(!is.null(missing_okay))
      missing_id <- missing_id %>% filter(!id %in% missing_okay)
   if (nrow(missing_id) > 0L){
      cat("have person but not lunaid in database!?\n")
      cat("in sheet:\n")
      print(missing_id)
      cat("in db\n")
      all_enroll %>%
         filter(pid %in% missing_id$pid ) %>%
         select(fname, lname, dob, pid, id, adddate, sex, hand) %>%
         collect %>% as.data.frame %>%
         print

   }

   # insert people we dont have
   people_to_add <- d %>%
      anti_join(have_lunaid, by="id") %>%
      anti_join(have_pid, by=c("fname", "lname"))

   bad_values <- people_to_add %>%
      filter(is.na(dob) | is.na(fname) | is.na(lname) | is.na(sex))
   if (nrow(bad_values) > 0L) {
     cat("WARNING: missing values for people to add\n")
     print.data.frame(bad_values %>% select(fname, lname, sex, dob),
                      row.names=F)
   }


   people_to_add <- people_to_add %>% anti_join(bad_values)
   # add people not in database (as either a person or lunaid)
   tryCatch({
      people_to_add %>%
         select(-id) %>%
         db_insert_into(con, "person", .)
   }, error=function(e){
      cat("Error adding people using db_insert.R:add_id_to_db\n")
      cat("20190620: issue with keys see ../mdb_psql_R/sql/06_update_seq.sql\n")
      cat("select setval('person_pid_seq', (select max(pid) from person));")
      stop(e)
   })

   to_enroll <-
      missing_id %>%
      select(-pid) %>%
      rbind(people_to_add, .)

   enroll_mash <- with(to_enroll, paste(fname, lname, dob))
   if (length(enroll_mash) == 0L){
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
   just_pids <- pid$pid # 20200709 - pid$pid in filter causes error?
   have_enroll <-
      tbl(con, "enroll") %>%
      filter(etype %like% "LunaID", pid %in% just_pids) %>%
      collect

   # if we are allowing muple lunaIDs for a single pid
   # remove pids from have_enroll that match the list
   if (!is.null(double_ok_list)) {
      have_enroll <-
         have_enroll %>%
         filter(!id  %in% double_ok_list)
   }

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
       merge(pid_df %>% select(pid, id), by="pid")
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
                " db: ", paste(collapse=", ", mis), "\n")
}

add_visit <- function(d, con) {
    # input
    req_cols <- c("id", "vtype", "vscore", "vtimestamp",
                  "visitno", "ra", "study", "age", "study",
                  "cohort", "googleuri")
    if (!all(req_cols %in% names(d)))
             stop("add_visit missing required columns: ",
                  paste(collapse=", ", setdiff(req_cols, names(d))),
                  "\n")

    # subset to just what we care about
    d <- d[, names(d) %in% req_cols]

    # format for db
    d$vtimestamp <- format(d$vtimestamp, format="%Y-%m-%d %H:%M:%S", tz="UTC")
    badidx <- is.na(d$vtimestamp)
    if (any(badidx)) warning("removing ", length(which(badidx)),
                             " nan visits of ", nrow(d))
    d <- d[!badidx, ]
    if (!"vstatus" %in% names(d)) d$vstatus <- "complete"

    # get pid
    pid_df <- tbl(con, "enroll") %>%
        filter(etype %like% "LunaID") %>%
        select(pid, id, -etype) %>%
        merge(d, by="id")

    # warn about missing (lunaid (and maybe person) not in database)
    missing_warn(d, pid_df, "enroll")

    ## find vid or create vid
    vids_df <- get_vids(pid_df)
    to_add <- anti_join(pid_df, vids_df, by="pid") %>% unique

    # add visit
    if (nrow(to_add)>0L){

       badidx <- is.na(to_add$age)
       if (any(badidx)) warning(length(which(badidx)), " no age visits of ",
                                nrow(to_add), " to_add. removing")
       to_add <- to_add[!badidx, ]

        dups <- to_add %>%
           select(pid, vtype, vtimestamp) %>%
           duplicated
        if (length(which(dups))>0L) {
           print("Dups in in visit: ")
           to_add[dups, ] %>%
              inner_join(to_add, by=c("pid", "vtype", "vtimestamp")) %>%
              print.data.frame
           stop("have dups in visit insert")
        }

        to_add %>%
            select(pid, vtype, vscore, vtimestamp, visitno, vstatus, age, googleuri) %>%
            db_insert_into(con, "visit", .)
        # refetch all vids
        vids_df <- get_vids(pid_df)
    }
    missing_warn(pid_df, vids_df, "visit")

    # ## JOIN TABLES ###
    # add visit_action
    #  vid,action=='checkedin' (vs sched), ra
    # add visit_study
    #  vid, study, cohort

    # add study and cohort back to vids_df
    db_tz <- tz(vids_df$vtimestamp[0]) # assume same timezone everywhere
    vids_with_study <-
       d %>% select(id, vtimestamp, study, cohort, ra) %>%
       mutate(vtimestamp=ymd_hms(vtimestamp, tz=db_tz)) %>%
       merge(vids_df, by=c("id", "vtimestamp"))

    nmissing <- nrow(vids_df) - nrow(vids_with_study)
    if (nmissing > 0)
       stop("could not add study back to dataframe after geting visiti id (vid). ",
            "Missing ", nmissing, "/", nrow(vids_df), ": ",
            paste(collapse=",", setdiff(vids_df$id, vids_with_study$id)))

    already_have_study <- get_vstudy(vids_with_study)

    to_add_study <- anti_join(vids_with_study, already_have_study,  by="vid")
    if (nrow(to_add_study)>0L) {
       # some info
       cat("already have", nrow(vids_with_study), "vids with study\n")
       cat("and ", nrow(already_have_study), "already_have_study\n")
       cat("will add ", nrow(to_add_study), "visit_study. like:\n")
       to_add_study %>%
          select(vid, study, cohort) %>%
          head %>%
          print.data.frame(row.names=F)

       # add what we can
       added <-
          to_add_study %>%
          select(vid, study, cohort) %>%
          add_new_only(con, "visit_study", .)

       #if we were confident we weren't adding things already there:
       # db_insert_into(con, "visit_study", .)
    }

   nora <- vids_with_study %>% with(is.na(vid) | is.na(ra))
   n_nora <- length(which(nora))
   if(n_nora>0L) {
      cat("BAD visit info: NA vid or RA!", n_nora, "\n")
      cat("do you need to update get_cal_ra RA list?!\n")
      print(vids_with_study[nora,])
      vids_with_study <- vids_with_study[!nora,]
   }
   # add visit_action -- 'complete' if date is old, 'sched' otherwise
   vids_with_study %>%
      mutate(action=ifelse(vtimestamp < today(), "complete", "sced")) %>%
      select(vid, action, ra) %>%
      add_new_only(con, "visit_action", .)
}

## add contacts (expect fname, lname, and contact type columns)
add_new_contacts <- function(con, contacts, pid_person) {
   if (!all(c("lname", "fname") %in% names(contacts))){
      stop("bad inptut to add_new_contacts. need fname and lname. have ",
           names(contacts))
   }
   # find pid
   contacts_pid <- inner_join(contacts, pid_person)
   # who dont we have?
   print("no pid for contacted persons:")
   anti_join(contacts, contacts_pid) %>%
      mutate(name=paste(fname, lname)) %>% select(name) %>%
      print.data.frame(row.names=F)
   c_as_db <-
      contacts_pid %>%
      unite("who", c("fname", "lname"), sep=" ") %>%
      gather(ctype, cvalue, -pid, -who) %>%
      filter(!is.na(cvalue), cvalue!="") %>%
      mutate(relation="Subject")
   # make sure we aren't double adding
   added_contacts <- add_new_only(con, "contact", c_as_db,
                                  c("pid", "who", "cvalue"))
}

na_col <- function(d, col, source="") {
   if (!col %in% names(d)) stop(glue::glue('no {col} in dataframe {source}'))
   missing_col <- is.na(d[[col]])
   if (any(missing_col)) {
       n_missing <- length(which(missing_col))
       cat(glue::glue("# WARNING: {col} is na for {n_missing}/{nrow(d)} {source}\n\n"))
       if(n_missing < 10) print(ret[missing_col,1:min(5,ncol(d))])
   }
   return(missing_col)
}

add_pid <- function(con, d, source="") {
   if (!'LunaID' %in% names(d)) stop('need LunaID in dataframe')
   # add pid to df using db luna ids
   # remove any that aren't in db (no pid) with warnings
   pid_luna <- tbl(con, 'enroll') %>%
       filter(etype == 'LunaID') %>%
       select(pid, LunaID=id) %>% collect
   d_pid <- merge(d, pid_luna, all.x=T, by="LunaID")

   missing_pid <- na_col(d, 'pid', source)
   if (any(missing_pid)){
       n_missing <- length(which(missing_pid))
       cat(glue::glue("WARNING: {source} missing pids for {n_missing}\n"))
       d_pid[missing_pid,] %>% select(LunaID, vdate, `7TROWID`) %>%
           print.data.frame(row.names=F)
   }
   d_pid <- d_pid[!missing_pid,]
}

# use vtimestamp and study to merge with db. add vid to dataframe
# requires pid. 
add_vid <- function(d, vdate, study="'%'", source="") {
   if (!'pid' %in% names(d)) stop('need pid in dataframe')
   if (!'vdate' %in% names(d)) stop('need vdate in dataframe')
   all_visits <- LNCDR::db_query(glue::glue('
     select pid, vid, vtimestamp::date as vdate
     from visit natural join visit_study
     where study like {study}'))

   ret <- merge(d, all_visits, by=c('pid','vdate'), all.x=T)
   missing_vid <- na_col(ret, 'vid', paste0(source, ' study ', study))
   return(ret[!missing_vid,])
}

mk_visit_task <- function(d, task_name, con, idcols=c('vid','task'), add=T) {
   if (!'vid' %in% names(d)) stop('need vid in dataframe')
   d$measures <- d %>% select(-matches('(id|pid|vid)$')) %>% jsonlite::toJSON()
   d$task <- task_name
   visit_tasks <- d %>% select(vid, task, measures) %>% filter(!is.na(vid))
   add_new_only(con, 'visit_task', visit_tasks, idcols=idcols, add=add)
}


# collapse T/F of personal values to get list of ethnicities
# assume ROWID column name is the identifier
demog_derive_eth <- function(demog) {
   eth <- demog %>%
    select(ROWID,hispanic,american_indian,asian,black,hawaiian,white) %>% 
    gather('eth','ethbool',-ROWID) %>%
    filter(ethbool==T) %>%
    group_by(ROWID) %>%
    summarise(eth=paste(collapse=",",sort(unique(eth))))
   merge(eth,demog,by="ROWID")
}
