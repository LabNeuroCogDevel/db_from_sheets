#source("db_insert.R")

# show dataframe with an error message if there are any rows
show_if_any <- function(d, msg) {
   if(nrow(d) <= 0L) return()
   cat(msg, "\n")
   print(d)
}

## insert people
idcols_7t <- function(sevent_xlsx) {
   sevent_xlsx %>%
    select(id=`Luna ID`, dob=DOB,
           fname=`First Name`, lname=`Last Name`,
           adddate=`Date Enrolled`, sex=`Gender`, source=Source) %>%
    mutate(id=as.character(id),
           dob=lubridate::ymd(dob),
           adddate= xlsx_date(adddate),
           hand="U",
           # 20200417 - change failing source (too long)
           # TODO: substr at max length
           #source=gsub("Gmail \\(mom said didn't remember how she heard about study\\)", "Gmail", source)) %>%
           source=substr(source, 0, 15)) %>%
    filter(!is.na(id))
}
aux_id_7t <- function(d, id_colname, etype){
   d<-as.data.frame(d)
   d$id <- d[, id_colname]
   if (! "edate" %in% names(d)) d$edate<-NA
   d  %>%
    select(lunaid=`Luna ID`, id, edate) %>%
    filter(grepl("^\\d{5}$", lunaid )) %>%
    mutate(lunaid=as.character(lunaid)) %>%
    mutate(etype=etype) %>%
   filter(!is.na(lunaid), !is.na(id))
}
extract_7t_date <- function(d, agevar, vdatevar, dobvar=NULL) {
   d<-as.data.frame(d)
   # cannot get !!vdatevar and !!vdatevar syntax to work?
   d$vtimestamp <- d[, vdatevar]
   if (agevar!="") d$age <- d[, agevar]

   res <- d %>%
       mutate(initials=paste0(substr(`First Name`, 0, 1),
                              substr(`Last Name`, 0, 1) %>%
                              gsub("[[\\(].*[]\\)]", "", .))%>%
                  toupper,
              vtimestamp=xlsx_date(vtimestamp, vdatevar) )

   if (!is.null(dobvar)) {
      dob <- dplyr::pull(d, dobvar) %>%
         xlsx_date(dobvar, minyear=1970) %>%
         lubridate::ymd()
      vdate <- lubridate::ymd(res$vtimestamp)
      res$age <- as.numeric(vdate - dob)/365.25
   }
   res %>%
      select(id=`Luna ID`,
              age,
              vtimestamp,
              initials) %>%
      # 20200114 consistant lunaid: '11748; 11515' -> '11748'
      mutate(id=gsub(";.*", "", id))
}

add_scan_as_task <- function(con, ws="/Volumes/L/bea_res/7T/fMRI/7T_fMRI_Scan_Log.xlsx", recur=0) {
   pids <- LNCDR::db_query("select pid, vid, id || '_' ||  to_char(vtimestamp, 'YYYYmmdd') as ld8
                          from visit natural join enroll where etype like 'LunaID'")
   slog <- readxl::read_xlsx(ws)
   slog_pid <-
       merge(pids, slog,  by.y="MR Center ID", by.x="ld8", all.y=T) %>%
       tidyr::separate(`MT-R`,c("MT1","MT2"), sep=",") %>%
       mutate_at(vars(matches("Rest|MGS|MPR|R2|Spec|MT|Confid")),
                 function(x) gsub('.*,','',x) %>% as.numeric)
   names(slog_pid) <- names(slog_pid) %>% gsub(' ', '_', .)
       
   slog_pid$measures <- slog_pid %>% select(-ld8, -pid, -vid) %>% jsonlite::toJSON()
   slog_pid$task <- "ScanLog"

   missing_all <- slog_pid %>% filter(is.na(vid))
   if(nrow(missing_all)>0L && recur < 1) {
       cat("missing visits for MR log for:\n")
       missing <- missing_all %>%
           select(matches('ld8|RA$|MPR|Rest|MGS.*1|Notes'))
       missing %>% select(-Notes) %>% print.data.frame(row.names=F)
       # remove a
       missing <- na.omit(missing) %>%
           filter(!grepl('drop',Notes, ignore.case=T)) %>%
           inner_join(missing_all) %>%
           select(ld8, Overall_Confidence, Study_Year, Notes)
       cat("have ", nrow(missing), " not dropped and with MGS, rest, & t1\n")
       ## add missing visits! and rerun 
       # newvisit<-data.frame
       # c("id", "vtype", "vscore", "vtimestamp", "visitno", "ra", "study", "age", "study", "cohort", "googleuri")
       # add_visit <- function(newvisit, con)
       # add_scan_as_task(con,recur=1)
   }

   # only good ones
   vtadd <- slog_pid %>% filter(!is.na(vid)) %>% select(vid, task, measures)

   add_new_only(con, 'visit_task', vtadd, idcols=c('vid','task'), add=T)
}

insert_visit_by_id<-function(con, cal, missing) {
  # ld8 split to vdate  and id
  # vdate => merged w/ all calendar (vdate and initials)
  # id =>    merged w/ db enroll->person  (dbsex, dob)

  cal$initals <- as.character(cal$initials)

  m <-
     # separate id/date
     missing %>% select(ld8) %>%
     separate(ld8,c('id','vdate')) %>%
     # find in calendar
     left_join(cal %>% filter(vtype=='scan') %>%
                mutate(vdate=gsub('-','',vtimestamp)),
               by=c("vdate")) %>%
     # find pid using enroll
     left_join(tbl(con,"enroll")%>%
                select(id,pid,edate) %>%
                collect,
               by="id") %>%
     # get initials and dob using pid
     left_join(tbl(con,"person") %>%
                mutate(initials=paste0(substr(fname,1,1),substr(lname,1,1)))%>%
                select(pid,initials,dob, dbsex=sex)  %>% collect,
               by=c("pid","initials")) %>%
     # get matching info
     mutate(agediff=abs(age-as.numeric(lubridate::ymd(vdate)-dob)/365.25),
            sexmatch=tolower(dbsex)==tolower(sex))  %>%
     # little more context about ids
     left_join(tbl(con,"enroll") %>%
                filter(etype=="LunaID"|etype=="MRID") %>%
                group_by(pid) %>%
                summarise(allids=str_flatten(id,",")) %>%
                select(pid,allids) %>%
                collect,
               by=c("pid"))
     

  # what do we have?
  cat("visits can recover with id enrolled in DB\n")
  m %>%
   filter(agediff<1, sexmatch) %>%
   select(id, vdate, agediff, sexmatch, initials, pid, allids) %>%
   print.data.frame(row.names=F)
  cat("not enrolled or missing in calendar. initials NA means no calendar\n")
  m %>%
   filter(is.na(sexmatch)|!sexmatch|agediff>1) %>%
   select(id, vdate, agediff, sexmatch, initials, pid) %>%
   print.data.frame(row.names=F)


  m  %>% select(id,vdate) 
  # 20200714: FINISH ME

}

match_7t_cal <- function(d, cal, vtype, matchfactor=2) {
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
              # remove '- MB,' b/c we dont have in ra db
              # TODO: should this use db_insert:get_cal_ra
              ra = str_extract(desc %>% gsub("- *MB *,*", "- ", .),
                               "- *(MM|LT|JL|JF|NR|KS|AF)") %>%
                   gsub("[- ]*", "", .),
              study="BrainMechR01",
              cohort="Control",
              vtype=cal_vtype_fix(vtype),
              age=age.x)

     # show RA is na
     m %>% filter(is.na(ra)) %>% select(vtimestamp, desc) %>%
        filter(complete.cases(.)) %>%
        show_if_any(msg="failed to parse RA from cal desc. see/edit txt/cal_events.csv")

     nmatch <- length(which(!is.na(m$sdate)))
     # 0*Inf == NaN. we want Inf
     if(nmatch==0 && !is.finite(matchfactor)) nmatch=1
     if (nrow(d) > matchfactor*nmatch)
        stop("too few visit <-> calendar vtimestamp/sdate matches! ",
             nmatch, " out of ", nrow(d))

     return(m %>% filter(!is.na(vtimestamp), !is.na(desc)))
}
insert_7t <- function(d, con) {
  d.good <-
     d %>%
     filter(!is.na(id), !is.na(vtype)) %>%
     # hard code remove some duplicate calendar matches
     filter(!(is.na(vscore) & id == "11746"),
            !(vscore==2.5   & id == "11718"),
            !(vscore==0     & id == "11670")) # 20180713: 9 and again at 14

  add_visit(d.good, con)
}

get_ids <- function(d) {
   d %>%
   filter(grepl("\\d{5}", `Luna ID`)) %>%
   select(matches("MRI Date|Luna ID")) %>%
   tidyr::gather("visit", "vdate", -"Luna ID") %>%
   filter(!is.na(vdate)) %>%
   mutate(vdate=xlsx_date(vdate),
          ld8=sprintf("%s_%s", `Luna ID`, gsub("-", "", vdate))) %>%
   select(ld8, `Luna ID`, edate=vdate)
}

cal_7t <- function(calfile="txt/cal_events.csv"){
  calv <-
     read.csv(calfile) %>%
     mutate(sdate=ymd_hms(sdate))
  cal <- calv %>%
      filter(study=="7T", !grepl("cancelled", desc)) %>%
      mutate(vtimestamp=format(sdate, "%Y-%m-%d")) %>%
      rename(googleuri=eid)
}

populate_7t <- function(con, sheet_filename_7t="sheets/7T.xlsx") {
  sevent_xlsx <- readxl::read_xlsx(sheet_filename_7t, sheet="Enrolled")

  cat("7t: add ids. from ",sheet_filename_7t, " in 'Enrolled' sheet\n")
  #brn_mch_7t <-lapply(list(sevent_xlsx, sevenTxlsx_drop),  idcols_7t) %>% bind_rows
  brn_mch_7t <-idcols_7t(sevent_xlsx)

  add_id_to_db(con, brn_mch_7t, double_ok_list=c("11390", "11515"), missing_okay=c('11748; 11515'))

  # add QualitricsID for those that have a lunaid

  print("7t: add qualtircs ids ('Screening ID' column)")
  aux_id_7t(sevent_xlsx, "Screening ID", "QualtricsID") %>%
     add_aux_id(con, .)

  print("7t: add dropped lunaids 'Dropped (Y1)' sheet")
  sevent_xlsx_drop <- readxl::read_xlsx(sheet_filename_7t, sheet="Dropped (Y1)")
  brn_mch7t_drop <- idcols_7t(sevent_xlsx_drop) %>%
     filter(!is.na(fname), !is.na(lname), id!=11821) #20201029 - 11821 was not enrolled, id went elsewhere

  badids <- is.na(brn_mch7t_drop$`Screening ID`)
  if (length(which(badids)) > 0L) {
     print("BAD aux ids IDS!")
     print(brn_mch7t_drop[badids, ])
     brn_mch7t_drop <- brn_mch7t_drop[!badids, ]
  }

  add_id_to_db(con, brn_mch7t_drop)
  aux_id_7t(sevent_xlsx_drop, "Screening ID", "QualtricsID") %>%
     add_aux_id(con, .)

  ## get calendar info -- buggy, use `gcalcli`
  # devtools::install_github("jdeboer/gcalendar")
  #library(gcalendar)
  #creds <- GoogleApiCreds(appCreds = "secret.json")
  #calve <- gCalendar$new(creds=creds, id="lunalncd@gmail.com")$events

  print("7t: pasre calendar")
  cal <- cal_7t()
  # "study"      "visitno"    "vtype"      "initials"   "age"
  # "sdate"      "durhr"      "vscore"     "desc"       "vtimestamp"

  print("7t: add study")
  insert_study("BrainMechR01", "BrainMechR01")

  ## visit 1 scan

  y2_sheet <- readxl::read_xlsx(sheet_filename_7t, sheet="Y2 scheduling")
  # TODO: make below a bit cleaner
  #colinfo <- list(
  #    behave1=list(sevent_xlsx, "Age @ Y1 Behav", "Y1 Behav Date"),
  #    scan1  =list(sevent_xlsx, "Age @ Y1 MRI", "Y1 MRI Date"),
  #    eeg1   =list(sevent_xlsx, "", "Y1 EEG Date", "DOB"),
  #    behave2=list(y2_sheet, "", "x2 Beh", "DOB"),
  #    scan2  =list(y2_sheet, "", "x2 Scan", "DOB"),
  #    eeg2   =list(y2_sheet, "", "x2 EEG", "DOB")
  #)    

  print("7t: add behave visits")
  extract_7t_date(sevent_xlsx, "Age @ Y1 Behav", "Y1 Behav Date") %>%
       match_7t_cal(cal, "behav") %>%
       insert_7t(con)
  print("7t: add scan visits")
  scans <- extract_7t_date(sevent_xlsx, "Age @ Y1 MRI", "Y1 MRI Date") %>%
     match_7t_cal(cal, "scan")
  insert_7t(scans, con)

  ## visit 1 EEG
  print("7t: add eeg tp1 visits")
  eegy1 <- extract_7t_date(sevent_xlsx, "", "Y1 EEG Date", "DOB")
  eegt1 <- extract_7t_date(sevent_xlsx, "", "Y1 EEG Date", "DOB") %>%
       match_7t_cal(cal, "eeg")
  insert_7t(eegt1, con)

  ## Year 2
  print("7t: add behave tp2 visits")
  behy2 <- extract_7t_date(y2_sheet, "", "x2 Beh", "DOB") %>%
       match_7t_cal(cal, "behav", matchfactor=Inf)
  insert_7t(behy2, con)
  print("7t: add scan tp2 visits")
  scansy2 <- extract_7t_date(y2_sheet, "", "x2 Scan", "DOB") %>%
     match_7t_cal(cal, "scan", matchfactor=Inf)
  insert_7t(scansy2, con)

  ## visit 2 EEG
  print("7t: add eeg tp2 visits")
  y2eeg <- extract_7t_date(y2_sheet, "", "x2 EEG", "DOB") 
  eegy2 <- extract_7t_date(y2_sheet, "", "x2 EEG", "DOB") %>%
       match_7t_cal(cal, "eeg", matchfactor=Inf)
  insert_7t(eegy2, con)


  ## MR ID for 7T
  # 20190404 - rawlinks generated by
  #  /Volumes/Hera/Projects/7TBrainMech/scripts/mri/001_dcm2bids.bash
  #  which uses visits populated above to tie lunaid to 7TMRID
  # -- needs to be run in two passes
  print("read 7t symbolic links -- 2nd pass (first pass sets visits used by linker)")
  raw_links <- Sys.glob("/Volumes/Hera/Raw/BIDS/7TBrainMech/rawlinks/*/000[12]*") %>%
      sapply(function(x) sprintf("find %s -type l -print -quit", x) %>%
                         system(intern=T))
  mrids <- data.frame(
     mrid =  raw_links %>%
        sapply(function(x) sprintf("readlink %s", x) %>% system(intern=T)) %>%
        str_extract("(?<=7TBrainMech/)[^/]*"),
     ld8 = str_extract(raw_links, "(?<=rawlinks/)[^/]*"),
     stringsAsFactors =F
  ) %>% unique

  #unames <- do.call(intersect, lapply(list(sevent_xlsx,sevent_xlsx_drop), names))

  luna_7t_lookup <-
     rbind(get_ids(sevent_xlsx_drop), get_ids(sevent_xlsx)) %>%
     inner_join(mrids)

  print("add 7t aux ids")
  aux_id_7t(luna_7t_lookup, "mrid", "7TMRID") %>%
     add_aux_id(con, .)

  # now that we have added everyone, get their pid into memory
  pid_person <-
     tbl(con, "person") %>%
     select(pid, fname, lname) %>% collect


  ## Contacts
  print("Contact")
  require(tidyr)
  # get contact info (Phone and Email) from the enroll and drop sheets
  contacts <-
     lapply(list(sevent_xlsx, sevent_xlsx_drop),
            function(x) x %>%
              select(fname=`First Name`, lname=`Last Name`, Phone, Email) %>%
              filter(!is.na(lname))) %>% bind_rows

  added_contacts <- add_new_contacts(con, contacts, pid_person)

  print("Notes")
  notes <-
     sevent_xlsx %>%
     select(fname=`First Name`, lname=`Last Name`, note=Notes) %>%
     filter(!is.na(note), note != "") %>%
     inner_join(pid_person) %>% select(pid, note)
  added_notes <- add_new_only(con, "note", notes, c("pid", "note"))

  print("Drop Notes")
  # add notes (that are reall "Reason Dropped"
  drop_notes <-
     sevent_xlsx_drop %>%
     select(fname=`First Name`, lname=`Last Name`, note=`Reason Dropped Details`,
            dropcode=`Reason Dropped`) %>%
     filter(!is.na(note), note != "") %>%
     mutate(dropcode = dropcode %>% as.character %>%
            gsub("Low IQ", "LOWIQ", .) %>%
            gsub("Uncomfortable in MRI", "MRI_UNCOMFY", .) %>%
            gsub("Clinical.*|Psychiatric.*", "PSYCH_CLINICAL", .) %>%
            gsub("No longer interested", "NOINTEREST", .) %>%
            gsub("Motion, inattention", "BADSUBJ", .) %>%
            gsub("Motion", "MOVER", .) %>%
            gsub("Claustrophobic", "CLAUSTROPHOBIC", .) %>%
            gsub("Non-removable metal", "METAL", .) %>%
            gsub("Inattention", "INATTENTIVE", .) %>%
            gsub("No contacts for MRI", "NO_ADULT", .) %>%
            gsub("Unable to contact", "NO_CONTACT", .) %>%
            gsub("Couldn't contact", "NO_CONTACT", .) %>%
            #gsub("NOINTEREST.*COVID-19.*", "NOINTEREST_CV19", .) %>%
            gsub("NOINTEREST.*COVID-19.*", "NOINTEREST", .) %>%
            gsub("Difficult schedule", "HARD_SUBJ", .)) %>%
     inner_join(pid_person) %>% select(pid, note, dropcode)

  print("Drop notes used:")
  print(unique(drop_notes$dropcode))
  added_drop_notes <- add_new_only(con, "note", drop_notes)

  # add ScanLog
  insert_task(con,'ScanLog','MR sequence numbers and scan ratings')
  add_scan_as_task(con)
  print("finished 7t")


  # read packet
}

read_7tpacket <- function(packet, sheet="Top") {
   top_sheet <- readxl::read_xlsx(packet, sheet=sheet)
   # WARNING: watch out. hopefully what first column holds doesn't change
   names(top_sheet)[1] <- 'LunaID'

   # if given two lunaids, looks like '11748; 11515'. just use first
   # b/c that exists, readxl doesn't default to pulling in as numeric
   # also need to combine Date and time to get timestamp
   top <- top_sheet %>%
       mutate(LunaID=as.numeric(gsub(';.*', '', LunaID)),
              vdate = lubridate::ymd(Date)) %>%
       filter(!is.na(LunaID)) %>%
       rename(ROWID=`7TROWID`)

   # # dont need full datetime but have code for it just in case
   if ('Time' %in% names(top))
       top <- top %>%
           mutate(vtimestamp = paste(Date, gsub('^.* ', '',Time), sep=" ") %>%
                 lubridate::ymd_hms())

   return(top)
}

# get vid and pid by merging sheet to lookup dataframe (ROWID)
read_7tpacket_merge <- function(packet, sheet, lookup){
   d <- read_7tpacket(packet, sheet)
   # use as much as we can to merge. help find typos
   merge_on <- intersect(names(d), c("LunaID", "vdate","ROWID"))
   if(length(merge_on) == 0L) stop("cannot merge {sheet}, missing ROWID: {names(d)}")
   m <-  merge(d, lookup, by=merge_on)
   if (nrow(d) != nrow(m)) {
       n_miss <- nrow(lookup) - nrow(m)
       miss <- anti_join(lookup, m, by=merge_on)
       cat(glue::glue("# warning failed match {n_miss}/{nrow(lookup)} lookup ({nrow(d)} {sheet}) to rowid. have {nrow(m)}.\n\n"))
       if(nrow(miss) < 10L) print.data.frame(miss, row.names=F)
   }

   return(m)
}

populate_7tpacket <- function(con, packet="sheets/7T_packet.xlsx") {
   top <- read_7tpacket(packet, 'Top')
   top_pid <- add_pid(con, top, source="7T packet")
   top_vid <- add_vid(top_pid, 'BrainMechR01', source="7T packet")
   lookup <- top_vid %>% select(LunaID, vdate, pid, vid, ROWID)

   # top sheet
   # TODO: add MRNotes
   #       eye tracking
   #       confrim VisitYear, rating

   overview <- read_7tpacket_merge(packet, "Overview", lookup)
   # TODO: add visit note

   # demographics
   demog_added <- read_7tpacket_merge(packet, 'Demographics', lookup) %>%
       demog_derive_eth() %>%
       mk_visit_task('Demographics', con=con, add=T)

   # copy sheets directly to db
   # Top, Overview, and Demographics handled individually
   # CANTAB is just a list of what the did
   # HVLT ignored for now
   all_sheets <- readxl::excel_sheets(packet)
   copy_sheets <- all_sheets %>%
       grep(value=T, invert=T,
            pattern="Top|Overview|HVLT|Demographics|CANTAB")

   # 20210326: tasks not already in DB:
   # need_tasks <- c("PreTest", "EyeBatteryRunSheet", "PANAS", "PreScanFemale", "PreScan", "PostScan", "DTS", "RT-18", "WHO-QOL")
   # lapply(need_tasks, function(x) DBI::dbExecute(con, glue::glue("insert into task (task) values ('{x}')")))
   
   for(sheet_name in copy_sheets) {
    sheet_data <- read_7tpacket_merge(packet, sheet_name, lookup)
    task_name <- gsub('^((Pre|Post)(Test|Scan).*)Quest', '\\1', sheet_name)
    tryCatch({
        task_added <- mk_visit_task(sheet_data, task_name, con=con, add=T)
        print(nrow(task_added))
      },
        error=function(e) print(e)
    )
   }
}

