# source("db_insert.R")

extract_pet_date <- function(d, agevar, vdatevar) {
   d<-as.data.frame(d)
   missing_names <- setdiff(c(vdatevar, agevar), names(d))
   if (length(missing_names)>0L)
      stop("missing ",
           paste(collapse=", ", missing_names),
           " in dataframe;",
           "have: ",
           paste(collapse=", ", paste0("'", names(d), "'")))

   d$vtimestamp <- d[, vdatevar] # cannot get !!vdatevar to work?
   d$age <- d[, agevar] # cannot get !!vdatevar to work?
   d %>%
       mutate(initials=mk_initials(`First Name`, `Last Name`),
              vtimestamp=xlsx_date(vtimestamp, vdatevar) ) %>%
       select(id=`Luna ID`,
              age,
              vtimestamp,
              initials,
              cohort=`Cohort`)
}

match_pet_cal <- function(d, cal, vtype) {
   m <-
     d %>%
     # match calendar on yyyy-mm-dd. hours are probably off
     merge(cal %>% filter(vtype==!!vtype),
           all.x=T,
           by=c("vtimestamp", "initials")) %>%
     mutate(
              # id should be a 5 digit number
              id=as.numeric(id),
              vtype=cal_vtype_fix(vtype),
              study="PET",
              vtimestamp=sdate,
              age=age.x)

   nmatch <- length(which(!is.na(m$sdate)))
   # if (nrow(d) > 2*nmatch)
   #      stop("too few visit <-> calendar vtimestamp/sdate matches! ",
   #           nmatch, " out of ", nrow(d))
   if (nmatch>0L)
      warning(nrow(d)-nmatch, " of ", nrow(d), " xlsx '",
              vtype, "' visits not matched to google calendar")

   # TODO: fix missing 8 scans without matches
   m %>% filter(!is.na(sdate))
}


# do it all
populate_pet<- function(con, pet_sheet_filename="sheets/PET.xlsx", petid_sheet="sheets/PET_idref.xlsx") {

   insert_study("PET", "mMR PET")

   sheet <- readxl::read_xlsx(pet_sheet_filename, sheet="MASTER INFO") %>%
      filter(!is.na(`Luna ID`))
   # get pet ids
   pet <-
      sheet %>%
      select(id=`Luna ID`, dob=`DOB`,
            fname=`First Name`, lname=`Last Name`,
            adddate=`x1 Behavioral`, sex=`Sex`,
            source=`Recruitment Source`) %>%
      filter(!is.na(fname)) %>%
      mutate(id=as.character(id),
             dob=dob,
             adddate=adddate,
             hand="U")

   # add to db
   add_id_to_db(con, pet)

   cal <- read_gcal() %>%
      filter(study=="PET") %>%
      mutate(ra=get_cal_ra(desc, c("KS", "JF", "JL", "JG",
                                   "LT", "MM", "FC", "BL")))

   ## TIME POINT ONE
   cat("pet scan tp1\n")
   t1_scan <-
      extract_pet_date(sheet, "x1 Scan Age", "x1 Scan") %>%
      match_pet_cal( cal, "scan") %>%
      mutate(visitno=1)
   add_visit(t1_scan, con)

   cat("pet scan tp2\n")
   #sheet <- readxl::read_xlsx(pet_sheet_filename, sheet="x2 Scheduling") %>%
   #   filter(!is.na(`Luna ID`))
   t2_scan <-
      extract_pet_date(sheet, "x2 Scan Age", "x2 Scan") %>%
      match_pet_cal(cal, "scan")%>%
      mutate(visitno=2)
   add_visit(t2_scan, con)

   cat("pet scan tp3\n")
   t3_scan <-
      extract_pet_date(sheet, "x3 Scan Age", "x3 Scan") %>%
      match_pet_cal(cal, "scan")%>%
      mutate(visitno=3)
   add_visit(t3_scan, con)

   ## Behavioral
   print("Behavioral visits")
   t1_beh <-
      extract_pet_date(sheet, "x1 Beh Age", "x1 Behavioral") %>%
      match_pet_cal(cal, "behav") %>%
      mutate(visitno=1) %>%
      add_visit(con)

   print("x2")
   t2_beh <-
      extract_pet_date(sheet, "x2 Beh Age", "x2 Behavioral") %>%
      match_pet_cal(cal, "behav") %>%
      mutate(visitno=2) %>%
      add_visit(con)

   print("x3")
   t3_beh <-
      extract_pet_date(sheet, "x3 Beh Age", "x3 Behavioral") %>%
      match_pet_cal(cal, "behav") %>%
      mutate(visitno=3) %>%
      add_visit(con)

   ## 20190703: Year 3 on own sheet more uptodate
   print("redo v3 from schedule sheet")
   sched3 <- readxl::read_xlsx(pet_sheet_filename, sheet="x3 Scheduling") %>%
      filter(!is.na(`Luna ID`)) %>%
      mutate(dob = ymd(xlsx_date(as.character(DOB), minyear=1970)),
             x3scanage= as.numeric(ymd(xlsx_date(`x3 Scan`)) - dob)/365.25,
             x3behage = as.numeric(ymd(xlsx_date(`x3 Behvaioral`)) - dob)/365.25,
             id = as.character(`Luna ID`)
      )

   # scan
   print("x3sched scan")
   t3s_sched <- sched3 %>%
      extract_pet_date("x3scanage", "x3 Scan") %>%
      filter(!is.na(vtimestamp))
   # add
   t3s_to_add <-
      match_pet_cal(t3s_sched, cal, "scan") %>%
      mutate(visitno=3)
   # actually add
   add_visit(t3s_to_add, con)

   # behave
   print("x3behav behave")
   t3b_sched <- sched3 %>%
      extract_pet_date("x3behage", "x3 Behvaioral") %>%
      filter(!is.na(vtimestamp))
   # add
   t3b_to_add <-
      match_pet_cal(t3b_sched, cal, "behav") %>%
      mutate(visitno=3)
   # actually add
   add_visit(t3b_to_add, con)

   ## whats missing
   nocal <- rbind(
                 anti_join(t3s_sched, t3s_to_add, by="id") %>% mutate(sheettype="scan"),
                 anti_join(t3s_sched, t3s_to_add, by="id") %>% mutate(sheettype="behave"))
   # filter to only these studies
   cal_missing <-
      cal %>% filter(study=="PET",
           initials %in% nocal$initials,
           visitno==3) %>%
    right_join(nocal, by="initials", suffix=c(".gcal", ".sheet")) %>%
    select(id, initials, sheetdate=vtimestamp.sheet, gcaldate=vtimestamp.gcal, caltype=vtype, sheettype)
   # report
   cat("missing/incongruent x3 scans: sheet vs gcal\n")
   print.data.frame(cal_missing, row.names=F)

   ## add visit notes on sched sheet
   # note is assocated with timepoint not actual visit. so tie to scan (arbitraliy)
   all_added_pet_scan <- tbl(con, "visit") %>%
      inner_join(tbl(con, "visit_study")) %>%
      inner_join(tbl(con, "enroll")) %>%
      filter(study == "PET", vtype == "Scan", etype == "LunaID") %>%
      select(pid, vid, id, vtimestamp, visitno) %>% collect
  v2_note <-
     all_added_pet_scan %>% filter(visitno==2) %>%
     inner_join(sched3 %>% select(id, note=`Year 2 Notes`))
  v3_note <-
     all_added_pet_scan %>% filter(visitno==3) %>%
     inner_join(sched3 %>% select(id, note=`Year 3 Notes`))

  # add both to the db
  pet_notes_added <-
     rbind(v2_note, v3_note) %>%
     filter(!is.na(note), str_length(note)>3) %>%
     select(pid, vid, note) %>%
     add_new_only(con, "note", .)



   ### CONTACT and NOTES
   # now that we have added everyone, get their pid into memory
   pid_person <-
      tbl(con, "person") %>%
      select(pid, fname, lname) %>% collect
   ## Contacts
   print("PET Contacts")

   # pull from email and phone from "MASTER INFO" and add to db
   added_contacts <-
      sheet %>%
      select(fname=`First Name`, lname=`Last Name`, Phone, Email) %>%
      add_new_contacts(con, ., pid_person)

   ## Notes
   print("Notes")
   added_notes <-
      sheet %>%
      filter(!grepl("Dropped", Type, ignore.case=T)) %>%
      select(fname=`First Name`, lname=`Last Name`, note=Notes) %>%
      filter(!is.na(note), note != "") %>%
      inner_join(pid_person) %>% select(pid, note) %>%
      add_new_only(con, "note", .)

   ## Drop notes (TODO: assumes all drops will have notes)
   pet_drop_notes <-
      sheet %>%
      filter(grepl("Dropped", Type, ignore.case=T)) %>%
      select(fname=`First Name`, lname=`Last Name`, note=Notes) %>%
      filter(!is.na(note), note != "") %>%
      inner_join(pid_person) %>% select(pid, note) %>%
      mutate(dropcode="OLDDBDSUBJ")

   ## 20190717 - bad vein is not an old subject drop
   pet_drop_notes$dropcode[pet_drop_notes$pid == 148 &&
                             grepl("Dropped after 3rd", pet_drop_notes$note)] <-  "BAD_VEIN"

   added_drop_notes <- pet_drop_notes %>% add_new_only(con, "note", .)

   ## ADM ID
   print("ADM Ids")
   adm_ids <-
      readxl::read_xlsx(pet_sheet_filename, sheet="ASR-YSR Log") %>%
      select(lunaid=`Luna ID`, id=`ASR/YSR ADM ID`, note=Notes) %>%
      filter(!is.na(lunaid), !is.na(id)) %>%
      inner_join(tbl(con, "enroll") %>%
                 filter(etype=="LunaID") %>%
                 select(pid, lunaid=id) %>% collect) %>%
      mutate(etype="ADM")

   adm_added <- adm_ids %>%
      select(pid, id, etype) %>%
      add_new_only(con, "enroll", .)

   print("ADM note")
   recruit_note <-
      adm_ids %>%
      select(pid, note) %>%
      filter(!is.na(note)) %>%
      add_new_only(con, "note", .)

   ## PET scan ID
   pid_enroll <-
      tbl(con, "enroll") %>%
      filter(etype %like% "LunaID") %>%
      collect %>%
      select(pid, lunaid=`id`)
   petids <-
      readxl::read_xlsx(petid_sheet, sheet="Sheet1") %>%
         mutate(lunaid=as.character(`LUNA ID`),
                etype="PETID") %>%
         left_join(pid_enroll, by="lunaid") %>%
         select(-lunaid)
   nopid <- petids[is.na(petids$pid), ]
   if (nrow(nopid)>0L) {
      warning(nrow(nopid), " PETIDs without lunas ids!")
      print(nopid)
   }

   addpetid <- function(edatecol, idcol) {
     theseids <- petids %>%
         select(pid, edate=!!edatecol, id=!!idcol, etype) %>%
         filter(!is.na(id)) %>%
         mutate(edate=lubridate::ymd(edate))
     added_id <- theseids %>% filter(!is.na(pid)) %>%
          add_new_only(con, "enroll", ., idcols=c("pid", "id"))
   }

   racadded <- addpetid("Scan Date", "PET ID")
   dtzbadded <- addpetid("DTBZDate", "DTBZID")

   # ## pet scan id notes
   # TODO: this could be by visit too?
   idnotesadded <- petids %>%
      select(ndate=`Scan Date`, pid, note=Note) %>%
      filter(!is.na(note)) %>%
      mutate(ndate=lubridate::ymd(ndate) %>% as.Date) %>%
      add_new_only(con, "note", ., idcols=c("pid", "note"))
}
