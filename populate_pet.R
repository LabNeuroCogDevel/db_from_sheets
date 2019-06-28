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
populate_pet<- function(con, sheet_filename="sheets/PET.xlsx") {

   insert_study("PET", "mMR PET")

   sheet <- readxl::read_xlsx(sheet_filename, sheet="MASTER INFO") %>%
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
   #sheet <- readxl::read_xlsx(sheet_filename, sheet="x2 Scheduling") %>%
   #   filter(!is.na(`Luna ID`))
   t2_scan <-
      extract_pet_date(sheet, "x2 Scan Age", "x2 Scan") %>%
      match_pet_cal(cal, "scan")%>%
      mutate(visitno=2)
   add_visit(t2_scan, con)

   cat("pet scan tp2\n")
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
   added_drop_notes <-
      sheet %>%
      filter(grepl("Dropped", Type, ignore.case=T)) %>%
      select(fname=`First Name`, lname=`Last Name`, note=Notes) %>%
      filter(!is.na(note), note != "") %>%
      inner_join(pid_person) %>% select(pid, note) %>%
      add_new_only(con, "note", .)

  did <-  added_drop_notes  %>%
     select(pid) %>%
     mutate(dropcode="OLDDBDSUBJ") %>%
     add_new_only(con, "dropped", .)

  # add to join table
  if (nrow(added_drop_notes) > 0L)
     did_nid <-
        inner_join(did, added_drop_notes) %>%
        select(did, nid) %>%
        add_new_only(con, "drop_note", .)
}
