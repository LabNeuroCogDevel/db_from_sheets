source("db_insert.R")

extract_pet_date <- function(d, agevar, vdatevar) {
   d<-as.data.frame(d)
   d$vtimestamp <- d[, vdatevar] # cannot get !!vdatevar to work?
   d$age <- d[, agevar] # cannot get !!vdatevar to work?
   d %>%
       mutate(initials=mk_initials(`First Name`, `Last Name`),
              vtimestamp=xlsx_date(vtimestamp, vdatevar) ) %>%
       select(id=`Luna ID`,
              age,
              vtimestamp,
              initials,
              cohort=`K/A`)
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

   sheet <- readxl::read_xlsx(sheet_filename, sheet="Completed") %>%
      filter(!is.na(`Luna ID`))
   # get pet ids
   pet <-
      sheet %>%
      select(id=`Luna ID`, dob=`DOB`,
            fname=`First Name`, lname=`Last Name`,
            adddate=`x1 Beh Date`, sex=`Sex`,
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
      extract_pet_date(sheet, "Age @ x1 Scan", "x1 Scan Date") %>%
      match_pet_cal( cal, "scan") %>%
      mutate(visitno=1)
   add_visit(t1_scan, con)

   cat("pet scan tp2\n")
   t2_scan <-
      extract_pet_date(sheet, "Age @ x2 Scan", "x2 Scan Date") %>%
      match_pet_cal(cal, "scan")%>%
      mutate(visitno=2)
   add_visit(t1_scan, con)

}
