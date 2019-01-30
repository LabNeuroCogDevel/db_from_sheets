source("db_insert.R")
populate_p5<- function(con, sheet_filename="sheets/P5.xlsx") {

   insert_study("P5", "P5")

   sheet <- readxl::read_xlsx(sheet_filename, sheet="P5 Completed")

   # ADD ID
   p5_id <- sheet %>%
      select(id=`Luna ID`, dob=`DOB`,
            fname=`First Name`, lname=`Last Name`,
            adddate=`fMRI Date`, sex=`Sex`) %>%
      mutate(id=as.character(id),
             dob=lubridate::ymd(dob),
             hand="U")

   # add id's to DB
   add_id_to_db(con, p5_id)

   # get google calendar P5 entries
   cal <- read_gcal() %>%
      filter(study=="P5") %>%
      mutate(ra=get_cal_ra(desc, c("KS", "JF", "JL", "JG", "MJ")))

   # merge calendar and MR visits
   p5_visit <- sheet %>%
      filter(`MRI Done?` == 1) %>%
      mutate(initials=mk_initials(`First Name`, `Last Name`)) %>%
      select(id=`Luna ID`,
             sdate.sheet=`fMRI Date`,
             age=`Age at Scan`,
             sex=`Sex`,
             initials,
             cohort=Cohort) %>%
      mutate(id=as.character(id),
             vtimestamp=format(lubridate::ymd_hms(sdate.sheet), "%Y-%m-%d")) %>%
      merge(cal,
            by=c("initials", "vtimestamp", "sex"),
            all.x=T,
            suffixes=c("", ".y")) %>%
      mutate(visitno=1, vtype="Scan")

    nmissing <-  p5_visit %>% filter(is.na(study)) %>% nrow
    cat("missing SzP5 cal entries for ", nmissing, "/", nrow(p5_visit), "\n")

    # select only those with calendar entries
    p5_visit <- p5_visit %>%
       filter(!is.na(study)) %>%
       mutate(vtimestamp=sdate)

   # add MR to db
   add_visit(p5_visit, con)

   # TODO:
   # add contact, notes, drops
   # WM CB, Task Order
}
