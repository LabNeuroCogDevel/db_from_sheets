source("db_insert.R")
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

#brn_mch_7t <-lapply(list(sevent_xlsx, sevenTxlsx_drop),  idcols_7t) %>% bind_rows
brn_mch_7t <-idcols_7t(sevent_xlsx)

add_id_to_db(con, brn_mch_7t, double_ok_list=c("11390"))

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
# "study"      "visitno"    "vtype"      "initials"   "age"
# "sdate"      "durhr"      "vscore"     "desc"       "vtimestamp"

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
              vtype=cal_vtype_fix(vtype),
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
   match_7t_cal(cal, "scan") %>%
   insert_7t

## MR ID for 7T
raw_links <- Sys.glob("/Volumes/Hera/Raw/BIDS/7TBrainMech/rawlinks/*/0001*") %>%
    sapply(function(x) sprintf("find %s -type l -print -quit", x) %>%
                       system(intern=T))
mrids <- data.frame(
   mrid =  raw_links %>%
      sapply(function(x) sprintf("readlink %s", x) %>% system(intern=T)) %>%
      str_extract("(?<=7TBrainMech/)[^/]*"),
   ld8 = str_extract(raw_links, "(?<=rawlinks/)[^/]*"),
   stringsAsFactors =F
)

#unames <- do.call(intersect, lapply(list(sevent_xlsx,sevent_xlsx_drop), names))
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

luna_7t_lookup <-
   rbind(get_ids(sevent_xlsx_drop), get_ids(sevent_xlsx)) %>%
   inner_join(mrids)

aux_id_7t(luna_7t_lookup, "mrid", "7TMRID") %>%
   add_aux_id(con, .)
