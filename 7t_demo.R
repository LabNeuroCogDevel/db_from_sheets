library(dplyr)
library(tidyr)
library(xlsx)
library(readxl)
library(stringr)
library(lubridate)
library(magrittr)

# qualtircs
fl <- Sys.glob('/Volumes/L/bea_res/Data/Temporary Raw Data/7T/1*_2*/*selfreport.csv')
demog_ext <- function(f) {
    d <- read.csv(f, skip=1);
    idx <- names(d) %>% grep(pattern='(ETHNICITY|RACE).*consider.yourself|Your.gender.|birthday..MM')
    if(length(idx) != 4) return()
    d <- d[nrow(d),idx]
    names(d) <- gsub(".*gender.*", "Sex", names(d)) %>%
         gsub('.*birthday.*','DOB',.) %>%
         gsub('\\..*','',.) 
    d$ID <- ld8from(f)
    # goes here (slower) b/c
    # errors if mergeing above with bind_rows before this data clean
    d %>%
        separate(ID,c('ID','Date'),sep="_") %>%
        mutate(Sex = as.character(Sex),
               DOB = as.character(DOB),
               # Black==AFRICAN AMERICAN, all "race" are in caps. rm everything else
               # paste back together lowercase and sep by ,
               # "Not" is an option. we can remove that
               eth = gsub('AFRICAN AMERICAN', '', RACE) %>%
                     str_extract_all('[A-Z]{3,}') %>%
                     sapply(function(x)
                         x %>% tolower %>%
                         Filter(function(e) e!='not', .) %>%
                         sort %>% paste(collapse=",")), 
               hispanic=ifelse(is.na(ETHNICITY), NA, !grepl('^Not', ETHNICITY)),
               Sex= case_when(
                       Sex == "FALSE" ~ NA_character_,
                       grepl("M",Sex,ign=T) ~ "M",
                       grepl("F",Sex,ign=T) ~ "F",
                       TRUE ~ NA_character_)) %>%
        select(ID,Date,DOB,Sex,hispanic,eth)
}

demog_qualt_raw <-
 lapply(fl,demog_ext) %>%
 bind_rows 

# update those who didn't put sex or db in the survey
db_replace <-
    demog_qualt_raw$ID[
      is.na(demog_qualt_raw$Sex)|
      is.na(demog_qualt_raw$DOB)|
      is.na(mdy(demog_qualt_raw$DOB))] %>%
    sprintf(fmt="'%s'") %>% paste(collapse=",") %>%
    sprintf(fmt="
             select id, sex, to_char(dob,'MM-DD-YYYY') dob
             from person
             natural join enroll
             where id in (%s)") %>%
    db_query %>%
    rename(ID=id, Sex=sex, DOB=dob) 

demog_qualt <-
    demog_qualt_raw %>%
    merge(db_replace, by="ID", suffixes = c("",".rep"),all.x=T) %>%
    mutate(Sex=ifelse(is.na(Sex),Sex.rep,Sex),
           DOB=ifelse(is.na(mdy(DOB)),DOB.rep,DOB),
           Age=as.numeric(ymd(Date) - mdy(DOB))/365.25) %>%
    select(-matches(".rep$"), -DOB)


# ----

# paper (put into google doc)
gsurl<- 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTBsSDjJ27hO6nOFyyyHPlLnDCms3dLWgw92dVkeue7UB4o1wZ9tMMe1Z-EA1ZM1g16pQW4HiCb62gu/pub?output=xlsx'
curl::curl_download(gsurl, "sheets/7T_packet.xlsx")

# pull tables in from excel doc
# reduce info to: ID, isdrop
info <- read_xlsx("sheets/7T_packet.xlsx", sheet="Top")
isdrop <-
    info %>%
    group_by(ID) %>%
    summarise(isdrop=any(SubDropped))

demog_sheet <- read_xlsx("sheets/7T_packet.xlsx", sheet="Demographics")
# bring it all together
d <- merge(info %>% select(-Date), demog_sheet, by=c("7TROWID", "ID")) %>%
   mutate(ID=ID %>% gsub(";.*", "", .) %>% as.numeric)

# check who is in the top sheet but does not have demographic
dids <- isdrop$ID %>% gsub(";.*", "", .) %>% as.numeric
idmissing <- setdiff(dids, d$ID) # missing
print(paste(collpase=" ", idmissing))

# merge ethnticities into on column
#  hack: go to long format (many rows per subject)
#        filter out false
#        collapse back into one value per subject
eth <-
 d %>%
 # gab only what we want
 select(ID, Date, Age, Sex,
        american_indian, asian, black, hawaiian, white, hispanic) %>%
 # eth columns into rows with col names eth and ethbool (true/false)
 # keep subj info along for the ride (id, sex, hispanic, age, isdrop)
 #   id    asian black white
 #   99999     F     T     T
 # becomes
 #   id    eth   ethbool
 #   99999 asian F
 #   99999 black T
 #   99999 white T
 gather("eth", "ethbool", -ID, -Sex, -hispanic, -Age, -Date) %>%
 filter(ethbool==T) %>%
 group_by(ID, Date, Age, Sex, hispanic) %>%
 summarise(eth=paste(collapse=",", sort(eth))) %>%
 ungroup() %>%
 mutate(Date=format(Date,"%Y%m%d"))

demog <-
    rbind(eth %>% mutate(from="sheet"),
          demog_qualt %>% mutate(from="qualt")) %>%
    merge(isdrop %>% mutate(ID=as.numeric(ID)), by="ID",all.x=T) %>%
    # remove sheet when we also have qualt
    group_by(ID,Date) %>% mutate(n=n()) %>% filter(n<2|from=="qualt") %>%
    group_by(ID) %>%
    mutate(visitno=rank(Date))  %>%
    ungroup() %>% select(-n) 


# define convience lables: age group, simplified ethnicity
demog %<>%
    mutate(
        Ageg=cut(Age, c(0, 11, 17, Inf), c("kid", "teen", "adult")),
        # simplify where multiple ethnicities
        # *,white => white+
        # *,*     => +
        # single  => stays single
        ethsimp=ifelse(grepl(",white", eth), "white+",
                ifelse(grepl(",", eth),      "+",
                ifelse(eth==""        ,      NA,
                                             eth))))

# save
write.csv(demog, "7Tdemo.csv", row.names=F)

# ----
# quick stats
countsimp <-
   demog %>%
   filter(!isdrop, visitno==1) %>%
   group_by(ethsimp, Sex) %>%
   tally() %>%
   spread(Sex, n)

counthisp <-
   demog %>%
   filter(!isdrop, visitno==1) %>%
   group_by(hispanic, Sex) %>%
   tally() %>%
   spread(Sex, n)

# finally print
pd <- function(...) print.data.frame(row.names=F,...)
demog %>% select(Ageg, isdrop) %>%  table
pd(countsimp)
pd(counthisp)

