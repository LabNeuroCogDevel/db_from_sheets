#!/usr/bin/env Rscript

# 20190702WF - init
#   test db add does what we expect


library(testthat)
library(lubridate)
# run testthat if run like ./test_dbadd.R
# alternative: Rscript -e 'testthat::test_dir(".")'
if (sys.nframe() == 0) testthat::test_dir(".")

source("db_insert.R")

con <- get_con()
get_notes_study_pid <- function(this_study) {
  note <- tbl(con, "visit") %>%
     inner_join(tbl(con, "visit_study")) %>%
     filter(study == this_study) %>%
     inner_join(tbl(con, "note"), by="pid") %>%
     select(pid, ndate, note, dropcode) %>%
     collect %>% unique
}

context("database add")
test_that("have all studies. good visit counts", {
   s <- tbl(con, "visit_study") %>% group_by(study) %>% tally %>% collect
   expect_true(all(c("P5", "BrainMechR01", "PET") %in% s$study))
   expect_gte(s$n[s$study=="P5"], 57)
   expect_gte(s$n[s$study=="PET"], 548)
   expect_gte(s$n[s$study=="BrainMechR01"], 243)
})

test_that("have notes", {
   b7t_notes <- get_notes_study_pid("BrainMechR01")
   n_7tdropcode <- b7t_notes$dropcode %>% unique %>% length
   #expect_gte(n_7tdropcode, 3)

   dropcodes <- tbl(con, "note") %>% group_by(dropcode) %>% tally() %>% collect %>% nrow
   expect_gte(dropcodes, 4)


   # 7t notes mention "7T"
   n_7tmentions <-  b7t_notes$note %>% grep("7T", ., value=T) %>% length
   expect_gte(n_7tmentions, 11)

   # notes that mention PET (and not 7T)
   pet_mention_notes <- tbl(con, "note") %>%
      filter(note %ilike% "% PET %", ! note %ilike% "%7T%") %>%
      select(note) %>% collect %>% nrow
   expect_gte(pet_mention_notes, 9)

})

test_that("visit type counts", {
   vs <- tbl(con, "visit_study") %>%
      inner_join(tbl(con, "visit")) %>%
      group_by(study, vtype) %>% tally %>% collect %>%
      filter(study %in% c("P5", "BrainMechR01", "PET")) %>%
      spread(vtype, n)

   expect_gte(nrow(vs), 3)
   expect_true(all(!is.na(vs$Scan)))
   # P5 has no behaviorals
   expect_true(all(!is.na(vs$Behavioral[vs$study!="P5"])))
})

test_that("have cal eid", {
   cal <- tbl(con, "visit") %>%
      inner_join(tbl(con, "visit_study")) %>%
      filter(!is.na(googleuri)) %>%
      group_by(study) %>%
      summarise(n=n(),
                newest=max(vtimestamp, na.rm=T),
                oldest=min(vtimestamp, na.rm=T)) %>%
      collect

   expect_gte(cal$n[cal$study == "PET"], 548)
   expect_gte(cal$n[cal$study == "P5"], 57)
   expect_gte(cal$n[cal$study == "BrainMechR01"], 244)

   # earliest date
   expect_true(cal$oldest[cal$study == "BrainMechR01"]  < ymd("20180125"))
   expect_true(cal$oldest[cal$study == "P5"]  < ymd("20160216"))
   expect_true(cal$oldest[cal$study == "PET"] < ymd("20180124"))

   # most recent date -- test will be out of date
   expect_true(cal$newest[cal$study == "BrainMechR01"]  >= ymd("20190702"))
   expect_true(cal$newest[cal$study == "P5"]  >= ymd("20171229"))
   # why is pet so far behind?
   # TODO: fix PET dates
   expect_true(cal$newest[cal$study == "PET"]  >= ymd("20190320"))
})
