#!/usr/bin/env Rscript
source("db_insert.R")
source("populate_7t.R")
source("populate_p5.R")
source("populate_pet.R")

con <- get_con()
populate_p5(con)
populate_pet(con)
populate_7t(con)
warnings()
testthat::test_dir(".")
