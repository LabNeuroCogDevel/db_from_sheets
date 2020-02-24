#
# default to update local database
# use 'make arnold' to push to remote

MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

.PHONY: all add_db arnold alwaysrun

all: pgdump.sql

arnold: pgdump.sql
	#cd ../mdb_psql_R/; make push-arnold
	../mdb_psql_R/push_to_prod.bash arnold lncddb

txt/all_gcal_events.tsv: alwaysrun
	./01_getCalendar.bash

txt/cal_events.csv: txt/all_gcal_events.tsv
	./parse_gcal.R

pgdump.sql: txt/cal_events.csv sheets/unpassed_dates.txt ../mdb_psql_R/last_visit_update.txt
	./02_populate_db_from_sheets.R
	psql lncddb lncd < manual_updates.sql
	pg_dump -U postgres lncddb > pgdump.sql

# when LunaDB.mdb changes: create psql db lncddb_r, copy to lncddb 
# last_visit_update.txt is last time visit table was changed
../mdb_psql_R/last_visit_update.txt:
	cd ../mdb_psql_R/; make

# check dates on password list
sheets/unpassed_dates.txt: xlsxunpass.jar  $(shell awk '{print $$2}' sheets_pass.list)
	 # removes passwords
	./00_getsheets.bash
	mkstat $@ sheets/*.xls* 

# here to record where this came from
xlsxunpass.jar:
	 # get xlsxunpass
	curl -L -o xlsxunpass.jar 'https://github.com/WillForan/xlsxunpass/blob/master/xlsxunpass.jar?raw=true'

