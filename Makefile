MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

.PHONY: all add_db arnold

all: pgdump.sql

arnold: pgdump.sql
	#cd ../mdb_psql_R/; make push-arnold
	../mdb_psql_R/push_to_prod.bash arnold lncddb

pgdump.sql: txt/all_gcal_events.tsv sheets/unpassed_dates.txt ../mdb_psql_R/last_visit_update.txt
	./02_populate_db_from_sheets.R
	pg_dump -U postgres lncddb > pgdump.sql

# when LunaDB.mdb changes: create psql db lncddb_r, copy to lncddb 
# last_visit_update.txt is last time visit table was changed
../mdb_psql_R/last_visit_update.txt:
	cd ../mdb_psql_R/; make

sheets/unpassed_dates.txt: xlsxunpass.jar  $(shell awk '{print $$2}' sheets_pass.list)
	 # removes passwords
	./00_getsheets.bash
	ls -l sheets/*.xls* > sheets/unpassed_dates.txt 


txt/all_gcal_events.tsv:
	./01_getCalendar.bash

xlsxunpass.jar:
	 # get xlsxunpass
	curl -L -o xlsxunpass.jar 'https://github.com/WillForan/xlsxunpass/blob/master/xlsxunpass.jar?raw=true'

