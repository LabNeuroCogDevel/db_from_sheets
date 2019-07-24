.PHONY: all lncddb add_db unpass
all: pgdump.sql

arnold:
	cd ../mdb_psql_R/; make push-arnold

pgdump.sql: all_gcal_events.tsv unpass lncddb
	./02_populate_db_from_sheets.R
	pg_dump -U postgres lncddb > pgdump.sql

lncddb: 
	# create lncddb_r to lncddb
	cd ../mdb_psql_R/; make

unpass: xlsxunpass.jar  $(shell awk '{print $$2}' sheets_pass.list)
	 # removes passwords
	./00_getsheets.bash

all_gcal_events.tsv:
	./01_getCalendar.bash

xlsxunpass.jar:
	 # get xlsxunpass
	curl -L -o xlsxunpass.jar 'https://github.com/WillForan/xlsxunpass/blob/master/xlsxunpass.jar?raw=true'

