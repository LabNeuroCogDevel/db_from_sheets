add_db: all_gcal_events.tsv unpass 
	./02_populate_db_from_sheets.R

unpass: sheets_pass.list xlsxunpass.jar
	 # removes passwords
	./00_getsheets.bash

all_gcal_events.tsv:
	./01_getCalendar.bash

xlsxunpass.jar:
	 # get xlsxunpass
	curl -L -o xlsxunpass.jar 'https://github.com/WillForan/xlsxunpass/blob/master/xlsxunpass.jar?raw=true'

