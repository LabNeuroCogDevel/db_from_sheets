-- missing sex
update person set sex='M' where pid=1411;
update person set sex='F' where pid=1388;
-- repeated lunaids
update enroll set id='11748' where id like '11748; 11515';
insert into enroll (pid,etype,id,edate) values (1346,'LunaID','11653','2018-04-30') ON CONFLICT DO NOTHING;


