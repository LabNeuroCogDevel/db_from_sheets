-- missing sex
update person set sex='M' where pid=1411 and sex like 'U';
update person set sex='F' where pid=1388 and sex like 'U';
-- repeated lunaids
update enroll set id='11748' where id like '11748; 11515';
--  before getting extra id 11653
insert into enroll (pid,etype,id,edate) values (1346,'LunaID','11653','2018-04-30') ON CONFLICT DO NOTHING;

-- petid to visit_enroll
insert into visit_enroll (vid, eid)
select v.vid, e.eid
  from enroll e
  left join visit_enroll ve on ve.eid = e.eid
  join visit v on e.pid=v.pid
       and e.edate = v.vtimestamp::date
  where etype like 'PETID'
   and ve.vid is null;
