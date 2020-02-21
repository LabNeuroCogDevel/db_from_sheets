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

-- missing birc/nic ids
drop table if exists missing_temp;
create temp table  missing_temp as
with m (ld8, nic) as (values
 ('10161_20120517','120517160603'),
 ('10216_20160113','160113170726'),
 ('10173_20130523','130523154926'),
 ('10344_20101214','101214545011'),
 ('10202_20180319','180319154612')) 
select v.pid,
 vid,ld8,
 vtimestamp::date as edate,
 nic,
 'BIRC' as etype
 from visits_view v join m on m.ld8 like v.lunaid||'_'||v.ymd left join enroll e on e.id= m.nic where e.id is null;

insert into enroll (pid, id, etype, edate) select pid, nic, etype, edate from missing_temp;
insert into visit_enroll (vid, eid) select vid, eid from missing_temp t join enroll e on e.id=t.nic;
