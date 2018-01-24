

data = LOAD '/user/hive/warehouse/project.db/h1b_final/*' USING PigStorage() as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,
year:chararray,
worksite:chararray,longitute:double,latitute:double);
--dump data;

a= group data by $4;
--dump a;
tot= foreach a generate group,COUNT(data.$1);
--dump tot;

a1= filter data by $1 == 'CERTIFIED';
--dump a1;
t1= group a1 by $4;
--dump t1;
certified= foreach t1 generate group,COUNT(a1.$1);
--dump certified;

a2= filter data by $1 == 'CERTIFIED-WITHDRAWN';
--dump a2;
t2= group a2 by $4;
--dump t2;
withdrawn= foreach t2 generate group,COUNT(a2.$1);
--dump withdrawn;

joined= join certified by $0,withdrawn by $0,tot by $0;
--dump joined;

separate= foreach joined generate $0,$1,$3,$5;
--dump separate;

add= foreach separate generate $0,(float)($1+$2)*100/($3),$3;
--dump add;
add1= filter add by $1>70 and $2>1000;
--dump add1;

end= order add1 by $1 DESC;
dump end;
 

store end into '/project';



