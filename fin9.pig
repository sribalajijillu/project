data = LOAD '/user/hive/warehouse/project.db/h1b_final/*' USING PigStorage() as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,
year:chararray,
worksite:chararray,longitute:double,latitute:double);
--dump data;

t= group data by $2;
--dump t;
tot= foreach t generate group,COUNT(data.$1);
--dump tot;

certified= filter data by $1 == 'CERTIFIED';
--dump certified;
t1= group certified by $2;
--dump t1;
totcer= foreach t1 generate group,COUNT(certified.$1);
--dump totcer;

certified= filter data by $1 == 'CERTIFIED-WITHDRAWN';
--dump certified;
t2= group certified by $2;
--dump t2;
withdrawn= foreach t2 generate group,COUNT(certified.$1);
--dump withdrawn;

joined= join totcer by $0,withdrawn by $0,tot by $0;
dump joined;

separate= foreach joined generate $0,$1,$3,$5;
--dump separate;

add= foreach separate generate $0,(float)($1+$2)*100/($3),$3;
--dump add;

add1= filter add by $1>70 and $2>1000;
--dump add1;

ans= order add1 by $1 DESC;
dump ans;


