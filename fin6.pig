data = LOAD '/user/hive/warehouse/project.db/h1b_final/*' USING PigStorage() as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,
worksite:chararray,longitute:double,latitute:double);		
--dump data;

t= group data  by $7;
--dump t;
t1= group data by ($7,$1);
--dump t1;

total= foreach t generate group,COUNT(data.$1);
--dump total;

year= foreach t1 generate group,group.$0,COUNT($1);
--dump year;
joined= join year by $1,total by $0;
--dump joined;

end= foreach joined generate FLATTEN($0),(float)($2*100)/$4,$2;
--dump end;

store end into '/project6';




