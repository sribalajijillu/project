
data = LOAD '/user/hive/warehouse/project.db/h1b_final/*' USING PigStorage() as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,
worksite:chararray,
longitute:double,
latitute:double);
--dump data			



t= filter data  by $7 == '2011';
--dump t;
a= group t by $4;
--dump a;
add= foreach a generate group,COUNT($1);				
--dump add;

t1= filter data  by $7 == '2012';
--dump t1;
a= group t1 by $4;						
--dump a;
add1= foreach a generate group,COUNT($1);
--dump add1;


t2= filter data  by $7 == '2013';
--dump t2;
a= group t2 by $4;
--dump a;
add3= foreach a generate group,COUNT($1);
--dump add3;
 
t3= filter data  by $7 == '2014';
--dump t3;
a= group t3 by $4;								
--dump a;
add4= foreach a generate group,COUNT($1);				
--dump add4;

t4= filter data by $7 == '2015';
--dump t4;
a= group t4 by $4;
--dump a;
add5= foreach a generate group,COUNT($1);	
--dump add5;

t5= filter data  by $7 == '2016';
--dump t5;
a= group t5 by $4;
--dump a;
add6= foreach a generate group,COUNT($1);
--dump add6;

joined= join add by $0, add1 by $0, add3 by $0, add4 by $0, add5 by $0, add6 by $0;
--dump joined;

yearwise= foreach joined generate $0,$1,$3,$5,$7,$9,$11;
--dump yearwise;

growth= foreach yearwise  generate $0,
(float)($6-$5)*100/$5,
(float)($5-$4)*100/$4,
(float)($4-$3)*100/$3,
(float)($3-$2)*100/$2,
(float)($2-$1)*100/$1;
--dump growth;

avg= foreach growth generate $0,($1+$2+$3+$4+$5)/5;
--dump avg;

avggrowth= order avg by $1 desc;
--dump avggrowth;

end = limit avggrowth  5;
--dump end;

store end into '/project1b';




								


								



