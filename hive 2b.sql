--2b) find top 5 locations in the US that have got certified visa for each year.
select worksite,count(case_status) as a,year from h1b_final where year ='2011' and case_status='CERTIFIED' group by worksite,year order by a desc limit 5;
select worksite,count(case_status) as b,year from h1b_final where year ='2012' and case_status='CERTIFIED' group by worksite,year order by b desc limit 5;
select worksite,count(case_status) as c,year from h1b_final where year ='2013' and case_status='CERTIFIED' group by worksite,year order by c desc limit 5;
select worksite,count(case_status) as d,year from h1b_final where year ='2014' and case_status='CERTIFIED' group by worksite,year order by d desc limit 5;
select worksite,count(case_status) as e,year from h1b_final where year ='2015' and case_status='CERTIFIED' group by worksite,year order by e desc limit 5;
select worksite,count(case_status) as f,year from h1b_final where year ='2016' and case_status='CERTIFIED' group by worksite,year order by f desc limit 5;
