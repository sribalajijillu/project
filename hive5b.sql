5b) Find the most popular top 10 job positions for H1B visa applications for each year?b) for only certified applications.

select job_title,year,count(case_status ) as a from h1b_final where year = 2011 and case_status='CERTIFIED' group by job_title,year  order by a desc limit 10; 
select job_title,year,count(case_status ) as a from h1b_final where year = 2012 and case_status='CERTIFIED' group by job_title,year  order by a desc limit 10; 
select job_title,year,count(case_status ) as a from h1b_final where year = 2013 and case_status='CERTIFIED' group by job_title,year  order by a desc limit 10; 
select job_title,year,count(case_status ) as a from h1b_final where year = 2014 and case_status='CERTIFIED' group by job_title,year  order by a desc limit 10; 
select job_title,year,count(case_status ) as a from h1b_final where year = 2015 and case_status='CERTIFIED' group by job_title,year  order by a desc limit 10; 
select job_title,year,count(case_status ) as a from h1b_final where year = 2016 and case_status='CERTIFIED' group by job_title,year  order by a desc limit 10; 

