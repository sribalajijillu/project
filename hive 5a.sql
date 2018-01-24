--5) Find the most popular top 10 job positions for H1B visa applications for each year?
--all applications

select job_title,year,count(case_status ) as a from h1b_final where year = 2011 group by job_title,year  order by a desc limit 10; 
select job_title,year,count(case_status ) as b from h1b_final where year = 2012 group by job_title,year  order by b desc limit 10; 
select job_title,year,count(case_status ) as c from h1b_final where year = 2013 group by job_title,year  order by c desc limit 10; 
select job_title,year,count(case_status ) as d from h1b_final where year = 2014 group by job_title,year  order by d desc limit 10; 
select job_title,year,count(case_status ) as e from h1b_final where year = 2015 group by job_title,year  order by e desc limit 10; 
select job_title,year,count(case_status ) as f from h1b_final where year = 2016 group by job_title,year  order by f desc limit 10; 
