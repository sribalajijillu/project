--7) Create a bar graph to depict the number of applications for each year

select year,count(*)  from h1b_final group by  year order by year;

