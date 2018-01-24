--5) Find the most popular top 10 job positions for H1B visa applications for each year?
--a) for all the applications
--b) for only certified applications.

H1B = LOAD 'hdfs://localhost:54310/user/hive/warehouse/ramit.db/h1b_final'  using  PigStorage('\t')  AS ( s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray , full_time_position:chararray , prevailing_wage:int , year:chararray , worksite:chararray,longitude,latitude);

--describe H1B;

delheader = FILTER H1B by $0>1;

abc = FILTER delheader by year == '$whichyear';

ghi = GROUP abc by $4;

jkl = FOREACH ghi GENERATE group as job ,abc.year as y;

mno = FOREACH jkl GENERATE job,COUNT($1);

pqr = ORDER mno by $1 DESC;

answer = LIMIT pqr 10;

dump answer;








