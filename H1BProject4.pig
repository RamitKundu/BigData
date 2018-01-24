--4)Which top 5 employers file the most petitions each year? - Case Status - ALL

H1B = LOAD 'hdfs://localhost:54310/user/hive/warehouse/ramit.db/h1b_final'  using  PigStorage('\t')  AS ( s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray , full_time_position:chararray , prevailing_wage:int , year:chararray , worksite:chararray,longitude,latitude);

--describe H1B;

delheader = FILTER H1B by $0>1;

abc = FILTER delheader by $7 matches '2011';

ghi = GROUP abc by $2;

jkl = FOREACH ghi GENERATE group as empname ,abc.case_status as petition;

mno = FOREACH jkl GENERATE empname,COUNT($1);

pqr = ORDER mno by $1 DESC;

answer = LIMIT pqr 5;

dump answer;










