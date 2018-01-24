--3)Which industry(SOC_NAME) has the most number of Data Scientist positions?[certified]

H1B = LOAD 'hdfs://localhost:54310/user/hive/warehouse/ramit.db/h1b_final'  using  PigStorage('\t')  AS ( s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray , full_time_position:chararray , prevailing_wage:int , year:chararray , worksite:chararray,longitude,latitude);

--describe H1B;

delheader = FILTER H1B by $0>1;

--describe delheader;

abc = FILTER delheader by ($1 matches '.*CERTIFIED.*');

def = FILTER abc by ($4 matches '.*DATA SCIENTIST.*');

ghi = GROUP def by $3;

jkl = FOREACH ghi GENERATE group as industry ,def.job_title as job;

mno = FOREACH jkl GENERATE industry,COUNT($1);

pqr = ORDER mno by $1 DESC;

answer = LIMIT pqr 1;

dump answer;

