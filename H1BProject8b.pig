--8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending --order - [Certified and Certified Withdrawn.]


H1B = LOAD 'hdfs://localhost:54310/user/hive/warehouse/ramit.db/h1b_final'  using  PigStorage('\t')  AS ( s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray , full_time_position:chararray , prevailing_wage:int , year:chararray , worksite:chararray,longitude,latitude);

--describe H1B;

delheader = FILTER H1B by $0>1;

abc = FILTER delheader by $7 matches '$whichyear';

def = FILTER abc by $5 matches 'N';

ram = FILTER def by $1 matches 'CERTIFIED';

ghi = GROUP ram by $4;

jkl = FOREACH ghi GENERATE group as job ,ram.prevailing_wage as wage;

mno = FOREACH jkl GENERATE job,AVG(wage);

pqr = ORDER mno by $1 DESC;


dump pqr;

