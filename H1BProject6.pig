H1B = LOAD 'hdfs://localhost:54310/user/hive/warehouse/ramit.db/h1b_final'  using  PigStorage('\t')  AS ( s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray , full_time_position:chararray , prevailing_wage:int , year:chararray , worksite:chararray,longitude,latitude);

--describe H1B;

delheader = FILTER H1B by $0>1;


status = FOREACH delheader GENERATE year,case_status;

grouped = GROUP status BY year;

totalApp = FOREACH grouped GENERATE group as year, COUNT(status) AS totalApplication;


caseStatusGroup = GROUP status BY (year,case_status);

--DESCRIBE caseStatusGroup;
--caseStatusGroup: {group: (year: chararray,case_status: chararray),status: {(year: chararray,case_status: chararray)}}


caseStatusTotal = FOREACH caseStatusGroup GENERATE group, group.year,COUNT(status) AS caseTotal;

joined = JOIN caseStatusTotal BY year, totalApp BY year;

percentCountCaseStatusYear = FOREACH joined GENERATE FLATTEN($0), (FLOAT)(caseStatusTotal::caseTotal*100)/(totalApp::totalApplication) AS percentage, caseStatusTotal::caseTotal AS caseStatusCount;

dump percentCountCaseStatusYear;
--store percentCountCaseStatusYear into '/home/hduser/Downloads/pig_result';
