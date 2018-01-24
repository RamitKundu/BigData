--1 b) Find top 5 job titles who are having highest avg growth in applications.[ALL]

H1B = LOAD 'hdfs://localhost:54310/user/hive/warehouse/ramit.db/h1b_final'  using  PigStorage('\t')  AS ( s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray , full_time_position:chararray , prevailing_wage:int , year:chararray , worksite:chararray,longitude,latitude);

--describe H1B;

delheader = FILTER H1B by $0>1;

ab = FILTER delheader by $7 matches '2011';

cd = GROUP ab BY $4;

--dump cd;

--describe cd;

s1 = FOREACH cd GENERATE group as job_t ,COUNT($1); 



gh = FILTER delheader by $7 matches '2012';

ij = GROUP gh BY $4;

s2 = FOREACH ij GENERATE group as job_t ,COUNT($1); 



mn = FILTER delheader by $7 matches '2013';

op = GROUP mn BY $4;

s3 = FOREACH op GENERATE group as job_t ,COUNT($1); 



qr = FILTER delheader by $7 matches '2014';

st = GROUP qr BY $4;

s4 = FOREACH st GENERATE group as job_t ,COUNT($1); 



st = FILTER delheader by $7 matches '2015';

uv = GROUP st BY $4;

s5 = FOREACH uv GENERATE group as job_t ,COUNT($1); 



xy = FILTER delheader by $7 matches '2016';

z = GROUP xy BY $4;

s6 = FOREACH z GENERATE group as job_t ,COUNT($1); 

joined = join s1 by $0,s2 by $0,s3 by $0, s4 by $0, s5 by $0,s6 by $0 ;

--dump s6;

xyz = FOREACH joined GENERATE $0,$1,$3,$5,$7,$9,$11;

--dump xyz;

growthpercent = FOREACH xyz GENERATE $0,(FLOAT)($6-$5)*100/$5,(FLOAT)($5-$4)*100/$4,(FLOAT)($4-$3)*100/$3,(FLOAT)($3-$2)*100/$2,(FLOAT)($2-$1)*100/$1;

avggrowth = FOREACH growthpercent GENERATE $0,($1+$2+$3+$4+$5)/5;

orderag = order avggrowth by $1 desc;

ans = LIMIT orderag 5;

dump ans;






