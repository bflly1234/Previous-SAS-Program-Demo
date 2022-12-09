proc import datafile="E:\DemoChurnRate.xlsx" out=DemoChurnRate DBMS=xlsx replace;
run; 

/*Reconcile the data first*/
proc means data=DemoChurnRate min max n nmiss;
            var EFFECTIVE_DATE TRANSACTION_DATE YR_MONTH;
            run;

proc freq data=DemoChurnRate; 
            table BUSINESS_SECTION TRANSACTION_TYPE PROVINCE; 
            run;
proc sql;
create table DemoChurnRate as 
select distinct YR_MONTH, TRANSACTION_TYPE, PROVINCE,
       count (distinct CUSTOMER_ID) as account_cnt
from  DemoChurnRate
where YEAR(TRANSACTION_DATE)=2020
group by YR_MONTH, TRANSACTION_TYPE, PROVINCE;
quit;

proc sql;
create table DemoChurnRate2 as
select YR_MONTH,
	   PROVINCE,
	   sum(case TRANSACTION_TYPE when 'ACTIVE' then account_cnt else 0 end) as ACTIVE,
	   sum(case TRANSACTION_TYPE when 'TERMINATED' then account_cnt else 0 end) as TERMINATED
from DemoChurnRate
group by YR_MONTH, PROVINCE
;
quit;

DATA DemoChurnRate2;
SET DemoChurnRate2;
CHURN_RATE=ROUND(TERMINATED/ACTIVE,.01);
FORMAT CHURN_RATE PERCENT7.2;
RUN ;

ODS tagsets.htmlpanel Path="E:\"
file = "FinancialStatus.html" STYLE=SASWEB;
proc report 
data=DemoChurnRate2 
nowindows
spacing=1
headline
headskip
nowd split="/";

column YR_MONTH PROVINCE ACTIVE TERMINATED CHURN_RATE;
define YR_MONTH/"Month" left ;
define PROVINCE/"Province" center ;
define ACTIVE/"Active/Account Count" center;
define TERMINATED/"Terminated/Account Count" right ;
define CHURN_RATE/"CHURN RATE" right format=percent11.2;
 title1 j=l font=arial height =10 pt "XXX Company Inc."
 		j=r "Page 1 of 1";
 title2 "Financial Status Overview";
 title3 "Churn Rate by Province";
 footnote1 "Program Name:DemoChurnRate.sas Rundate: &sysdate9..";
run;
ODS tagsets.htmlpanel CLOSE;

