data _null_;
length characterofmonth $ 4.;
currentday = today();
currentmonth = month(today());
currentyear = year(today());
backtolastdayofmonth = today()-1;
backtolastmonth = Month(backtolastdayofmonth);
backtolastyear = Year(backtolastdayofmonth);
characterofdate = put(backtolastdayofmonth,Date9.);
characterofmonth = substr(characterofdate,3,3)||'%';

if currentmonth ^= backtolastmonth then do;
      currentmonth = backtolastmonth;
      end;
if currentyear ^= backtolastyear then do;
      currentyear = backtolastyear;
      end;
call symput("currentday", currentday);
call symput("currentmonth", currentmonth);
call symput("currentyear", currentyear);
call symput("characterofmonth", characterofmonth);
run;

proc sql;
      connect to teradata as teradata_tables
      (user=&username password=&password server="" tpt=yes fastexport=yes);
      create table fatchedData as
      select * from connection to teradata_tables
            (select BELL_DIVISION
                        ,CASE
                        WHEN STATUS_GENERAL_DESC = 'Completed' THEN 'CHURN'
                        WHEN STATUS_GENERAL_DESC = 'Issued' THEN 'PENDING'
                        END AS ORDER_STATUS
                        ,CASE
                        WHEN KPI_LOB = 'TV' AND KPI_PORTFOLIO = 'FIBE' THEN 'FIBE TV'
                        WHEN KPI_LOB = 'TV' AND KPI_PORTFOLIO = 'SAT TV' THEN 'SAT TV'
                        WHEN KPI_LOB = 'VOICE' THEN 'VOICE'
                        WHEN KPI_LOB = 'INTERNET' THEN 'INTERNET'
                        WHEN KPI_LOB = 'BSA' THEN 'BSA'
                        END AS LOB
                        ,ORDER_TYPE_LEVEL_3 AS DEACT_TYPE
                        ,CASE WHEN ORDER_TYPE_LEVEL_4 = 'Port Out' THEN 1 ELSE 0 END AS PORT_OUT_FLAG
                        ,COMPLETE_Month_Of_Year
                        ,COMPLETE_Year_YYYY
                        ,ISSUED_Month_Of_Year
                        ,ISSUED_Year_YYYY
                        ,ISSUED_FULL_DATE
                        ,CASE
                        WHEN STATUS_GENERAL_DESC = 'Issued' THEN (CASE WHEN DUE_FULL_DATE < ISSUED_FULL_DATE THEN ISSUED_FULL_DATE ELSE DUE_FULL_DATE END)
                        WHEN STATUS_GENERAL_DESC = 'Completed' THEN (CASE WHEN COMPLETE_FULL_DATE < ISSUED_FULL_DATE THEN ISSUED_FULL_DATE ELSE COMPLETE_FULL_DATE END)
                        END AS FUTURE_CANCEL_DT
                        ,MONTH(FUTURE_CANCEL_DT) AS FUTURE_CANCEL_MONTH
                        ,YEAR(FUTURE_CANCEL_DT) AS FUTURE_CANCEL_YEAR
                        ,CASE
                        WHEN ORDER_STATUS = 'PENDING' AND FUTURE_CANCEL_DT = ISSUED_FULL_DATE THEN 'SAMEDAY'
                        WHEN ORDER_STATUS = 'PENDING' AND FUTURE_CANCEL_DT > ISSUED_FULL_DATE AND FUTURE_CANCEL_MONTH = ISSUED_Month_Of_Year AND FUTURE_CANCEL_YEAR = ISSUED_Year_YYYY THEN 'IN_MONTH'
                        WHEN ORDER_STATUS = 'PENDING' AND FUTURE_CANCEL_DT > ISSUED_FULL_DATE AND (FUTURE_CANCEL_MONTH <> ISSUED_Month_Of_Year OR FUTURE_CANCEL_YEAR <> ISSUED_Year_YYYY) THEN 'FUTURE_MONTH'
                        WHEN ORDER_STATUS = 'CHURN' AND COMPLETE_FULL_DATE = ISSUED_FULL_DATE THEN 'SAMEDAY'
                        WHEN ORDER_STATUS = 'CHURN' AND COMPLETE_FULL_DATE > ISSUED_FULL_DATE AND COMPLETE_Month_Of_Year = ISSUED_Month_Of_Year AND COMPLETE_Year_YYYY = ISSUED_Year_YYYY THEN 'IN_MONTH'
                        WHEN ORDER_STATUS = 'CHURN' AND COMPLETE_FULL_DATE > ISSUED_FULL_DATE AND (COMPLETE_Month_Of_Year <> ISSUED_Month_Of_Year OR COMPLETE_Year_YYYY <> ISSUED_Year_YYYY) THEN 'FUTURE_MONTH'
                        ELSE 'SAMEDAY'
                        END AS TIME_TYPE  
                        ,(ABS(SUM(QUANTITY_RGU))) AS NATIONAL
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BELL' AND PROVINCE_CODE IS NOT NULL THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BELL' AND PROVINCE_CODE = 'ON' THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_ONTARIO
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BELL' AND PROVINCE_CODE = 'QC' THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_QUEBEC
                        ,(ABS(SUM(CASE WHEN PROVINCE_CODE = 'ON' AND BELL_DIVISION IN ('BELL', 'BNQ') THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_BNQ_ONTARIO
                        ,(ABS(SUM(CASE WHEN PROVINCE_CODE = 'QC' AND BELL_DIVISION IN ('BELL', 'BNQ') THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_BNQ_QUEBEC
                        ,(ABS(SUM(CASE WHEN PROVINCE_CODE IS NOT NULL AND BELL_DIVISION IN ('BELL', 'BNQ') THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_BNQ
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'ALIANT' THEN QUANTITY_RGU ELSE 0 END))) AS ATLANTIC
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BNQ' THEN QUANTITY_RGU ELSE 0 END))) AS BNQ
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BNQ' AND PROVINCE_CODE = 'ON' THEN QUANTITY_RGU ELSE 0 END))) AS BNQ_ONTARIO
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BNQ' AND PROVINCE_CODE = 'QC' THEN QUANTITY_RGU ELSE 0 END))) AS BNQ_QUEBEC
                        ,0 AS MTS
            FROM  
            WHERE ORDER_TYPE_LEVEL_2 = 'Churn' AND STATUS_GENERAL_DESC IN ('Completed', 'Issued')AND BUSINESS_UNIT = 'BRSB' AND QUANTITY_RGU <> 0
            group by BELL_DIVISION, ORDER_STATUS, LOB, DEACT_TYPE, PORT_OUT_FLAG, COMPLETE_Month_Of_Year, COMPLETE_Year_YYYY, ISSUED_Month_Of_Year, ISSUED_Year_YYYY, ISSUED_FULL_DATE, FUTURE_CANCEL_DT, TIME_TYPE

            union all

            SELECT      BELL_DIVISION
                        ,'PENDING' AS ORDER_STATUS
                        ,CASE
                WHEN KPI_LOB = 'TV' AND KPI_PORTFOLIO = 'FIBE' THEN 'FIBE TV'
                        WHEN KPI_LOB = 'TV' AND KPI_PORTFOLIO = 'SAT TV' THEN 'SAT TV'
                WHEN KPI_LOB = 'VOICE' THEN 'VOICE'
                WHEN KPI_LOB = 'INTERNET' THEN 'INTERNET'
                WHEN KPI_LOB = 'BSA' THEN 'BSA'
                ELSE 'OTHER'
                        END AS LOB
                        ,ORDER_TYPE_LEVEL_3 AS DEACT_TYPE
                        ,CASE WHEN ORDER_TYPE_LEVEL_4 = 'Port Out' THEN 1 ELSE 0 END AS PORT_OUT_FLAG
                        ,COMPLETE_Month_Of_Year
                        ,COMPLETE_Year_YYYY
                        ,ISSUED_Month_Of_Year
                        ,ISSUED_Year_YYYY
                        ,ISSUED_FULL_DATE
                        ,CASE WHEN COMPLETE_FULL_DATE < ISSUED_FULL_DATE THEN ISSUED_FULL_DATE ELSE COMPLETE_FULL_DATE END AS FUTURE_CANCEL_DT
                        ,MONTH(FUTURE_CANCEL_DT) AS FUTURE_CANCEL_MONTH
                        ,YEAR(FUTURE_CANCEL_DT) AS FUTURE_CANCEL_YEAR
                        ,CASE
                WHEN COMPLETE_FULL_DATE = ISSUED_FULL_DATE THEN 'SAMEDAY'
                        WHEN COMPLETE_FULL_DATE > ISSUED_FULL_DATE AND COMPLETE_Month_Of_Year = ISSUED_Month_Of_Year AND COMPLETE_Year_YYYY = ISSUED_Year_YYYY THEN 'IN_MONTH'
                WHEN COMPLETE_FULL_DATE > ISSUED_FULL_DATE AND (COMPLETE_Month_Of_Year <> ISSUED_Month_Of_Year OR COMPLETE_Year_YYYY <> ISSUED_Year_YYYY) THEN 'FUTURE_MONTH'
                ELSE 'SAMEDAY'
                END AS TIME_TYPE
                        ,(ABS(SUM(QUANTITY_RGU))) AS NATIONAL
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BELL' AND PROVINCE_CODE IS NOT NULL THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BELL' AND PROVINCE_CODE = 'ON' THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_ONTARIO
                ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BELL' AND PROVINCE_CODE = 'QC' THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_QUEBEC
                ,(ABS(SUM(CASE WHEN PROVINCE_CODE = 'ON' AND BELL_DIVISION IN ('BELL', 'BNQ') THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_BNQ_ONTARIO
                ,(ABS(SUM(CASE WHEN PROVINCE_CODE = 'QC' AND BELL_DIVISION IN ('BELL', 'BNQ') THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_BNQ_QUEBEC
                ,(ABS(SUM(CASE WHEN PROVINCE_CODE IS NOT NULL AND BELL_DIVISION IN ('BELL', 'BNQ') THEN QUANTITY_RGU ELSE 0 END))) AS CENTRAL_BNQ
                ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'ALIANT' THEN QUANTITY_RGU ELSE 0 END))) AS ATLANTIC
                ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BNQ' THEN QUANTITY_RGU ELSE 0 END))) AS BNQ
                ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BNQ' AND PROVINCE_CODE = 'ON' THEN QUANTITY_RGU ELSE 0 END))) AS BNQ_ONTARIO
                ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'BNQ' AND PROVINCE_CODE = 'QC' THEN QUANTITY_RGU ELSE 0 END))) AS BNQ_QUEBEC
                ,0 AS MTS
            FROM  
            WHERE ORDER_TYPE_LEVEL_2 = 'Churn' AND STATUS_GENERAL_DESC IN ('Completed') AND BUSINESS_UNIT = 'BRSB' AND QUANTITY_RGU <> 0
            group by BELL_DIVISION, ORDER_STATUS, LOB, DEACT_TYPE, PORT_OUT_FLAG, COMPLETE_Month_Of_Year, COMPLETE_Year_YYYY, ISSUED_Month_Of_Year, ISSUED_Year_YYYY, ISSUED_FULL_DATE, FUTURE_CANCEL_DT, TIME_TYPE

            union all

            SELECT      BELL_DIVISION
                        ,CASE
                        WHEN STATUS_GENERAL_DESC = 'Completed' THEN 'CHURN'
                        WHEN STATUS_GENERAL_DESC = 'Issued' THEN 'PENDING'
                        END AS ORDER_STATUS
                        ,CASE
                        WHEN KPI_LOB = 'TV' THEN 'TV'
                        WHEN KPI_LOB = 'VOICE' THEN 'VOICE'
                        WHEN KPI_LOB = 'INTERNET' THEN 'INTERNET'
                        ELSE 'OTHER'
                        END AS LOB
                        ,ORDER_TYPE_LEVEL_3 AS DEACT_TYPE
                        ,0 AS PORT_OUT_FLAG
                        ,COMPLETE_Month_Of_Year
                        ,COMPLETE_Year_YYYY
                        ,ISSUED_Month_Of_Year
                        ,ISSUED_Year_YYYY
                        ,ISSUED_FULL_DATE
                        ,CASE
                        WHEN STATUS_GENERAL_DESC = 'Issued' THEN (CASE WHEN DUE_FULL_DATE < ISSUED_FULL_DATE THEN ISSUED_FULL_DATE ELSE DUE_FULL_DATE END)
                        WHEN STATUS_GENERAL_DESC = 'Completed' THEN (CASE WHEN COMPLETE_FULL_DATE < ISSUED_FULL_DATE THEN ISSUED_FULL_DATE ELSE COMPLETE_FULL_DATE END)
                        END AS FUTURE_CANCEL_DT
                        ,MONTH(FUTURE_CANCEL_DT) AS FUTURE_CANCEL_MONTH
                        ,YEAR(FUTURE_CANCEL_DT) AS FUTURE_CANCEL_YEAR
                        ,CASE
                        WHEN STATUS_GENERAL_DESC = 'Issued' AND FUTURE_CANCEL_DT = ISSUED_FULL_DATE THEN 'SAMEDAY'
                        WHEN STATUS_GENERAL_DESC = 'Issued' AND FUTURE_CANCEL_DT > ISSUED_FULL_DATE AND FUTURE_CANCEL_MONTH = ISSUED_Month_Of_Year AND FUTURE_CANCEL_YEAR = ISSUED_Year_YYYY THEN 'IN_MONTH'
                        WHEN STATUS_GENERAL_DESC = 'Issued' AND FUTURE_CANCEL_DT > ISSUED_FULL_DATE AND (FUTURE_CANCEL_MONTH <> ISSUED_Month_Of_Year OR FUTURE_CANCEL_YEAR <> ISSUED_Year_YYYY) THEN 'FUTURE_MONTH'
                        WHEN STATUS_GENERAL_DESC = 'Completed' AND COMPLETE_FULL_DATE = ISSUED_FULL_DATE THEN 'SAMEDAY'
                        WHEN STATUS_GENERAL_DESC = 'Completed' AND COMPLETE_FULL_DATE > ISSUED_FULL_DATE AND COMPLETE_Month_Of_Year = ISSUED_Month_Of_Year AND COMPLETE_Year_YYYY = ISSUED_Year_YYYY THEN 'IN_MONTH'
                        WHEN STATUS_GENERAL_DESC = 'Completed' AND COMPLETE_FULL_DATE > ISSUED_FULL_DATE AND (COMPLETE_Month_Of_Year <> ISSUED_Month_Of_Year OR COMPLETE_Year_YYYY <> ISSUED_Year_YYYY) THEN 'FUTURE_MONTH'
                        ELSE 'SAMEDAY'    
                        END AS TIME_TYPE
                        ,0 AS NATIONAL    
                        ,0 AS CENTRAL
                        ,0 AS CENTRAL_ONTARIO
                        ,0 AS CENTRAL_QUEBEC
                        ,0 AS CENTRAL_BNQ_ONTARIO
                        ,0 AS CENTRAL_BNQ_QUEBEC
                        ,0 AS CENTRAL_BNQ
                        ,0 AS ATLANTIC
                        ,0 AS BNQ
                        ,0 AS BNQ_ONTARIO
                        ,0 AS BNQ_QUEBEC
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'MTS' THEN QUANTITY_RGU ELSE 0 END))) AS MTS
            FROM  
            WHERE ORDER_TYPE_LEVEL_2 = 'Churn' AND STATUS_GENERAL_DESC IN ('Completed', 'Issued') AND QUANTITY_RGU <> 0 AND Channel_level_5 NOT IN ('MTS WLN - BUSINESS CHANNEL SUPPORT')
            group by BELL_DIVISION, ORDER_STATUS, LOB, DEACT_TYPE, PORT_OUT_FLAG, COMPLETE_Month_Of_Year, COMPLETE_Year_YYYY, ISSUED_Month_Of_Year, ISSUED_Year_YYYY, ISSUED_FULL_DATE, FUTURE_CANCEL_DT, TIME_TYPE

            union all

            select      BELL_DIVISION
                        ,'PENDING' AS ORDER_STATUS
                        ,CASE
                        WHEN KPI_LOB = 'TV' THEN 'TV'
                        WHEN KPI_LOB = 'VOICE' THEN 'VOICE'
                        WHEN KPI_LOB = 'INTERNET' THEN 'INTERNET'
                        ELSE 'OTHER'
                        END AS LOB
                        ,ORDER_TYPE_LEVEL_3 AS DEACT_TYPE
                        ,0 AS PORT_OUT_FLAG
                        ,COMPLETE_Month_Of_Year
                        ,COMPLETE_Year_YYYY
                        ,ISSUED_Month_Of_Year
                        ,ISSUED_Year_YYYY
                        ,ISSUED_FULL_DATE
                        ,CASE
                        WHEN STATUS_GENERAL_DESC = 'Issued' THEN (CASE WHEN DUE_FULL_DATE < ISSUED_FULL_DATE THEN ISSUED_FULL_DATE ELSE DUE_FULL_DATE END)
                        WHEN STATUS_GENERAL_DESC = 'Completed' THEN (CASE WHEN COMPLETE_FULL_DATE < ISSUED_FULL_DATE THEN ISSUED_FULL_DATE ELSE COMPLETE_FULL_DATE END)
                        END AS FUTURE_CANCEL_DT
                        ,MONTH(FUTURE_CANCEL_DT) AS FUTURE_CANCEL_MONTH
                        ,YEAR(FUTURE_CANCEL_DT) AS FUTURE_CANCEL_YEAR
                        ,CASE
                        WHEN STATUS_GENERAL_DESC = 'Issued' AND FUTURE_CANCEL_DT = ISSUED_FULL_DATE THEN 'SAMEDAY'
                        WHEN STATUS_GENERAL_DESC = 'Issued' AND FUTURE_CANCEL_DT > ISSUED_FULL_DATE AND FUTURE_CANCEL_MONTH = ISSUED_Month_Of_Year AND FUTURE_CANCEL_YEAR = ISSUED_Year_YYYY THEN 'IN_MONTH'
                        WHEN STATUS_GENERAL_DESC = 'Issued' AND FUTURE_CANCEL_DT > ISSUED_FULL_DATE AND (FUTURE_CANCEL_MONTH <> ISSUED_Month_Of_Year OR FUTURE_CANCEL_YEAR <> ISSUED_Year_YYYY) THEN 'FUTURE_MONTH'
                        WHEN STATUS_GENERAL_DESC = 'Completed' AND COMPLETE_FULL_DATE = ISSUED_FULL_DATE THEN 'SAMEDAY'
                        WHEN STATUS_GENERAL_DESC = 'Completed' AND COMPLETE_FULL_DATE > ISSUED_FULL_DATE AND COMPLETE_Month_Of_Year = ISSUED_Month_Of_Year AND COMPLETE_Year_YYYY = ISSUED_Year_YYYY THEN 'IN_MONTH'
                        WHEN STATUS_GENERAL_DESC = 'Completed' AND COMPLETE_FULL_DATE > ISSUED_FULL_DATE AND (COMPLETE_Month_Of_Year <> ISSUED_Month_Of_Year OR COMPLETE_Year_YYYY <> ISSUED_Year_YYYY) THEN 'FUTURE_MONTH'
                        ELSE 'SAMEDAY'
                        END AS TIME_TYPE
                        ,0 AS NATIONAL
                        ,0 AS CENTRAL
                        ,0 AS CENTRAL_ONTARIO
                        ,0 AS CENTRAL_QUEBEC
                        ,0 AS CENTRAL_BNQ_ONTARIO
                        ,0 AS CENTRAL_BNQ_QUEBEC
                        ,0 AS CENTRAL_BNQ
                        ,0 AS ATLANTIC
                        ,0 AS BNQ
                        ,0 AS BNQ_ONTARIO
                        ,0 AS BNQ_QUEBEC
                        ,(ABS(SUM(CASE WHEN BELL_DIVISION = 'MTS' THEN QUANTITY_RGU ELSE 0 END))) AS MTS
            FROM  
            WHERE ORDER_TYPE_LEVEL_2 = 'Churn' AND STATUS_GENERAL_DESC IN ('Completed', 'Issued') AND QUANTITY_RGU <> 0 AND Channel_level_5 NOT IN ('MTS WLN - BUSINESS CHANNEL SUPPORT')
            group by BELL_DIVISION, ORDER_STATUS, LOB, DEACT_TYPE, PORT_OUT_FLAG, COMPLETE_Month_Of_Year, COMPLETE_Year_YYYY, ISSUED_Month_Of_Year, ISSUED_Year_YYYY, ISSUED_FULL_DATE, FUTURE_CANCEL_DT, TIME_TYPE;);
      disconnect from teradata_tables;
quit;

proc sort data=fatchedData;
by FUTURE_CANCEL_DT;
run;
 
/*Deact Actuals*/
proc sql;
create table PivotMTDOrder as
select LOB, PORT_OUT_FLAG, FUTURE_CANCEL_DT, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, TIME_TYPE, CENTRAL_BNQ_ONTARIO, CENTRAL_BNQ_QUEBEC, ATLANTIC, MTS from fatchedData
where ORDER_STATUS in ('CHURN') and DEACT_TYPE in ('Voluntary Churn')
order by FUTURE_CANCEL_DT, TIME_TYPE;
quit;

proc sql;
create table TOTALSUM as
select FUTURE_CANCEL_DT, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, TIME_TYPE, sum(CENTRAL_BNQ_ONTARIO) as ONBNQ, sum(CENTRAL_BNQ_QUEBEC) as QCBNQ, sum(ATLANTIC) as ATLANTIC, sum(MTS) as MTS from PivotMTDOrder
group by FUTURE_CANCEL_DT, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, TIME_TYPE;
quit;

proc transpose data=TOTALSUM out=TOTALSUM;
by FUTURE_CANCEL_DT FUTURE_CANCEL_MONTH FUTURE_CANCEL_YEAR;
id TIME_TYPE;
run;

data TOTALSUM;
set TOTALSUM;
array change _numeric_;
      do over change;
      if change='.' then change=0;
      end;
Actuals= FUTURE_MONTH + SAMEDAY + IN_MONTH;
run;

proc sql;
create table TOTALs as
select FUTURE_CANCEL_DT as Date, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, Actuals, _NAME_ as NAME, FUTURE_MONTH as Prior_Month, IN_MONTH as In_Month, SAMEDAY as Sameday from TOTALSUM;
quit;

proc sql;
create table National as
select Date, sum(Actuals) as Actuals, sum(Prior_Month) as Prior_Month , sum(In_Month) as In_Month, sum(Sameday) as Sameday from TOTALS
group by Date;
quit;

proc sql;
create table ONBNQ as
select DATE, Actuals, Prior_Month, In_Month, Sameday from TOTALs
where NAME in ('ONBNQ');
quit;

proc sql;
create table QCBNQ as
select DATE, Actuals, Prior_Month, In_Month, Sameday from TOTALs
where NAME in ('QCBNQ');
quit;

proc sql;
create table ATLANTIC as
select DATE, Actuals, Prior_Month, In_Month, Sameday from TOTALs
where NAME in ('ATLANTIC');
quit;

proc sql;
create table MTS as
select DATE, Actuals, Prior_Month, In_Month, Sameday from TOTALS
where NAME in ('MTS');
quit;

/*Deact Plan:Plan*/
proc sql;
      connect to teradata as teradata_tables
      (user=&username password=&password server="" tpt=yes fastexport=yes);
      create table fatchedData2 as
      select * from connection to teradata_tables
            (select     ORDER_TYPE_LEVEL_2,     
                        ORDER_TYPE_LEVEL_3,
                        KPI_LOB,
                        KPI_LEVEL_2,
                        PROVINCE_CODE,
                        FULL_DATE,
                        MONTH_OF_YEAR,
                        YEAR_YYYY,
                        abs(sum(QUANTITY_RGU)) as QUANTITY_RGU ,
                        LEDGER
            from 
            where ORDER_TYPE_LEVEL_2 in ('Churn')
                        and ORDER_TYPE_LEVEL_3 in ('Voluntary Churn')
                        and STATUS_GENERAL_DESC in('Completed')
                        and MONTH_OF_YEAR = &currentmonth
                        and YEAR_YYYY = &currentyear
/*                      and LEDGER like ('FEB%')*/
/*                      and LEDGER like '&characterofmonth'*/
            group by ORDER_TYPE_LEVEL_2,  
                        ORDER_TYPE_LEVEL_3,
                        KPI_LOB,
                        KPI_LEVEL_2,
                        PROVINCE_CODE,
                        FULL_DATE,
                        MONTH_OF_YEAR,
                        YEAR_YYYY,
                        QUANTITY_RGU,
                        LEDGER
            order by FULL_DATE;);
      disconnect from teradata_tables;
quit;

data fatchedData2;
set fatchedData2;;
where LEDGER like "&characterofmonth";
run;

proc sql;
create table nationalplan as
select FULL_DATE as Date, sum(QUANTITY_RGU) as Plan
from fatchedData2
where LEDGER not like ('%OL W')
group by FULL_DATE;
quit;

proc sql;
create table ontarioplan as
select FULL_DATE as Date, sum(QUANTITY_RGU) as Plan
from fatchedData2
where LEDGER not like ('%OL W') and PROVINCE_CODE in ('ON')
group by FULL_DATE;
quit;

proc sql;
create table qcplan as
select FULL_DATE as Date, sum(QUANTITY_RGU) as Plan
from fatchedData2
where LEDGER not like ('%OL W') and PROVINCE_CODE in ('QC')
group by FULL_DATE;
quit;

proc sql;
create table atlanticplan as
select FULL_DATE as Date, sum(QUANTITY_RGU) as Plan
from fatchedData2
where LEDGER not like ('%OL W') and PROVINCE_CODE in ('NS')
group by FULL_DATE;
quit;

proc sql;
create table mtsplan as
select FULL_DATE as Date, sum(QUANTITY_RGU) as Plan
from fatchedData2
where LEDGER not like ('%OL W') and PROVINCE_CODE in ('MB')
group by FULL_DATE;
quit;

proc sql;
create table nationalmee as
select sum(plan) as mee from nationalplan;
quit;

data _null_;
      set nationalmee;
call symput("nationalmee", compress(round(mee,1)));
run;

proc sql;
create table ontariomee as
select sum(plan) as mee from ontarioplan;
quit;

data _null_;
      set ontariomee;
call symput("ontariomee", compress(round(mee,1)));
run;

proc sql;
create table quebecmee as
select sum(plan) as mee from qcplan;
quit;

data _null_;
      set quebecmee;
call symput("quebecmee", compress(round(mee,1)));
run;

proc sql;
create table atlanticmee as
select sum(plan) as mee from atlanticplan;
quit;

data _null_;
      set atlanticmee;
call symput("atlanticmee", compress(round(mee,1)));
run;

proc sql;
create table mtsmee as
select sum(plan) as mee from mtsplan;
quit;

data _null_;
      set mtsmee;
call symput("mtsmee", compress(round(mee,1)));
run;


/*Deact YoY: Direct user table of: Totals, because it already includes all previous period data I need*/
      /*Total*/
proc sql;
create table YoY as
select Date, (Date-364) as WeekDay_Compared format date9., (Date-365) as Day_of_the_Mth_Compared format date9., FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, NAME, Sum(Actuals) as Actuals from totals
where FUTURE_CANCEL_YEAR  between (&currentyear-1) and &currentyear
group by Date, WeekDay_Compared, Day_of_the_Mth_Compared, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, NAME;
quit;

      /*National*/
proc sql ;
create table nationaltotal as
select Date, WeekDay_Compared, Day_of_the_Mth_Compared, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, sum (Actuals) as Actuals
from yoy
group by Date, WeekDay_Compared, Day_of_the_Mth_Compared, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR;
quit;
 
proc sql;
create table nationalyoy as
select Date, WeekDay_Compared, Day_of_the_Mth_Compared, Actuals
from nationaltotal
where FUTURE_CANCEL_MONTH=&currentmonth and FUTURE_CANCEL_YEAR=&currentyear;
quit;

proc sql;
create table nationalyoyw as
select nationalyoy.*, nationaltotal.Actuals as PActuals
from nationalyoy left join nationaltotal
on nationalyoy.WeekDay_Compared = nationaltotal.Date;
quit;

proc sql;
create table nationalyoydm as
select nationalyoyw.* , nationaltotal.Actuals as YPActuals
from nationalyoyw left join nationaltotal
on nationalyoyw.Day_of_the_Mth_Compared = nationaltotal.Date;
quit;

data nationalyoy (Drop=WeekDay_Compared Day_of_the_Mth_Compared Actuals PActuals YPActuals);
merge nationalyoyw
        nationalyoydm;
by Date;
YOY_WeekDay_Compared = Actuals - PActuals;
YOY_Day_of_the_Mth_Compared = Actuals - YPActuals;
run;

      /*Ontario*/
proc sql;
create table ontarioyoy as
select * from YoY
where FUTURE_CANCEL_MONTH=&currentmonth and FUTURE_CANCEL_YEAR=&currentyear and NAME in ('ONBNQ');

proc sql;
create table ontarioyoyw as
select ontarioyoy.* , YoY.Actuals as PActuals
from ontarioyoy left join YoY
on ontarioyoy.WeekDay_Compared = YoY.Date
where YoY.NAME in ('ONBNQ');
quit;

proc sql;
create table ontarioyoydm as
select ontarioyoyw.* , YoY.Actuals as YPActuals
from ontarioyoyw left join YoY
on ontarioyoyw.Day_of_the_Mth_Compared = YoY.Date
where YoY.NAME in ('ONBNQ');
quit;

data ontarioyoy (Drop=WeekDay_Compared Day_of_the_Mth_Compared FUTURE_CANCEL_MONTH FUTURE_CANCEL_YEAR NAME Actuals PActuals YPActuals);
merge ontarioyoyw
        ontarioyoydm;
by Date;
YOY_WeekDay_Compared = Actuals - PActuals;
YOY_Day_of_the_Mth_Compared = Actuals - YPActuals;
run;
      
      /*QC*/
proc sql;
create table qcyoy as
select * from YoY
where FUTURE_CANCEL_MONTH=&currentmonth and FUTURE_CANCEL_YEAR=&currentyear and NAME in ('QCBNQ');

proc sql;
create table qcyoyw as
select qcyoy.* , YoY.Actuals as PActuals
from qcyoy left join YoY
on qcyoy.WeekDay_Compared = YoY.Date
where YoY.NAME in ('QCBNQ');
quit;

proc sql;
create table qcyoydm as
select qcyoyw.* , YoY.Actuals as YPActuals
from qcyoyw left join YoY
on qcyoyw.Day_of_the_Mth_Compared = YoY.Date
where YoY.NAME in ('QCBNQ');
quit;

data qcyoy (Drop=WeekDay_Compared Day_of_the_Mth_Compared FUTURE_CANCEL_MONTH FUTURE_CANCEL_YEAR NAME Actuals PActuals YPActuals);
merge qcyoyw
        qcyoydm;
by Date;
YOY_WeekDay_Compared = Actuals - PActuals;
YOY_Day_of_the_Mth_Compared = Actuals - YPActuals;
run;

            /*ATLANTIC*/
proc sql;
create table atyoy as
select * from YoY
where FUTURE_CANCEL_MONTH=&currentmonth and FUTURE_CANCEL_YEAR=&currentyear and NAME in ('ATLANTIC');

proc sql;
create table atyoyw as
select atyoy.* , YoY.Actuals as PActuals
from atyoy left join YoY
on atyoy.WeekDay_Compared = YoY.Date
where YoY.NAME in ('ATLANTIC');
quit;

proc sql;
create table atyoydm as
select atyoyw.* , YoY.Actuals as YPActuals
from atyoyw left join YoY
on atyoyw.Day_of_the_Mth_Compared = YoY.Date
where YoY.NAME in ('ATLANTIC');
quit;

data atyoy (Drop=WeekDay_Compared Day_of_the_Mth_Compared FUTURE_CANCEL_MONTH FUTURE_CANCEL_YEAR NAME Actuals PActuals YPActuals);
merge atyoyw
        atyoydm;
by Date;
YOY_WeekDay_Compared = Actuals - PActuals;
YOY_Day_of_the_Mth_Compared = Actuals - YPActuals;
run;

      /*MTS*/
proc sql;
create table mtsyoy as
select * from YoY
where FUTURE_CANCEL_MONTH=&currentmonth and FUTURE_CANCEL_YEAR=&currentyear and NAME in ('MTS');

proc sql;
create table mtsyoyw as
select mtsyoy.* , YoY.Actuals as PActuals
from mtsyoy left join YoY
on mtsyoy.WeekDay_Compared = YoY.Date
where YoY.NAME in ('MTS');
quit;

proc sql;
create table mtsyoydm as
select mtsyoyw.* , YoY.Actuals as YPActuals
from mtsyoyw left join YoY
on mtsyoyw.Day_of_the_Mth_Compared = YoY.Date
where YoY.NAME in ('MTS');
quit;

data mtsyoy (Drop=WeekDay_Compared Day_of_the_Mth_Compared FUTURE_CANCEL_MONTH FUTURE_CANCEL_YEAR NAME Actuals PActuals YPActuals);
merge mtsyoyw
        mtsyoydm;
by Date;
YOY_WeekDay_Compared = Actuals - PActuals;
YOY_Day_of_the_Mth_Compared = Actuals - YPActuals;
run;


/*FSUM Actuals*/

proc sql;
create table PivotMTDOrderFSUM as
select * from fatchedData
where ORDER_STATUS in ('PENDING') and DEACT_TYPE in ('Voluntary Churn')
order by Issued_Full_Date, TIME_TYPE;
quit;

proc sql;
create table FSUMFM as
select Issued_Full_Date as Date, Issued_Month_Of_Year, Issued_Year_YYYY, sum(CENTRAL_BNQ_ONTARIO) as ONTARIO_FUTURE_MONTH, sum(CENTRAL_BNQ_QUEBEC) as QUEBEC_FUTURE_MONTH, sum(ATLANTIC) as ATLANTIC_FUTURE_MONTH, sum(MTS) as MTS_FUTURE_MONTH, sum(CENTRAL_BNQ_ONTARIO + CENTRAL_BNQ_QUEBEC + ATLANTIC + MTS) as TotalFM from PivotMTDOrderFSUM
where PORT_OUT_FLAG=0 and TIME_TYPE in ('FUTURE_MONTH') and LOB in ('INTERNET', 'FIBE TV', 'SAT TV', 'VOICE')
group by Date, Issued_Month_Of_Year, Issued_Year_YYYY
order by Date;
quit;

proc sql;
create table FSUMIM as
 select Date format date9., Issued_Month_Of_Year, Issued_Year_YYYY, sum(CENTRAL_BNQ_ONTARIO) as ONTARIO_In_Month, sum(CENTRAL_BNQ_QUEBEC) as QUEBEC_In_Month, sum(ATLANTIC) as ATLANTIC_In_Month, sum(MTS) as MTS_In_Month, sum(CENTRAL_BNQ_ONTARIO + CENTRAL_BNQ_QUEBEC + ATLANTIC + MTS) as TotalIN from (
      select Issued_Full_Date as Date, Issued_Month_Of_Year, Issued_Year_YYYY, CENTRAL_BNQ_ONTARIO, CENTRAL_BNQ_QUEBEC, ATLANTIC, MTS from PivotMTDOrderFSUM
      where PORT_OUT_FLAG=0 and TIME_TYPE in ('IN_MONTH') and LOB in ('INTERNET', 'FIBE TV', 'SAT TV', 'VOICE')
      union all
      select Issued_Full_Date as Date, Issued_Month_Of_Year, Issued_Year_YYYY, CENTRAL_BNQ_ONTARIO, CENTRAL_BNQ_QUEBEC, ATLANTIC, MTS from PivotMTDOrderFSUM
      where PORT_OUT_FLAG=1 and TIME_TYPE in ('IN_MONTH') and LOB in ('VOICE')
      )
 group by Date, Issued_Month_Of_Year, Issued_Year_YYYY
 order by Date;
quit;

proc sql;
create table FSUMSD as
 select Date format date9., Issued_Month_Of_Year, Issued_Year_YYYY, sum(CENTRAL_BNQ_ONTARIO) as ONTARIO_Sameday, sum(CENTRAL_BNQ_QUEBEC) as QUEBEC_Sameday, sum(ATLANTIC) as ATLANTIC_Sameday, sum(MTS) as MTS_Sameday, sum(CENTRAL_BNQ_ONTARIO + CENTRAL_BNQ_QUEBEC + ATLANTIC + MTS) as TotalSD from (
      select Issued_Full_Date as Date, Issued_Month_Of_Year, Issued_Year_YYYY, CENTRAL_BNQ_ONTARIO, CENTRAL_BNQ_QUEBEC, ATLANTIC, MTS from PivotMTDOrderFSUM
      where PORT_OUT_FLAG=0 and TIME_TYPE in ('SAMEDAY') and LOB in ('INTERNET', 'FIBE TV', 'SAT TV', 'VOICE', 'BSA')
      union all
      select Issued_Full_Date as Date, Issued_Month_Of_Year, Issued_Year_YYYY, CENTRAL_BNQ_ONTARIO, CENTRAL_BNQ_QUEBEC, ATLANTIC, MTS from PivotMTDOrderFSUM
      where PORT_OUT_FLAG=1 and TIME_TYPE in ('SAMEDAY') and LOB in ('VOICE')
      )
 group by Date, Issued_Month_Of_Year, Issued_Year_YYYY
 order by Date;
quit;

data wholeFSUMtable;
merge FSUMFM (in=in_FSUMFM)
            FSUMIM (in=in_FSUMIM)
            FSUMSD (in=in_FSUMIM);
by Date;

array missing _numeric_;
      do over missing;
            if missing=. then missing=0;
            end;
run;

proc sql;
create table NationalFSUM as
select Date, Issued_Month_Of_Year, Issued_Year_YYYY, (Date-364) as FSUM_WeekDay_Compared format date9., (Date-365) as FSUM_Day_of_the_Mth_Compared format date9., TotalFM, TotalIN, TotalSD, sum(TotalFM+TotalIN+TotalSD) as NationalTotal from wholeFSUMtable
group by Date;
quit;

proc sql;
create table NationalFSUMYOY as
select *
from NationalFSUM
where Issued_Month_Of_Year=&currentmonth and Issued_Year_YYYY=&currentyear;
quit;

proc sql;
create table NationalFSUMYOYWD as
select *, NationalFSUM.NationalTotal as NATIONALYOYWD
from NationalFSUMYOY left join NationalFSUM on NationalFSUMYOY.FSUM_WeekDay_Compared = NationalFSUM.Date;
quit;

proc sql;
create table NationalFSUMYOYDMC as
select *, NationalFSUM.NationalTotal as NATIONALYOYDMC
from NationalFSUMYOYWD left join NationalFSUM on NationalFSUMYOYWD.FSUM_Day_of_the_Mth_Compared = NationalFSUM.Date;
quit;

data NationalFSUMYOYDMC;
set NationalFSUMYOYDMC;
NATIONALYOYWD = NationalTotal - NATIONALYOYWD;
NATIONALYOYDMC = NationalTotal - NATIONALYOYDMC;
run;

proc sql;
create table ONBNQFSUM as
select Date, Issued_Month_Of_Year, Issued_Year_YYYY, (Date-364) as FSUM_WeekDay_Compared format date9., (Date-365) as FSUM_Day_of_the_Mth_Compared format date9., ONTARIO_FUTURE_MONTH, ONTARIO_In_Month, ONTARIO_Sameday, sum(ONTARIO_FUTURE_MONTH+ONTARIO_In_Month+ONTARIO_Sameday) as Ontario_Total from wholeFSUMtable
group by Date;
quit;


proc sql;
create table ONBNQFSUMYOY as
select *
from ONBNQFSUM
where Issued_Month_Of_Year=&currentmonth and Issued_Year_YYYY=&currentyear;
quit;

proc sql;
create table ONBNQFSUMYOYWD as
select *, ONBNQFSUM.Ontario_Total as ONTARIOYOYWD
from ONBNQFSUMYOY left join ONBNQFSUM on ONBNQFSUMYOY.FSUM_WeekDay_Compared = ONBNQFSUM.Date;
quit;

proc sql;
create table ONBNQFSUMYOYDMC as
select *, ONBNQFSUM.Ontario_Total as ONTARIOFSUMYOYDMC
from ONBNQFSUMYOYWD left join ONBNQFSUM on ONBNQFSUMYOYWD.FSUM_Day_of_the_Mth_Compared = ONBNQFSUM.Date;
quit;

data ONBNQFSUMYOYDMC;
set ONBNQFSUMYOYDMC;
ONTARIOYOYWD = Ontario_Total - ONTARIOYOYWD;
ONTARIOFSUMYOYDMC = Ontario_Total - ONTARIOFSUMYOYDMC;
run;

proc sql;
create table QCBNQFSUM as
select Date, Issued_Month_Of_Year, Issued_Year_YYYY, (Date-364) as FSUM_WeekDay_Compared format date9., (Date-365) as FSUM_Day_of_the_Mth_Compared format date9., QUEBEC_FUTURE_MONTH, QUEBEC_In_Month, QUEBEC_Sameday, sum(QUEBEC_FUTURE_MONTH+QUEBEC_In_Month+QUEBEC_Sameday) as QUEBEC_Total from wholeFSUMtable
group by Date;
quit;

proc sql;
create table QCBNQFSUMYOY as
select *
from QCBNQFSUM
where Issued_Month_Of_Year=&currentmonth and Issued_Year_YYYY=&currentyear;
quit;

proc sql;
create table QCBNQFSUMYOYWD as
select *, QCBNQFSUM.QUEBEC_Total as QCYOYWD
from QCBNQFSUMYOY left join QCBNQFSUM on QCBNQFSUMYOY.FSUM_WeekDay_Compared = QCBNQFSUM.Date;
quit;

proc sql;
create table QCBNQFSUMYOYDMC as
select *, QCBNQFSUM.QUEBEC_Total as QCYOYDMC
from QCBNQFSUMYOYWD left join QCBNQFSUM on QCBNQFSUMYOYWD.FSUM_Day_of_the_Mth_Compared = QCBNQFSUM.Date;
quit;

data QCBNQFSUMYOYDMC;
set QCBNQFSUMYOYDMC;
QCYOYWD = QUEBEC_Total - QCYOYWD;
QCYOYDMC = QUEBEC_Total - QCYOYDMC;
run;

proc sql;
create table ATLANTICFSUM as
select Date, Issued_Month_Of_Year, Issued_Year_YYYY, (Date-364) as FSUM_WeekDay_Compared format date9., (Date-365) as FSUM_Day_of_the_Mth_Compared format date9., ATLANTIC_FUTURE_MONTH, ATLANTIC_In_Month, ATLANTIC_Sameday, sum(ATLANTIC_FUTURE_MONTH+ATLANTIC_In_Month+ATLANTIC_Sameday) as ATLANTIC_Total from wholeFSUMtable
group by Date;
quit;

proc sql;
create table ATLANTICFSUMYOY as
select *
from ATLANTICFSUM
where Issued_Month_Of_Year=&currentmonth and Issued_Year_YYYY=&currentyear;
quit;

proc sql;
create table ATLANTICFSUMYOYWD as
select *, ATLANTICFSUM.ATLANTIC_Total as ATYOYWD
from ATLANTICFSUMYOY left join ATLANTICFSUM on ATLANTICFSUMYOY.FSUM_WeekDay_Compared = ATLANTICFSUM.Date;
quit;

proc sql;
create table ATLANTICFSUMYOYDMC as
select *, ATLANTICFSUM.ATLANTIC_Total as ATYOYDMC
from ATLANTICFSUMYOYWD left join ATLANTICFSUM on ATLANTICFSUMYOYWD.FSUM_Day_of_the_Mth_Compared = ATLANTICFSUM.Date;
quit;

data ATLANTICFSUMYOYDMC;
set ATLANTICFSUMYOYDMC;
ATYOYWD = ATLANTIC_Total - ATYOYWD;
ATYOYDMC = ATLANTIC_Total - ATYOYDMC;
run;

proc sql;
create table MTSFSUM as
select Date, Issued_Month_Of_Year, Issued_Year_YYYY, (Date-364) as FSUM_WeekDay_Compared format date9., (Date-365) as FSUM_Day_of_the_Mth_Compared format date9., MTS_FUTURE_MONTH, MTS_In_Month, MTS_Sameday, sum(MTS_FUTURE_MONTH+MTS_In_Month+MTS_Sameday) as MTS_Total from wholeFSUMtable
group by Date;
quit;

proc sql;
create table MTSFSUMYOY as
select *
from MTSFSUM
where Issued_Month_Of_Year=&currentmonth and Issued_Year_YYYY=&currentyear;
quit;

proc sql;
create table MTSFSUMYOYWD as
select *, MTSFSUM.MTS_Total as MTSYOYWD
from MTSFSUMYOY left join MTSFSUM on MTSFSUMYOY.FSUM_WeekDay_Compared = MTSFSUM.Date;
quit;

proc sql;
create table MTSFSUMYOYDMC as
select *, MTSFSUM.MTS_Total as MTSYOYDMC
from MTSFSUMYOYWD left join MTSFSUM on MTSFSUMYOYWD.FSUM_Day_of_the_Mth_Compared = MTSFSUM.Date;
quit;

data MTSFSUMYOYDMC;
set MTSFSUMYOYDMC;
MTSYOYWD = MTS_Total - MTSYOYWD;
MTSYOYDMC = MTS_Total - MTSYOYDMC;
run;

/*Pendingfsums*/
proc sql;
create table onpendingfsumsall as
select FUTURE_CANCEL_DT as Date, sum (CENTRAL_BNQ_ONTARIO) as pendingfsums from fatchedData
where ORDER_STATUS in ('PENDING') and DEACT_TYPE in ('Voluntary Churn') and TIME_TYPE in ('IN_MONTH', 'FUTURE_MONTH')
group by Date;
quit;

proc export data=onpendingfsumsall
                  outfile=""
                  dbms=xlsx
                  replace;
                  sheet= 'onpendingfsumsall';
run;

proc sql;
create table onpendingfsums as
select FUTURE_CANCEL_DT as Date, sum (CENTRAL_BNQ_ONTARIO) as pendingfsums from fatchedData
where ORDER_STATUS in ('PENDING') and DEACT_TYPE in ('Voluntary Churn') and FUTURE_CANCEL_MONTH = &currentmonth and FUTURE_CANCEL_YEAR = &currentyear and FUTURE_CANCEL_DT > (&currentday-1)
group by FUTURE_CANCEL_DT;
quit;

proc sql;
create table onpendingfsumsdate as
select Date, (Date-365) as pendingfsumyoyweek format date9., pendingfsums from onpendingfsums;
quit;

proc sql;
create table onpendingfsumsyoyw as
select ONPENDINGFSUMSDATE.Date, ONPENDINGFSUMSALL.pendingfsums as pendingfsumsw
from ONPENDINGFSUMSDATE left join ONPENDINGFSUMSALL
on ONPENDINGFSUMSDATE.pendingfsumyoyweek = ONPENDINGFSUMSALL.Date;
quit;


proc sql;
create table qcpendingfsumsall as
select FUTURE_CANCEL_DT as Date, sum (CENTRAL_BNQ_QUEBEC) as pendingfsums from fatchedData
where ORDER_STATUS in ('PENDING') and DEACT_TYPE in ('Voluntary Churn') and TIME_TYPE in ('IN_MONTH', 'FUTURE_MONTH')
group by Date;
quit;

proc sql;
create table qcpendingfsums as
select FUTURE_CANCEL_DT as Date, sum (CENTRAL_BNQ_QUEBEC) as pendingfsums from fatchedData
where ORDER_STATUS in ('PENDING') and DEACT_TYPE in ('Voluntary Churn') and FUTURE_CANCEL_MONTH = &currentmonth and FUTURE_CANCEL_YEAR = &currentyear and FUTURE_CANCEL_DT > (&currentday-1)
group by FUTURE_CANCEL_DT;
quit;

proc sql;
create table qcpendingfsumsdate as
select Date, (Date-365) as pendingfsumyoyweek format date9., pendingfsums from qcpendingfsums;
quit;

proc sql;
create table qcpendingfsumsyoyw as
select qcPENDINGFSUMSDATE.Date, qcPENDINGFSUMSALL.pendingfsums as pendingfsumsw
from qcPENDINGFSUMSDATE left join qcPENDINGFSUMSALL
on qcPENDINGFSUMSDATE.pendingfsumyoyweek = qcPENDINGFSUMSALL.Date;
quit;


proc sql;
create table atpendingfsumsall as
select FUTURE_CANCEL_DT as Date, sum (ATLANTIC) as pendingfsums from fatchedData
where ORDER_STATUS in ('PENDING') and DEACT_TYPE in ('Voluntary Churn') and TIME_TYPE in ('IN_MONTH', 'FUTURE_MONTH')
group by Date;
quit;

proc sql;
create table atpendingfsums as
select FUTURE_CANCEL_DT as Date, sum (ATLANTIC) as pendingfsums from fatchedData
where ORDER_STATUS in ('PENDING') and DEACT_TYPE in ('Voluntary Churn') and FUTURE_CANCEL_MONTH = &currentmonth and FUTURE_CANCEL_YEAR = &currentyear and FUTURE_CANCEL_DT > (&currentday-1) and LOB in ('INTERNET', 'VOICE', 'TV')
group by FUTURE_CANCEL_DT;
quit;

proc export data=atpendingfsumsall
                  outfile=""
                  dbms=xlsx
                  replace;
                  sheet= 'atpendingfsumsall';
run;

proc sql;
create table atpendingfsumsdate as
select Date, (Date-365) as pendingfsumyoyweek format date9., pendingfsums from atpendingfsums;
quit;

proc sql;
create table atpendingfsumsyoyw as
select atPENDINGFSUMSDATE.Date, atPENDINGFSUMSALL.pendingfsums as pendingfsumsw
from atPENDINGFSUMSDATE left join atPENDINGFSUMSALL
on atPENDINGFSUMSDATE.pendingfsumyoyweek = atPENDINGFSUMSALL.Date;
quit;


proc sql;
create table mtspendingfsumsall as
select FUTURE_CANCEL_DT as Date, sum (MTS) as pendingfsums from fatchedData
where ORDER_STATUS in ('PENDING') and DEACT_TYPE in ('Voluntary Churn') and TIME_TYPE in ('IN_MONTH', 'FUTURE_MONTH')
group by Date;
quit;

proc sql;
create table mtspendingfsums as
select FUTURE_CANCEL_DT as Date, sum (MTS) as pendingfsums from fatchedData
where ORDER_STATUS in ('PENDING') and DEACT_TYPE in ('Voluntary Churn') and FUTURE_CANCEL_MONTH = &currentmonth and FUTURE_CANCEL_YEAR = &currentyear and FUTURE_CANCEL_DT > (&currentday-1) and LOB in ('INTERNET', 'VOICE', 'TV')
group by FUTURE_CANCEL_DT;
quit;

proc sql;
create table mtspendingfsumsdate as
select Date, (Date-365) as pendingfsumyoyweek format date9., pendingfsums from mtspendingfsums;
quit;

proc sql;
create table mtspendingfsumsyoyw as
select mtsPENDINGFSUMSDATE.Date, mtsPENDINGFSUMSALL.pendingfsums as pendingfsumsw
from mtsPENDINGFSUMSDATE left join mtsPENDINGFSUMSALL
on mtsPENDINGFSUMSDATE.pendingfsumyoyweek = mtsPENDINGFSUMSALL.Date;
quit;


proc sql;
create table nationalpendingfsums as
select Date, sum(pendingfsums) as pendingfsums from(
      select * from onpendingfsums
      union all
      select * from qcpendingfsums
      union all
      select * from atpendingfsums
      union all
      select * from mtspendingfsums)
group by Date;
quit;

proc sql;
create table nationalpendingfsumsyoyw as
select Date, sum(pendingfsumsw) as pendingfsumsw from(
      select * from onpendingfsumsyoyw
      union all
      select * from qcpendingfsumsyoyw
      union all
      select * from atpendingfsumsyoyw
      union all
      select * from mtspendingfsumsyoyw)
group by Date;
quit;


data onpendingfsums;
merge onpendingfsums (in=in_onpendingfsums)
            onpendingfsumsyoyw (in=in_onpendingfsumsyoyw);
by Date;
if in_onpendingfsums;
run;

data qcpendingfsums;
merge qcpendingfsums (in=in_qcpendingfsums)
            qcpendingfsumsyoyw (in=in_qcpendingfsumsyoyw);
by Date;
if in_qcpendingfsums;
run;

data atpendingfsums;
merge atpendingfsums (in=in_atpendingfsums)
            atpendingfsumsyoyw (in=in_atpendingfsumsyoyw);
by Date;
if in_atpendingfsums;
run;

data mtspendingfsums;
merge mtspendingfsums (in=in_mtspendingfsums)
            mtspendingfsumsyoyw (in=in_mtspendingfsumsyoyw);
by Date;
if in_mtspendingfsums;
run;

data nationalpendingfsums;
merge nationalpendingfsums (in=in_nationalpendingfsums)
            nationalpendingfsumsyoyw (in=in_nationalpendingfsumsyoyw);
by Date;
if in_nationalpendingfsums;
run;

/*Whole Report*/
data National (where = (date<&currentday));
merge National (in=in_National)
        nationalplan (in=in_nationalplan)
        nationalyoy (in=in_nationalyoy)
        NationalFSUM (in=in_NationalFSUM)
        NationalFSUMYOYDMC (in=in_NationalFSUMYOYDMC);
by Date;
if in_nationalyoy;

Actual_VS_Plan=Actuals-Plan;
if Actual_VS_Plan>0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan), 0.01);
      else if Actual_VS_Plan<0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan)*(-1), 0.01);
      else if Actual_VS_Plan=0 then Actual_VS_Plan_in_Penc=0;

run;

data National;
set National nationalpendingfsums;
CDate=put(Date, Date9.);
weekday=weekday(Date);
select (weekday);
       when (1) cweekday="Mon";
       when (2) cweekday="Tue";
       when (3) cweekday="Wed";
       when (4) cweekday="Thu";
       when (5) cweekday="Fri";
       when (6) cweekday="Sat";
       when (7) cweekday="Sun";
       end;
Cfulldate=Cdate||" "||cweekday;
pending_FSUM_change=pendingfsums-pendingfsumsw;
pending_FSUM_change_Penc=round((pending_FSUM_change/pendingfsums), 0.01);
run;

data ONBNQ (where = (date<&currentday));
merge ONBNQ (in=in_ONBNQ)
        ontarioplan (in=in_ontarioplan)
        ontarioyoy (in=in_ontarioyoy)
        ONBNQFSUM (in=in_ONBNQFSUM)
        ONBNQFSUMYOYDMC (in=in_ONBNQFSUMYOYDMC);
by Date;
if in_ontarioyoy;

Actual_VS_Plan=Actuals-Plan;
if Actual_VS_Plan>0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan), 0.01);
      else if Actual_VS_Plan<0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan)*(-1), 0.01);
      else if Actual_VS_Plan=0 then Actual_VS_Plan_in_Penc=0;
run;

data ONBNQ;
set ONBNQ onpendingfsums;
CDate=put(Date, Date9.);
weekday=weekday(Date);
select (weekday);
       when (1) cweekday="Mon";
       when (2) cweekday="Tue";
       when (3) cweekday="Wed";
       when (4) cweekday="Thu";
       when (5) cweekday="Fri";
       when (6) cweekday="Sat";
       when (7) cweekday="Sun";
       end;
Cfulldate=Cdate||" "||cweekday;
pending_FSUM_change=pendingfsums-pendingfsumsw;
pending_FSUM_change_Penc=round((pending_FSUM_change/pendingfsums), 0.01);
run;

data QCBNQ (where = (date<&currentday));
merge QCBNQ (in=in_QCBNQ)
        qcplan (in=in_qcplan)
        qcyoy (in=in_qcyoy)
        QCBNQFSUM (in=in_QCBNQFSUM)
        QCBNQFSUMYOYDMC (in=in_QCBNQFSUMYOYDMC);
by Date;
if in_qcyoy;

Actual_VS_Plan=Actuals-Plan;
if Actual_VS_Plan>0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan), 0.01);
      else if Actual_VS_Plan<0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan)*(-1), 0.01);
      else if Actual_VS_Plan=0 then Actual_VS_Plan_in_Penc=0;
run;

data QCBNQ;
set QCBNQ qcpendingfsums;
CDate=put(Date, Date9.);
weekday=weekday(Date);
select (weekday);
       when (1) cweekday="Mon";
       when (2) cweekday="Tue";
       when (3) cweekday="Wed";
       when (4) cweekday="Thu";
       when (5) cweekday="Fri";
       when (6) cweekday="Sat";
       when (7) cweekday="Sun";
       end;
Cfulldate=Cdate||" "||cweekday;
pending_FSUM_change=pendingfsums-pendingfsumsw;
pending_FSUM_change_Penc=round((pending_FSUM_change/pendingfsums), 0.01);
run;

data ATLANTIC (where = (date<&currentday));
merge ATLANTIC (in=in_ATLANTIC)
        atlanticplan (in=in_atlanticplan)
        atyoy (in=in_atyoy)
        ATLANTICFSUM (in=in_ATLANTICFSUM)
        ATLANTICFSUMYOYDMC (in=in_ATLANTICFSUMYOYDMC);
by Date;
if in_atyoy;

Actual_VS_Plan=Actuals-Plan;
if Actual_VS_Plan>0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan), 0.01);
      else if Actual_VS_Plan<0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan)*(-1), 0.01);
      else if Actual_VS_Plan=0 then Actual_VS_Plan_in_Penc=0;
run;

data ATLANTIC;
set ATLANTIC atpendingfsums;
CDate=put(Date, Date9.);
weekday=weekday(Date);
select (weekday);
       when (1) cweekday="Mon";
       when (2) cweekday="Tue";
       when (3) cweekday="Wed";
       when (4) cweekday="Thu";
       when (5) cweekday="Fri";
       when (6) cweekday="Sat";
       when (7) cweekday="Sun";
       end;
Cfulldate=Cdate||" "||cweekday;
pending_FSUM_change=pendingfsums-pendingfsumsw;
pending_FSUM_change_Penc=round((pending_FSUM_change/pendingfsums), 0.01);
run;

data MTS (where = (date<&currentday));
merge MTS (in=in_MTS)
        mtsplan (in=in_mtsplan)
        mtsyoy (in=in_mtsyoy)
        MTSFSUM (in=in_MTSFSUM)
      MTSFSUMYOYDMC (in=in_MTSFSUMYOYDMC);
by Date;
if in_mtsyoy;

Actual_VS_Plan=Actuals-Plan;
if Actual_VS_Plan>0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan), 0.01);
      else if Actual_VS_Plan<0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan)*(-1), 0.01);
      else if Actual_VS_Plan=0 then Actual_VS_Plan_in_Penc=0;
run;

data MTS;
set MTS mtspendingfsums;
CDate=put(Date, Date9.);
weekday=weekday(Date);
select (weekday);
       when (1) cweekday="Mon";
       when (2) cweekday="Tue";
       when (3) cweekday="Wed";
       when (4) cweekday="Thu";
       when (5) cweekday="Fri";
       when (6) cweekday="Sat";
       when (7) cweekday="Sun";
       end;
Cfulldate=Cdate||" "||cweekday;
pending_FSUM_change=pendingfsums-pendingfsumsw;
pending_FSUM_change_Penc=round((pending_FSUM_change/pendingfsums), 0.01);
run;




/*TAB Internet*/

proc sql;
create table tab_int as
select Bell_Division, ORDER_STATUS, LOB, DEACT_TYPE, PORT_OUT_FLAG, Complete_Month_Of_Year, Complete_Year_YYYY, Issued_Month_Of_Year, Issued_Year_YYYY, Issued_Full_Date, FUTURE_CANCEL_DT, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, TIME_TYPE, ATLANTIC, CENTRAL_BNQ_QUEBEC, CENTRAL_BNQ_ONTARIO from FATCHEDDATA
where LOB in ("INTERNET") and ORDER_STATUS in ('CHURN') and DEACT_TYPE in ('Voluntary Churn');
quit;

proc sql;
create table tab_int_actural as
select FUTURE_CANCEL_DT, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, TIME_TYPE, sum(CENTRAL_BNQ_ONTARIO) as ONBNQ, sum(CENTRAL_BNQ_QUEBEC) as QCBNQ, sum(ATLANTIC) as ATLANTIC from tab_int
group by FUTURE_CANCEL_DT, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, TIME_TYPE;
quit;

proc transpose data=tab_int_actural out=tab_int_actural;
by FUTURE_CANCEL_DT FUTURE_CANCEL_MONTH FUTURE_CANCEL_YEAR;
id TIME_TYPE;
run;

data tab_int_actural;
set tab_int_actural;
array change _numeric_;
      do over change;
      if change='.' then change=0;
      end;
Actuals= FUTURE_MONTH + SAMEDAY + IN_MONTH;
run;

proc sql;
create table tab_int_actural_full as
select FUTURE_CANCEL_DT as Date, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR,  (Date-364) as WeekDay_Compared format date9., (Date-365) as Day_of_the_Mth_Compared format date9.,sum(SAMEDAY) as SAMEDAY, sum(IN_MONTH) as IN_MONTH, sum(FUTURE_MONTH) as FUTURE_MONTH, sum(Actuals) as Actuals from tab_int_actural
group by Date, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR;
quit;


proc sql;
create table tab_int_DeactPlan as
select FULL_DATE as Date, sum(QUANTITY_RGU) as Plan
from fatchedData2
where KPI_LOB in ("INTERNET") and LEDGER not like ('%OL W')
group by FULL_DATE;
quit;


proc sql;
create table tab_int_DeactPlanYoY as
select Date, (Date-364) as WeekDay_Compared format date9., (Date-365) as Day_of_the_Mth_Compared format date9., FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR, Sum(Actuals) as Actuals from tab_int_actural_full
where FUTURE_CANCEL_YEAR  between (&currentyear-1) and &currentyear
group by Date, WeekDay_Compared, Day_of_the_Mth_Compared, FUTURE_CANCEL_MONTH, FUTURE_CANCEL_YEAR;
quit;

proc sql;
create table tab_int_DeactPlanYoY_w as
select tab_int_actural_full.*, tab_int_DeactPlanYoY.*, tab_int_DeactPlanYoY.Actuals as Actualsw
from tab_int_actural_full left join tab_int_DeactPlanYoY on tab_int_actural_full.WeekDay_Compared=tab_int_DeactPlanYoY.Date;
quit;

proc sql;
create table tab_int_DeactPlanYoY_m as
select tab_int_DeactPlanYoY_w.*, tab_int_DeactPlanYoY.Actuals as Actualsm
from tab_int_DeactPlanYoY_w left join tab_int_DeactPlanYoY on tab_int_DeactPlanYoY_w.Day_of_the_Mth_Compared = tab_int_DeactPlanYoY.Date;
quit;

proc sql;
create table tab_int_DeactPlanYoY_m_change as
select Date, Actuals, Actualsw, Actualsm, (Actuals-Actualsw) as Actualswchange, (Actuals-Actualsm) as Actualsmchange
from tab_int_DeactPlanYoY_m;
quit;

data tab_int_DeactPlanYoY_m_change(keep=Date Actualswchange Actualsmchange);
set tab_int_DeactPlanYoY_m_change;
run;

/**/
/*FSUM Actuals/Future/In Month/Sameday */

proc sql;
create table INTFSUMFM as
select Issued_Full_Date as Date, Issued_Month_Of_Year, Issued_Year_YYYY, sum(CENTRAL_BNQ_ONTARIO) as ONTARIO_FUTURE_MONTH, sum(CENTRAL_BNQ_QUEBEC) as QUEBEC_FUTURE_MONTH, sum(ATLANTIC) as ATLANTIC_FUTURE_MONTH, sum(MTS) as MTS_FUTURE_MONTH, sum(CENTRAL_BNQ_ONTARIO + CENTRAL_BNQ_QUEBEC + ATLANTIC + MTS) as TotalFM from PivotMTDOrderFSUM
where TIME_TYPE in ('FUTURE_MONTH') and LOB in ('INTERNET')
group by Date, Issued_Month_Of_Year, Issued_Year_YYYY
order by Date;
quit;

proc sql;
create table INTFSUMIM as
 select Date format date9., Issued_Month_Of_Year, Issued_Year_YYYY, sum(CENTRAL_BNQ_ONTARIO) as ONTARIO_In_Month, sum(CENTRAL_BNQ_QUEBEC) as QUEBEC_In_Month, sum(ATLANTIC) as ATLANTIC_In_Month, sum(MTS) as MTS_In_Month, sum(CENTRAL_BNQ_ONTARIO + CENTRAL_BNQ_QUEBEC + ATLANTIC + MTS) as TotalIN from (
      select Issued_Full_Date as Date, Issued_Month_Of_Year, Issued_Year_YYYY, CENTRAL_BNQ_ONTARIO, CENTRAL_BNQ_QUEBEC, ATLANTIC, MTS from PivotMTDOrderFSUM
      where TIME_TYPE in ('IN_MONTH') and LOB in ('INTERNET')
      )
 group by Date, Issued_Month_Of_Year, Issued_Year_YYYY
 order by Date;
quit;

proc sql;
create table INTFSUMSD as
 select Date format date9., Issued_Month_Of_Year, Issued_Year_YYYY, sum(CENTRAL_BNQ_ONTARIO) as ONTARIO_Sameday, sum(CENTRAL_BNQ_QUEBEC) as QUEBEC_Sameday, sum(ATLANTIC) as ATLANTIC_Sameday, sum(MTS) as MTS_Sameday, sum(CENTRAL_BNQ_ONTARIO + CENTRAL_BNQ_QUEBEC + ATLANTIC + MTS) as TotalSD from (
      select Issued_Full_Date as Date, Issued_Month_Of_Year, Issued_Year_YYYY, CENTRAL_BNQ_ONTARIO, CENTRAL_BNQ_QUEBEC, ATLANTIC, MTS from PivotMTDOrderFSUM
      where TIME_TYPE in ('SAMEDAY') and LOB in ('INTERNET')
      )
 group by Date, Issued_Month_Of_Year, Issued_Year_YYYY
 order by Date;
quit;


data wholeintFSUMtable (drop = ONTARIO_FUTURE_MONTH QUEBEC_FUTURE_MONTH ATLANTIC_FUTURE_MONTH MTS_FUTURE_MONTH
                                           ONTARIO_In_Month QUEBEC_In_Month ATLANTIC_In_Month MTS_In_Month
                                          ONTARIO_Sameday QUEBEC_Sameday ATLANTIC_Sameday MTS_Sameday);
merge INTFSUMFM (in=in_INTFSUMFM)
            INTFSUMIM (in=in_INTFSUMIM)
            INTFSUMSD (in=in_INTFSUMIM);
by Date;

array missing _numeric_;
      do over missing;
            if missing=. then missing=0;
            end;
FSUM_actural=TotalFM+TotalIN+TotalSD;
run;

data tab_int_whole (where = (date<&currentday));
merge tab_int_actural_full (in=in_tab_int_actural_full)
        tab_int_DeactPlan (in=in_tabintDeactPlan)
        tab_int_DeactPlanYoY_m_change (in=in_tab_int_DeactPlanYoY_m_change);
by Date;
if in_tabintDeactPlan;

Actual_VS_Plan=Actuals-Plan;
if Actual_VS_Plan>0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan), 0.01);
      else if Actual_VS_Plan<0 then Actual_VS_Plan_in_Penc=round((Actuals/Plan)*(-1), 0.01);
      else if Actual_VS_Plan=0 then Actual_VS_Plan_in_Penc=0;
run;

proc format;
      value fcolor
            low - 0 = 'green'
            0 - high = 'red';
run;

options orientation=landscape nodate;
ods pdf file="" style=htmlblue pdftoc=1 uniform;
ods proclabel='National Deacts & FSUMS';
ods pdf startpage=never;

proc report data=National
      contents="National Deacts & FSUMS"
      nowindows
      spacing=1
      headline
      headskip
      split="|"
      style(header) = {font_weight=bold font_size=6pt}
      style(summary) = {font_weight=bold font_size=6pt}
      style(column)={frame=box bordercolor=black borderwidth=1px font_size=6pt};

      columns (Cfulldate Actuals Prior_Month In_Month Sameday Plan Actual_VS_Plan Actual_VS_Plan_in_Penc YOY_WeekDay_Compared YOY_Day_of_the_Mth_Compared NationalTotal TotalFM TotalIN TotalSD NATIONALYOYWD NATIONALYOYDMC pendingfsums pendingfsumsw pending_FSUM_change_Penc);
      define Cfulldate /display "Date";
      define Actuals /analysis sum "Deact Actuals ";
      define Prior_Month /analysis sum "Deact Prior Month";
      define In_Month /analysis sum "Deact In Month";
      define Sameday /analysis sum "Deact Sameday";
      define Plan /analysis sum "Deact Plan" format=10.;
      define Actual_VS_Plan /analysis sum "Deact Actual VS Deact Plan" format=10. style(column)={foreground=fcolor.};
      define Actual_VS_Plan_in_Penc /analysis sum "% to Plan" f=percentN12. style(column)={foreground=fcolor.};
      define YOY_WeekDay_Compared /analysis sum "Deact YoY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define YOY_Day_of_the_Mth_Compared /analysis sum "Deact YoY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define NationalTotal /analysis sum "FSUM Actuals";
      define TotalFM /analysis sum "FSUM Future Month";
      define TotalIN /analysis sum "FSUM In Month";
      define TotalSD /analysis sum "FSUM Sameday";
      define NATIONALYOYWD /analysis sum "FSUM YOY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define NATIONALYOYDMC /analysis sum "FSUM YOY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define pendingfsums /analysis sum "Pending Fsums";
      define pendingfsumsw /analysis sum "Pending Fsums YOY (WeekDay Compared)";
      define pending_FSUM_change_Penc /analysis sum "Pending Fsums % Change" f=percentN12. style(column)={foreground=fcolor.};
      rbreak after/summarize;
      compute after;
            Cfulldate = "Total";
      endcomp;

      title1 "National Deacts & FSUMS (MEE: &nationalmee)";
      footnote1 j=left height=5pt "Deacts: Total  Vol Churn driven by sameday, current month and prior month deactivations";
      footnote2 j=left height=5pt "FSUMS: Future deactivations driven by sameday, in month and next month forecasted deactivations";
      footnote3 j=left height=5pt "Actuals & Outlook: Actual Month-to-Date (MTD) performance and remaining forecasted outlook";
      footnote4 j=left height=5pt "Next Month: Pending deacts into subsequent month";
      footnote5 j=left height=5pt "Pending Fsums: Pending Deactivation corresponding to date in current month";
      footnote6 j=left height=5pt "Actuals +Pending: View of how full the day's deact target is - pending futures / deact forecast (only includes In Month and Future Month deacts)";
      footnote7 j=left height=5pt "MEE: Actuals + Outlook";
      footnote8 j=left height=5pt "Pending Fsums: YOY(WeekDay Compared) and percent change to Plan";
      run;

ods proclabel='Ontario Deacts & FSUMS';
ods startpage=now;

proc report data=ONBNQ
      contents="Ontario Deacts & FSUMS"
      nowindows
      spacing=1
      headline
      headskip
      split="|"
      style(header) = {font_weight=bold font_size=6pt}
      style(summary) = {font_weight=bold font_size=6pt}
      style(column)={frame=box bordercolor=black borderwidth=1px font_size=6pt};

      columns (Cfulldate Actuals Prior_Month In_Month Sameday Plan Actual_VS_Plan Actual_VS_Plan_in_Penc YOY_WeekDay_Compared YOY_Day_of_the_Mth_Compared Ontario_Total ONTARIO_FUTURE_MONTH ONTARIO_In_Month ONTARIO_Sameday ONTARIOYOYWD ONTARIOFSUMYOYDMC pendingfsums pendingfsumsw pending_FSUM_change_Penc);
      define Cfulldate /display "Date";
      define Actuals /analysis sum "Deact Actuals ";
      define Prior_Month /analysis sum "Deact Prior Month";
      define In_Month /analysis sum "Deact In Month";
      define Sameday /analysis sum "Deact Sameday";
      define Plan /analysis sum "Deact Plan" format=10.;
      define Actual_VS_Plan /analysis sum "Deact Actual VS Deact Plan" format=10. style(column)={foreground=fcolor.};
      define Actual_VS_Plan_in_Penc /analysis sum "% to Plan" f=percentN12. style(column)={foreground=fcolor.};
      define YOY_WeekDay_Compared /analysis sum "Deact YoY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define YOY_Day_of_the_Mth_Compared /analysis sum "Deact YoY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define Ontario_Total /analysis sum "FSUM Actuals";
      define ONTARIO_FUTURE_MONTH /analysis sum "FSUM Future Month";
      define ONTARIO_In_Month /analysis sum "FSUM In Month";
      define ONTARIO_Sameday /analysis sum "FSUM Sameday";
      define ONTARIOYOYWD /analysis sum "FSUM YOY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define ONTARIOFSUMYOYDMC /analysis sum "FSUM YOY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define pendingfsums /analysis sum "Pending Fsums";
      define pendingfsumsw /analysis sum "Pending Fsums YOY (WeekDay Compared)";
      define pending_FSUM_change_Penc /analysis sum "Pending Fsums % Change" f=percentN12. style(column)={foreground=fcolor.};

      rbreak after/summarize;
      compute after;
            Cfulldate = "Total";
      endcomp;
      title1 "Ontario Deacts & FSUMS (MEE: &ontariomee)";
      footnote1 j=left height=5pt "Deacts: Total  Vol Churn driven by sameday, current month and prior month deactivations";
      footnote2 j=left height=5pt "FSUMS: Future deactivations driven by sameday, in month and next month forecasted deactivations";
      footnote3 j=left height=5pt "Actuals & Outlook: Actual Month-to-Date (MTD) performance and remaining forecasted outlook";
      footnote4 j=left height=5pt "Next Month: Pending deacts into subsequent month";
      footnote5 j=left height=5pt "Pending Fsums: Pending Deactivation corresponding to date in current month";
      footnote6 j=left height=5pt "Actuals +Pending: View of how full the day's deact target is - pending futures / deact forecast (only includes In Month and Future Month deacts)";
      footnote7 j=left height=5pt "MEE: Actuals + Outlook";
      footnote8 j=left height=5pt "Pending Fsums: YOY(WeekDay Compared) and percent change to Plan";
      run;

ods proclabel='Quebec Deacts & FSUMS';
ods startpage=now;

proc report data=QCBNQ
      contents="Quebec Deacts & FSUMS"
      nowindows
      spacing=1
      headline
      headskip
      split="|"
      style(header) = {font_weight=bold font_size=6pt}
      style(summary) = {font_weight=bold font_size=6pt}
      style(column)={frame=box bordercolor=black borderwidth=1px font_size=6pt};

      columns (Cfulldate Actuals Prior_Month In_Month Sameday Plan Actual_VS_Plan Actual_VS_Plan_in_Penc YOY_WeekDay_Compared YOY_Day_of_the_Mth_Compared QUEBEC_Total QUEBEC_FUTURE_MONTH QUEBEC_In_Month QUEBEC_Sameday QCYOYWD QCYOYDMC pendingfsums pendingfsumsw pending_FSUM_change_Penc);
      define Cfulldate /display "Date";
      define Actuals /analysis sum "Deact Actuals ";
      define Prior_Month /analysis sum "Deact Prior Month";
      define In_Month /analysis sum "Deact In Month";
      define Sameday /analysis sum "Deact Sameday";
      define Plan /analysis sum "Deact Plan" format=10.;
      define Actual_VS_Plan /analysis sum "Deact Actual VS Deact Plan" format=10. style(column)={foreground=fcolor.};
      define Actual_VS_Plan_in_Penc /analysis sum "% to Plan" f=percentN12. style(column)={foreground=fcolor.};
      define YOY_WeekDay_Compared /analysis sum "Deact YoY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define YOY_Day_of_the_Mth_Compared /analysis sum "Deact YoY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define QUEBEC_Total /analysis sum "FSUM Actuals";
      define QUEBEC_FUTURE_MONTH /analysis sum "FSUM Future Month";
      define QUEBEC_In_Month /analysis sum "FSUM In Month";
      define QUEBEC_Sameday /analysis sum "FSUM Sameday";
      define QCYOYWD /analysis sum "FSUM YOY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define QCYOYDMC /analysis sum "FSUM YOY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define pendingfsums /analysis sum "Pending Fsums";
      define pendingfsumsw /analysis sum "Pending Fsums YOY (WeekDay Compared)";
      define pending_FSUM_change_Penc /analysis sum "Pending Fsums % Change" f=percentN12. style(column)={foreground=fcolor.};

      rbreak after/summarize;
      compute after;
            Cfulldate = "Total";
      endcomp;
      title1 "Quebec Deacts & FSUMS (MEE: &quebecmee)";
      footnote1 j=left height=5pt "Deacts: Total  Vol Churn driven by sameday, current month and prior month deactivations";
      footnote2 j=left height=5pt "FSUMS: Future deactivations driven by sameday, in month and next month forecasted deactivations";
      footnote3 j=left height=5pt "Actuals & Outlook: Actual Month-to-Date (MTD) performance and remaining forecasted outlook";
      footnote4 j=left height=5pt "Next Month: Pending deacts into subsequent month";
      footnote5 j=left height=5pt "Pending Fsums: Pending Deactivation corresponding to date in current month";
      footnote6 j=left height=5pt "Actuals +Pending: View of how full the day's deact target is - pending futures / deact forecast (only includes In Month and Future Month deacts)";
      footnote7 j=left height=5pt "MEE: Actuals + Outlook";
      footnote8 j=left height=5pt "Pending Fsums: YOY(WeekDay Compared) and percent change to Plan";
      run;

ods proclabel='Atlantic Deacts & FSUMS';
ods startpage=now;

proc report data=ATLANTIC
      contents="Atlantic Deacts & FSUMS"
      nowindows
      spacing=1
      headline
      headskip
      split="|"
      style(header) = {font_weight=bold font_size=6pt}
      style(summary) = {font_weight=bold font_size=6pt}
      style(column)={frame=box bordercolor=black borderwidth=1px font_size=6pt};

      columns (Cfulldate Actuals Prior_Month In_Month Sameday Plan Actual_VS_Plan Actual_VS_Plan_in_Penc YOY_WeekDay_Compared YOY_Day_of_the_Mth_Compared ATLANTIC_Total ATLANTIC_FUTURE_MONTH ATLANTIC_In_Month ATLANTIC_Sameday ATYOYWD ATYOYDMC pendingfsums pendingfsumsw pending_FSUM_change_Penc);
      define Cfulldate /display "Date" style(column)={frame=box bordercolor=black borderwidth=1px font_size=7.5pt};
      define Actuals /analysis sum "Deact Actuals ";
      define Prior_Month /analysis sum "Deact Prior Month";
      define In_Month /analysis sum "Deact In Month";
      define Sameday /analysis sum "Deact Sameday";
      define Plan /analysis sum "Deact Plan" format=10.;
      define Actual_VS_Plan /analysis sum "Deact Actual VS Deact Plan" format=10. style(column)={foreground=fcolor.};
      define Actual_VS_Plan_in_Penc /analysis sum "% to Plan" f=percentN12. style(column)={foreground=fcolor.};
      define YOY_WeekDay_Compared /analysis sum "Deact YoY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define YOY_Day_of_the_Mth_Compared /analysis sum "Deact YoY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define ATLANTIC_Total /analysis sum "FSUM Actuals";
      define ATLANTIC_FUTURE_MONTH /analysis sum "FSUM Future Month";
      define ATLANTIC_In_Month /analysis sum "FSUM In Month";
      define ATLANTIC_Sameday /analysis sum "FSUM Sameday";
      define ATYOYWD /analysis sum "FSUM YOY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define ATYOYDMC /analysis sum "FSUM YOY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define pendingfsums /analysis sum "Pending Fsums";
      define pendingfsumsw /analysis sum "Pending Fsums YOY (WeekDay Compared)";
      define pending_FSUM_change_Penc /analysis sum "Pending Fsums % Change" f=percentN12. style(column)={foreground=fcolor.};

      rbreak after/summarize;
      compute after;
            Cfulldate = "Total";
      endcomp;
      title1 "Atlantic Deacts & FSUMS (MEE: &atlanticmee)";
      footnote1 j=left height=5pt "Deacts: Total  Vol Churn driven by sameday, current month and prior month deactivations";
      footnote2 j=left height=5pt "FSUMS: Future deactivations driven by sameday, in month and next month forecasted deactivations";
      footnote3 j=left height=5pt "Actuals & Outlook: Actual Month-to-Date (MTD) performance and remaining forecasted outlook";
      footnote4 j=left height=5pt "Next Month: Pending deacts into subsequent month";
      footnote5 j=left height=5pt "Pending Fsums: Pending Deactivation corresponding to date in current month";
      footnote6 j=left height=5pt "Actuals +Pending: View of how full the day's deact target is - pending futures / deact forecast (only includes In Month and Future Month deacts)";
      footnote7 j=left height=5pt "MEE: Actuals + Outlook";
      footnote8 j=left height=5pt "Pending Fsums: YOY(WeekDay Compared) and percent change to Plan";
      run;

ods proclabel='MTS Deacts & FSUMS';
ods startpage=now;

proc report data=MTS
      contents="MTS Deacts & FSUMS"
      nowindows
      spacing=1
      headline
      headskip
      split="|"
      style(header) = {font_weight=bold font_size=6pt}
      style(summary) = {font_weight=bold font_size=6pt}
      style(column)={frame=box bordercolor=black borderwidth=1px font_size=6pt};

      columns (Cfulldate Actuals Prior_Month In_Month Sameday Plan Actual_VS_Plan Actual_VS_Plan_in_Penc YOY_WeekDay_Compared YOY_Day_of_the_Mth_Compared MTS_Total MTS_FUTURE_MONTH MTS_In_Month MTS_Sameday MTSYOYWD MTSYOYDMC pendingfsums pendingfsumsw pending_FSUM_change_Penc);
      define Cfulldate /display "Date";
      define Actuals /analysis sum "Deact Actuals ";
      define Prior_Month /analysis sum "Deact Prior Month";
      define In_Month /analysis sum "Deact In Month";
      define Sameday /analysis sum "Deact Sameday";
      define Plan /analysis sum "Deact Plan" format=10.;
      define Actual_VS_Plan /analysis sum "Deact Actual VS Deact Plan" format=10. style(column)={foreground=fcolor.};
      define Actual_VS_Plan_in_Penc /analysis sum "% to Plan" f=percentN12. style(column)={foreground=fcolor.};
      define YOY_WeekDay_Compared /analysis sum "Deact YoY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define YOY_Day_of_the_Mth_Compared /analysis sum "Deact YoY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define MTS_Total /analysis sum "FSUM Actuals";
      define MTS_FUTURE_MONTH /analysis sum "FSUM Future Month";
      define MTS_In_Month /analysis sum "FSUM In Month";
      define MTS_Sameday /analysis sum "FSUM Sameday";
      define MTSYOYWD /analysis sum "FSUM YOY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define MTSYOYDMC /analysis sum "FSUM YOY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      define pendingfsums /analysis sum "Pending Fsums";
      define pendingfsumsw /analysis sum "Pending Fsums YOY (WeekDay Compared)";
      define pending_FSUM_change_Penc /analysis sum "Pending Fsums % Change" f=percentN12. style(column)={foreground=fcolor.};

      rbreak after/summarize;
      compute after;
            Cfulldate = "Total";
      endcomp;
      title1 "MTS Deacts & FSUMS (MEE: &mtsmee)";
      footnote1 j=left height=5pt "Deacts: Total  Vol Churn driven by sameday, current month and prior month deactivations";
      footnote2 j=left height=5pt "FSUMS: Future deactivations driven by sameday, in month and next month forecasted deactivations";
      footnote3 j=left height=5pt "Actuals & Outlook: Actual Month-to-Date (MTD) performance and remaining forecasted outlook";
      footnote4 j=left height=5pt "Next Month: Pending deacts into subsequent month";
      footnote5 j=left height=5pt "Pending Fsums: Pending Deactivation corresponding to date in current month";
      footnote6 j=left height=5pt "Actuals +Pending: View of how full the day's deact target is - pending futures / deact forecast (only includes In Month and Future Month deacts)";
      footnote7 j=left height=5pt "MEE: Actuals + Outlook";
      footnote8 j=left height=5pt "Pending Fsums: YOY(WeekDay Compared) and percent change to Plan";
      run;

ods proclabel='Internet Deacts & FSUMS';
ods startpage=now;

proc report data=tab_int_whole
      contents="Internet Deacts & FSUMS"
      nowindows
      spacing=1
      headline
      headskip
      split="|"
      style(header) = {font_weight=bold font_size=6pt}
      style(summary) = {font_weight=bold font_size=6pt}
      style(column)={frame=box bordercolor=black borderwidth=1px font_size=6pt};

      columns (Date Actuals FUTURE_MONTH IN_MONTH SAMEDAY Plan Actual_VS_Plan Actual_VS_Plan_in_Penc Actualswchange Actualsmchange);
      define Date /display "Date";
      define Actuals /analysis sum "Deact Actuals ";
      define FUTURE_MONTH /analysis sum "Deact Prior Month";
      define IN_MONTH /analysis sum "Deact In Month";
      define SAMEDAY /analysis sum "Deact Sameday";
      define Plan /analysis sum "Deact Plan" format=10.;
      define Actual_VS_Plan /analysis sum "Deact Actual VS Deact Plan" format=10. style(column)={foreground=fcolor.};
      define Actual_VS_Plan_in_Penc /analysis sum "% to Plan" f=percentN12. style(column)={foreground=fcolor.};
      define Actualswchange /analysis sum "Deact YoY (WeekDay Compared)" style(column)={foreground=fcolor.};
      define Actualsmchange /analysis sum "Deact YoY (Day of the Mth Compared)" style(column)={foreground=fcolor.};
      rbreak after/summarize;
      compute after;
            CDate = "Total";
      endcomp;
      title1 "Internet Deacts & FSUMS (MEE: &mtsmee)";
      footnote1 j=left height=5pt "Deacts: Total  Vol Churn driven by sameday, current month and prior month deactivations";
      footnote2 j=left height=5pt "FSUMS: Future deactivations driven by sameday, in month and next month forecasted deactivations";
      footnote3 j=left height=5pt "Actuals & Outlook: Actual Month-to-Date (MTD) performance and remaining forecasted outlook";
      footnote4 j=left height=5pt "Next Month: Pending deacts into subsequent month";
      footnote5 j=left height=5pt "Pending Fsums: Pending Deactivation corresponding to date in current month";
      footnote6 j=left height=5pt "Actuals +Pending: View of how full the day's deact target is - pending futures / deact forecast (only includes In Month and Future Month deacts)";
      footnote7 j=left height=5pt "MEE: Actuals + Outlook";
      footnote8 j=left height=5pt "Pending Fsums: YOY(WeekDay Compared) and percent change to Plan";
      run;

ods pdf close;

FILENAME MailBox EMAIL
ATTACH=("" content_type="application/pdf") content_type="text/html";
data _NULL_;
FILE MailBox
/*          TO= ('')*/
/*          CC= ('')*/
/*          FROM=('a')*/
            TO= ('')
            CC= ('')
            FROM=('')
        SUBJECT="SB FSUMS(Incl MTS) Trending Sheet &sysdate9.";
            PUT "Hi, please see attached SB FSUMS(Incl MTS) Trending Sheet &sysdate9.";
            PUT '<br>';
            PUT '<br>';
            PUT "For changes to the distribution list please";
        PUT '<a href="">Click Here</a>';
        PUT '<br>';
        PUT '<br>';
        PUT "If you have any questions or require further information please";
        PUT '<a href="">Contact Us</a>';
        PUT '<br>';
run;
