/*This program mainly uses proc sql to automatically search the time to complete the weekly sales report*/
data _null_;
a=time();
call symput('start',a);
run;
%let folder=%str(folder);
libname KPI "&folder"; 
%let datafile=kpi.sasbi_report;
%let var1=prov;
%let var2=channel;
%let var3=Buy_date;
%let var4=buy_amount;

%macro reconcile;
        proc means data=&datafile min max sum n nmiss;
                   var &var3 &var4;
             run;

	    proc freq data=&datafile; 
                  table &var1 &var2; 
             run;
%mend;
%reconcile;

sasfile kpi.sasbi_report load;
proc sql;
     create table DailySales as
     select &var1,
            &var2,
            &var3,
            Sum (&var4) as DailySales
     from kpi.sasbi_report
     group by &var1, &var2, &var3
;
quit;
sasfile kpi.sasbi_report close;

proc export data=DailySales
                outfile='E:\SAS\DailySales'
                dbms=xlsx
                replace;
quit;
data DailySales;
     set DailySales;
         DayofWeek=Weekday(&var3);
         WeekofYear=Week(&var3);
		 DayofMonth=Day(&var3);
         MonthofYear=Month(&var3);
         QuarterofYear=Qtr(&var3);
         SaleofYear=Year(&var3);
run; 

sasfile DailySales load;
proc sql;
    create table WeektoDateReport as
	select *	       
	from (     select SaleofYear,
                      prov,
                      WeekofYear,
                      Sum(DailySales) as WeeklySales
               from DailySales
	           group by SaleofYear,
                        prov,
                        WeekofYear
	           having SaleofYear=(select Max(SaleofYear) from DailySales))
	having weekofyear>Max(weekofyear)-4
;
quit;
sasfile DailySales close;

data WeektoDateReport;
set WeektoDateReport;
format WOW 8.2;
select (prov);
    when ('AB') WOW = Round(dif( WeeklySales ) / lag( WeeklySales ) * 100, 0.01);
    when ('Atlantic') WOW = Round( dif(WeeklySales ) / lag( WeeklySales ) * 100, 0.01);
    when ('BC') WOW = Round(dif( WeeklySales ) / lag( WeeklySales ) * 100, 0.01);
	when ('ON') WOW = Round(dif( WeeklySales ) / lag( WeeklySales ) * 100, 0.01);
    when ('QC') WOW = Round(dif( WeeklySales ) / lag( WeeklySales ) * 100, 0.01);
	otherwise WOW = Round(dif( WeeklySales ) / lag( WeeklySales ) * 100, 0.01);
	end;
	run;

proc transpose data=WeektoDateReport out=WeektoDateReport1 prefix=week;
by saleofyear prov;
run;

proc sql;
create table MonthtoDateReport as
select *
from (select SaleofYear,
             prov,
             MonthofYear,
             Sum(DailySales) as MonthtoDate
      from DailySales
	  group by SaleofYear,
             prov,
             MonthofYear
	  having SaleofYear=(select Max(SaleofYear) from DailySales)
)
having MonthofYear=Max(MonthofYear)
;
quit;

proc sql;
create table QuarterReport as
select *
from ( select SaleofYear,
              prov,
              QuarterofYear,
              Sum(DailySales) as QuarterSales
       from DailySales
	   group by SaleofYear,
                prov,
                QuarterofYear
	   having SaleofYear=(select Max(SaleofYear) from DailySales))
having QuarterofYear=Max(QuarterofYear)
;
quit;

proc sql;
create table YeartoDateReport as
select SaleofYear,
       prov,
       Sum(DailySales) as YeartoDateSales
from DailySales
group by SaleofYear,
         &var1
having SaleofYear=(select Max(SaleofYear) from DailySales)
;
quit;

data KPI_uptodate;
merge weektodatereport1 monthtodatereport quarterreport yeartodatereport;
by prov;
run;

data _null_;
a=time();
b=a-&start;
put b time.;
run;
