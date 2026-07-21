
ALTER PROCEDURE [dbo].[usp_RPT_Envision_Placement]
AS 


TRUNCATE TABLE RPT_Envision_Placement
INSERT INTO RPT_Envision_Placement
select  dd.MonthYear as Month_Year,case when LEFT(dcl.ClientId,1)='R' then 'Prime' 
when LEFT(dcl.ClientId,1)='2' then 'Second' else 'Others' end as class ,

count(DISTINCT dcu.KeyCustomer) Accounts,count(distinct (case when  LEN(rp.PhoneNumber)=10 then dcu.Keycustomer end)) PhoneNumbers,
count(distinct (case when  LEN(dcu.CustomerAddress1)>5 then dcu.Keycustomer end)) Adresses,
count(distinct (case when  LEN(dcu.SSN)>=4 then dcu.Keycustomer end)) SSNs,

count( distinct case when DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate) < 3 then dcu.KeyCustomer  end ) as less_than_months,
count( distinct case  when DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate) >= 3 and DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate)<6 then dcu.KeyCustomer end) as three_six_months,
count( distinct case when DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate) >= 6 and DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate)<9 then dcu.KeyCustomer end) as six_nine_months,
count( distinct case when DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate) >= 9 and DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate)<12 then dcu.KeyCustomer end) as nine_12_months
---,
---count( distinct case when DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate) >=12 then dcu.KeyCustomer end) as more_12_months

,count( distinct case  when DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate) >= 12 and DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate)<15 then dcu.KeyCustomer end) as twelve_fifteen_months
,count( distinct case  when DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate) >= 15 and DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate)<18 then dcu.KeyCustomer end) as fifteen_eighteen_months
,count( distinct case when DATEDIFF(MONTH,dcu.ClientOpenDate,dcu.ListDate) >=18 then dcu.KeyCustomer end) as more_18_months

from [DW_MSTR_DM].dbo.DimCustomer dcu
left JOIN [DW_MSTR_DM].[dbo].[RadiusPhone] rp ON dcu.KeyCustomer=rp.KeyCustomer

join [DW_MSTR_DM].[dbo].[DimClient] dcl ON dcu.ClientId=dcl.ClientId

join [DW_MSTR_DM].[dbo].[DimDate] dd on dd.Calendardate=dcu.listDate


WHERE dcl.ClientParent LIKE 'PRT%'  


group by dd.MonthYear ,case when LEFT(dcl.ClientId,1)='R' then 'Prime' 
when LEFT(dcl.ClientId,1)='2' then 'Second' else 'Others' end  


GO


 


