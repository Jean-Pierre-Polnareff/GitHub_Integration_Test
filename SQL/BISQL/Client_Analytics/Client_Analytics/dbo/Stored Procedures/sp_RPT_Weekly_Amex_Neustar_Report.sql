




--SET QUOTED_IDENTIFIER ON
--GO



---CREATE PROCEDURE  [dbo].[sp_RPT_Weekly_Amex_Neustar_Report]
CREATE PROCEDURE  [dbo].[sp_RPT_Weekly_Amex_Neustar_Report]
     
AS
/* 
Object: sp_RPT_Weekly_Amex_Neustar_Report

Description: Identify and insert Amex Neustar Report Weekly data into [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Summarized] and [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Active_Account_Phone_Inventory_Summarized]

Author			Date		Description
Amod Ramugade	12/27/2021	Created
*/

BEGIN
	SET NOCOUNT ON;




----declare @start_dt datetime = '2021-12-17 00:00:00';
----declare @end_dt datetime = '2021-12-23 23:59:59';	


-----------------------------------------------Date range for Weekly report.........................................................................
----------IF DATENAME(dw,getdate()) = 'Friday' AND DAY(GETDATE()) <> 1
---------DECLARE @start_dt DATETIME =  DATEADD(DAY, -8, DATEADD(day, DATEDIFF(day, 0, GETDATE()), 0));                        ----start of today minus 7 days
---------DECLARE @end_dt datetime =  DATEADD(SECOND, -2, DATEADD(day, DATEDIFF(day, 0, GETDATE()), 0));                       ----start of today minus 1 second
----------SELECT @start_dt;
----------SELECT @end_dt;

DECLARE @fri DATETIME = DATEADD(D,-(DATEPART(W,GETDATE())+1)%7,GETDATE())                                                     ----last Friday
---DECLARE @fri DATETIME =  DATEADD(day, DATEDIFF(day, 0,DATEADD(D,-(DATEPART(W,GETDATE())+1)%7,GETDATE())),0);               ----start of last Friday
DECLARE @start_dt DATETIME = DATEADD(day, DATEDIFF(day, 0, DATEADD(D, -7, @fri)), 0);                                         ----start of last Friday minus 7 days
DECLARE @end_dt datetime =  DATEADD(SECOND, -1, DATEADD(day, DATEDIFF(day, 0, @fri), 0));                                     ----start of last Friday minus 1 second


--------- Inserting the data into [#TMP_Amex_Neustar_Scored_All] --------------

----DROP TABLE	IF EXISTS	#TMP_Amex_Neustar_Scored_All
IF OBJECT_ID('tempdb..#TMP_Amex_Neustar_Scored_All') IS NOT NULL
		DROP TABLE #TMP_Amex_Neustar_Scored_All

SELECT * INTO #TMP_Amex_Neustar_Scored_All FROM
(
SELECT 
---DISTINCT
a.CustomerID
,a.KeyCustomer
,a.KeySourceSystem
,b.SourceSystem
,case when ltrim(a.Neu_PhoneInService)='' then null
      else a.Neu_PhoneInService end as phoneactivity
----,a.Neu_PhoneInService
,Case when a.Neu_IsCell = 1 then 'C'
when a.Neu_IsCell = 0 then 'L'
else 'U' end as PhoneType
---,a.PhoneType
,ISNULL(a.neu_PhoneCollectabilityScore,'N') as Contactability
,a.PhoneNumber
,CAST(a.NeustarScore AS VARCHAR(10)) AS NeustarScore
,b.ListDate
,b.InitialBalance
,b.PaidOnAccountAmt
,b.TUScore
,b.ClientId
,b.ConsumerId
---,b.CustomerId
,b.CancelDate
,c.[PortFolios] as portfolios
,c.[Site] as calling_Site
FROM dw_mstr_dm.[dbo].[RadiusPhone] (NOLOCK) a
INNER JOIN 
dw_mstr_dm.[dbo].[DimCustomer] (NOLOCK) b
ON
--a.CustomerID = b.CustomerId
--AND
 b.SourceSystem = 'Amex Latitude'
AND a.KeyCustomer = b.KeyCustomer

left join [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_PortFolios_Site_Mapping] as c 
 on 
 rtrim(ltrim(b.[ClientId])) = rtrim(ltrim(c.[RCodes]))

WHERE
a.NeustarScore IS NOT NULL
---AND b.SourceSystem = 'Amex Latitude'
AND 
(
b.CancelDate IS NULL 
OR b.CancelDate > @start_dt)
AND b.ListDate <> '9999-12-31'
AND b.StatusCode <> 'dw_deactivate'
) AS A1

---DROP TABLE #TMP_Amex_Neustar_Scored_All
---SELECT CustomerID, ListDate, Count(1) FROM #TMP_Amex_Neustar_Scored_All GROUP BY CustomerID, ListDate order by listdate, customerid

CREATE INDEX IX_PhoneNumber
ON #TMP_Amex_Neustar_Scored_All (PhoneNumber)

CREATE INDEX IX_KeyCustomer
ON #TMP_Amex_Neustar_Scored_All (KeyCustomer)

CREATE INDEX IX_CustomerID
ON #TMP_Amex_Neustar_Scored_All (CustomerID)

-------DECLARE @start_dt datetime = '2021-06-01 00:00:00';
-------declare @end_dt datetime = '2021-06-30 23:59:59';	

-----DROP TABLE	IF EXISTS #TMP_Scored_Attempt_Data	
IF OBJECT_ID('tempdb..#TMP_Scored_Attempt_Data') IS NOT NULL
		DROP TABLE #TMP_Scored_Attempt_Data

SELECT * INTO #TMP_Scored_Attempt_Data FROM
(
select 
 a.Phone_Dialed
,b.KeyCustomer
,a.Call_Date 
,a.Call_Connect_Time_CT
,a.Result_Id
,a.Livevox_Result
,a.Custom_Outcome_1
,a.Account_Number

	from dw_mstr_dm.[dbo].[RadiusCall] (nolock) a
	JOIN DW_MSTR_DM.dbo.FactCustomerCall (nolock) b
	ON a.Session_Id = b.SessionId
	WHERE
	-- a.Call_Connect_Time_CT  >= @start_dt
	--and a.Call_Connect_Time_CT  < @end_dt 
	a.Call_Date >= @start_dt
	and a.Call_Date  <= @end_dt 
	AND a.LV_Client_Name = 'Veldos'
	AND a.Livevox_Result NOT IN ('SMS MT Failed', 'SMS MT Delivered')
 ) AS A2

---- SELECT  COUNT(1) FROM #TMP_Scored_Attempt_Data WHERE CAST(Call_Connect_Time_CT AS TIME) > '23:07:00.000'

CREATE INDEX IX_Phone_Dialed
ON #TMP_Scored_Attempt_Data (Phone_Dialed)

CREATE INDEX IX_KeyCustomer
ON #TMP_Scored_Attempt_Data (KeyCustomer)

CREATE INDEX IX_Account_Number
ON #TMP_Scored_Attempt_Data (Account_Number)

-----DECLARE @start_dt datetime = '2021-06-01 00:00:00';
-----declare @end_dt datetime = '2021-06-30 23:59:59';

-----DROP TABLE IF EXISTS #TMP_Amex_Neustar_Scored_All_02
IF OBJECT_ID('tempdb..#TMP_Amex_Neustar_Scored_All_02') IS NOT NULL
		DROP TABLE #TMP_Amex_Neustar_Scored_All_02

 SELECT * INTO #TMP_Amex_Neustar_Scored_All_02  FROM
(
 select 
a.*
---,b.Call_Date
,b.[Result_Id]
,rtrim(ltrim(b.[Livevox_Result])) as ClientResult
,b.Phone_Dialed as LV_phoneNumber
,b.Account_Number as LV_File_Number
,c.[Description] as DialerResult_Description
,(case 
when upper(rtrim(ltrim(b.[Livevox_Result]))) in 
(
'AGENT - CUST RPC 7',
'AGENT - CUST RPC 8',
'AGENT - CUST RPC 13',
'AGENT - CUST RPC 14',
'AGENT - DEBTOR DISPUTE',
'AGENT - PTP CREDIT CARD',
'AGENT - PTP DIRECT CHECK',
'AGENT - ATTORNEY HANDLING',
'AGENT - BANKRUPT',
'AGENT - CUST RPC 11',
'AGENT - CUST RPC 12',
'AGENT - CUST RPC PTP 1',
'AGENT - CUST RPC PTP 2',
'AGENT - CUST RPC PTP 5',
'AGENT - CUSTOMER HUNG UP',
'AGENT - CUST RPC 1',
'AGENT - CUST RPC 2',
'AGENT - CUST RPC 3',
'AGENT - CUST RPC 4',
'AGENT - CUST RPC 5',
'AGENT - CUST RPC 6',
'AGENT - CUST RPC 9',
'AGENT - CUST RPC 10'

) then 1 else 0 end )
as RPC_Ind
,(case when  upper(rtrim(ltrim([b].[Livevox_Result])))  in ('AGENT - WRONG NUMBER') then 1 else 0
end) as WPC_Ind
---,b.Call_Connect_Time_CT as CallStart
,b.Call_Date AS CallStart
,b.Call_Connect_Time_CT

from #TMP_Amex_Neustar_Scored_All  as a
left join 
#TMP_Scored_Attempt_data as b 
on
(
ltrim(rtrim(a.PhoneNumber)) = b.Phone_Dialed
and
LTRIM(rtrim(a.CustomerID)) = b.[Account_Number]
---a.KeyCustomer = b.KeyCustomer
)
left join [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_result] as c (nolock)
on
CAST(b.[Result_Id] AS varchar) = c.[code]
WHERE
--- b.Call_Connect_Time_CT  >= @start_dt
---	and b.Call_Connect_Time_CT  < @end_dt
b.Call_Date  >= @start_dt
and b.Call_Date  <= @end_dt
-----------------AND CAST(Call_Connect_Time_CT AS TIME) BETWEEN '00:00:00.000' AND '23:08:00.000'
)	AS A3
---SELECT * FROM #TMP_Amex_Neustar_Scored_All_02 


update  #TMP_Amex_Neustar_Scored_All_02
set [neustarscore] = 'Control'
where
cast( substring([ConsumerId],8,2) as int) > 89

update  #TMP_Amex_Neustar_Scored_All_02
set calling_Site = 'Unmapped'
where calling_Site is NULL

-------------DELETE the existing records from [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Summarized] for same DateRange------------------------------------------------------------------
DELETE FROM [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Summarized]
WHERE [Date Range] = CONCAT(CONVERT(VARCHAR,@start_dt, 110), ' through ',CONVERT(VARCHAR, @end_dt, 110))
---AND [Insert Date] =   CAST(GETDATE() AS DATE)

-------------Insert Phone Inventory Data into [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Summarized]-------------------------------------------------------------------------------------
INSERT INTO [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Summarized]

---------------------------- checking the phone activity for the Amex legal portfolios --------------

select 
neustarscore
----,a.calling_Site
---,a.Call_Date
,Portfolios
---,case when ltrim(z.PhoneActivity)='' then null
---      else z.PhoneActivity end as phoneactivity
,a.phoneactivity
,a.PhoneType as PhoneType
---,ISNULL(z.Contactability,'N') as Contactability
,a.Contactability
,a.InitialBalance as Balance                            
,count(a.Result_Id) as Cnt_Attempt
,count(distinct A.PhoneNumber) as Score_phone
,sum(cast(A.RPC_Ind as int )) as total_RPC
,sum(cast(A.WPC_Ind as int )) as total_WPC
,count(B.first_attempt_RPC) as Unq_RPC
,count(C.first_attempt_RPC) as Unq_WPC

,CASE WHEN a.InitialBalance <= 2500 THEN '0 to 2.5k'
		  WHEN a.InitialBalance >= 5000 THEN '5k & Above'
		  ELSE '2.5k to 5k'
		  END AS [Balance Range]
,CONCAT(CONVERT(VARCHAR,@start_dt, 110), ' through ',CONVERT(VARCHAR, @end_dt, 110)) AS [Date Range]
,GETDATE() AS [Insert Date]
,CASE WHEN (a.calling_Site  <> 'AXP Legal' and a.ClientId Not in ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' ))   THEN 'Recovery'             -----------Recovery
WHEN (a.calling_Site  <> 'AXP Legal' and a.ClientId  in ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' ))      THEN 'RLK Series'                 -----------RLK series
WHEN (a.calling_Site  = 'AXP Legal')     THEN 'AXP Legal' ELSE 'Unknown' END AS [Segment]                                               -----------AXP Legal
,a.ClientId                                           
,'Weekly' AS [Frequency]
---,'Monthly' AS [Frequency]

from #TMP_Amex_Neustar_Scored_All_02 as a 
-----left join [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_PhoneActivity-Contactability] as z 
------on
------(A.phoneNumber = z.phonenumber
------and 
------A.CUSTOMERID = z.file_number)
left join 
(select 
 phonenumber 
,CustomerID
,min(A.Call_Connect_Time_CT) as first_attempt_RPC
FROM  #TMP_Amex_Neustar_Scored_All_02  as A
GROUP BY phoneNumber,CustomerID, RPC_IND
having RPC_Ind = 1
)  as B
on
(A.phoneNumber = B.phonenumber
and 
A.CustomerID = B.CustomerID
and 
A.Call_Connect_Time_CT =b.first_attempt_RPC
)
left join 
(select 
 phonenumber 
,A.CustomerID
,min(A.Call_Connect_Time_CT) as first_attempt_RPC
FROM  #TMP_Amex_Neustar_Scored_All_02  as A
GROUP BY phoneNumber,A.CustomerID,WPC_IND
having WPC_Ind = 1
)  as c
on
(A.phoneNumber = c.phonenumber
and 
A.CustomerID = c.CustomerID
and 
A.Call_Connect_Time_CT=c.first_attempt_RPC
)
-- NEED TO UNCOMMENT THIS CONDITION FOR EXCLUDING  AMEX LEGAL CONDITIONING
---where 
---(site  = 'AXP Legal') --and 
---ClientId  in ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' )
--)
-----------where 
-----------(a.calling_Site  != 'AXP Legal' and a.ClientId  in ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' ))
group by 
neustarscore
---,a.calling_Site
---,a.Call_Date
,Portfolios
---,case when ltrim(z.PhoneActivity)='' then null
---      else z.PhoneActivity end
,a.phoneactivity
,a.PhoneType
---,z.Contactability
,a.Contactability
,a.InitialBalance                      
, CASE WHEN (a.calling_Site  <> 'AXP Legal' and a.ClientId Not in ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' ))   THEN 'Recovery'              -----------Recovery
WHEN (a.calling_Site  <> 'AXP Legal' and a.ClientId  in ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' ))      THEN 'RLK Series'                 -----------RLK series
WHEN (a.calling_Site  = 'AXP Legal')     THEN 'AXP Legal' ELSE 'Unknown' END                                                            -----------AXP Legal
,a.ClientId
order by 
--site,
NeustarScore

-------------DELETE the existing records from [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Active_Account_Phone_Inventory_Summarized] for same DateRange------------------------------------------------------------------
DELETE FROM [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Active_Account_Phone_Inventory_Summarized]
WHERE [Date Range] = CONCAT(CONVERT(VARCHAR,@start_dt, 110), ' through ',CONVERT(VARCHAR, @end_dt, 110))
---AND [Insert Date] =   CAST(GETDATE() AS DATE)

-------------Insert Phone Inventory Data into [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Active_Account_Phone_Inventory_Summarized]-------------------------------------------------------------------------------------
INSERT INTO [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_Active_Account_Phone_Inventory_Summarized]
SELECT CASE WHEN LTRIM(a.Neu_PhoneInService)='' THEN NULL
      ELSE a.Neu_PhoneInService END AS PhoneActivity 
       ---  , pa.PhoneType
	   , nsa.PhoneType
	   ,a.neu_PhoneCollectabilityScore AS Contactability
       ,nsa.NeustarScore 
         , psm.PortFolios
		 ,nsa.InitialBalance     AS Balance           
         , COUNT(*) AS Phones
		 ,CASE WHEN nsa.InitialBalance <= 2500 THEN '0 to 2.5k'
		  WHEN nsa.InitialBalance >= 5000 THEN '5k & Above'
		  ELSE '2.5k to 5k'
		  END AS [Balance Range]
		 ,CONCAT(CONVERT(VARCHAR,@start_dt, 110), ' through ',CONVERT(VARCHAR, @end_dt, 110)) AS [Date Range]
		 , GETDATE()  AS [Insert Date]
,CASE WHEN (nsa.calling_Site  <> 'AXP Legal' AND nsa.ClientId NOT IN ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' ))   THEN 'Recovery'             -----------Recovery
WHEN (nsa.calling_Site  <> 'AXP Legal' AND nsa.ClientId  IN ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' ))      THEN 'RLK Series'                 -----------RLK series
WHEN (nsa.calling_Site  = 'AXP Legal')     THEN 'AXP Legal' ELSE 'Unknown' END AS [Segment]                                                 -----------AXP Legal
,nsa.ClientId
,'Weekly' AS [Frequency]
---,'Monthly' AS [Frequency]

FROM dw_mstr_dm.[dbo].[RadiusPhone] (NOLOCK) a
INNER JOIN 
dw_mstr_dm.[dbo].[DimCustomer] (NOLOCK) b
ON
--a.CustomerID = b.CustomerId
--AND
 b.SourceSystem = 'Amex Latitude'
AND a.KeyCustomer = b.KeyCustomer
INNER JOIN
[CLIENT_ANALYTICS].[dbo].[Amex_Neustar_PortFolios_Site_Mapping] (NOLOCK) psm 
ON b.ClientId = psm.RCodes
LEFT JOIN [CLIENT_ANALYTICS].[dbo].[Amex_Neustar_PortFolios_Site_Mapping] AS c 
 ON  RTRIM(LTRIM(b.[ClientId])) = RTRIM(LTRIM(c.[RCodes]))

LEFT JOIN #TMP_Amex_Neustar_Scored_All_02 nsa 
ON nsa.CustomerId= b.CustomerId 
AND nsa.PhoneNumber= a.PhoneNumber
------AND nsa.KeyCustomer = b.KeyCustomer
WHERE 
a.NeustarScore IS NOT NULL
 AND b.CancelDate IS NULL
----------------------------------------------------------------and b. qlevel < '998'
--(coalesce(b.closed,b.returned) is null )
----------------------------------------------------------------AND (coalesce(b.chargeoffdate,b.CancelDate) is null )
---------------------------------and psm.PortFolios like 'I-coll%'
AND nsa.NeustarScore IS NOT NULL
AND a.Neu_PhoneCollectabilityScore IS NOT NULL
AND b.ListDate <> '9999-12-31'
AND b.StatusCode <> 'dw_deactivate'


GROUP BY CASE WHEN LTRIM(a.Neu_PhoneInService)='' THEN NULL
      ELSE a.Neu_PhoneInService END
        --- , pa.PhoneType
		,nsa.PhoneType
		,a.neu_PhoneCollectabilityScore
         , nsa.NeustarScore
         , psm.PortFolios
		 ,nsa.InitialBalance   
		--- ,CASE WHEN nsa.Plcd_balance <= 2500 THEN '0 to 2.5k'
		---  WHEN nsa.Plcd_balance >= 5000 THEN '5k & Above'
		---  ELSE '2.5k to 5k'
		---  END  
		,CASE WHEN (nsa.calling_Site  <> 'AXP Legal' AND nsa.ClientId NOT IN ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' ))   THEN 'Recovery'                  -----------Recovery
WHEN (nsa.calling_Site  <> 'AXP Legal' AND nsa.ClientId  IN ('RLK1M','RLK2M','RLK3M','RLK4M','RLUJM' ))      THEN 'RLK Series'                              -----------RLK series
WHEN (nsa.calling_Site  = 'AXP Legal')     THEN 'AXP Legal' ELSE 'Unknown' END                                                                           -----------AXP Legal
,nsa.ClientId


END;









GO


