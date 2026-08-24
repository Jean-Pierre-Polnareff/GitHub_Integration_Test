USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_insert_fact_letter_excpt_compliance_check]    Script Date: 12/5/2022 11:50:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


ALTER procedure [dbo].[sp_insert_fact_letter_excpt_compliance_check]

 @startdate datetime = NULL,

 @end datetime =  Null

 as 

BEGIN
	SET NOCOUNT ON;

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_letter_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------

DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_letter_excpt_CRM_level_count]
WHERE contact_date = ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND ltr_expt_id  = 1
AND KeySourceSystem in (1,2)


--------------Cartesian product of ltr_expt_id and KeySourceSystem for all time---------------------------------------

IF OBJECT_ID('tempdb..#t') IS NOT NULL
		DROP TABLE #t;
SELECT ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS day, dss.keysourcesystem
, dle.ltr_expt_id 
INTO #t 
FROM DW_MSTR_DM.dbo.DimSourceSystem dss 
CROSS JOIN 
CLIENT_ANALYTICS.dbo.dim_letter_excpt dle
WHERE 
dle.ltr_expt_id  = 1
AND 
dss.KeySourceSystem in (1,2)
AND dle.all_client_flag = 0

---1) Find all calls/keycustomers with call that's RPC or Left Message for Artiva

		drop table if exists #calls_raw  
  
SELECT            
			  --cust.ClientId    
			  fct.KeyCustomer 
			, dss.KeySourceSystem
			, rc.Call_Date
			, fct.IsRPC
			, rc.Livevox_Result			  
INTO #calls_raw    
FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
	INNER JOIN DW_MSTR_DM.dbo.RadiusCall rc (nolock) on fct.SessionId = rc.Session_Id 
	INNER JOIN DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem = dss.KeySourceSystem
WHERE   
        rc.Call_Date >= isnull(@StartDate,GETDATE()) -7  
		and rc.Call_Date >= isnull(@StartDate,GETDATE()) -14    
	AND (fct.IsRPC = 1 or rc.Livevox_Result LIKE '%Left Message%') 
	AND rc.Service_Name NOT LIKE '%HTI%'
	AND dss.KeySourceSystem IN (1,2) ----add keysourcesystem = 3 for Amex data


	drop table if exists #calls
   
select        cust.ClientId     
			, C.KeyCustomer
			, C.KeySourceSystem
			, C.Call_Date
			, C.IsRPC
			, C.Livevox_Result
			, dcl.ClientParent 
			, cust.consumerid
			, cust.initialbalance
			, cust.listdate
			, cust.sourcesystem
into #calls
from #calls_raw c 
	INNER JOIN DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK) ON c.KeyCustomer = cust.KeyCustomer AND cust.StatusCode <> 'DW_deactivate'
	LEFT OUTER JOIN DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId = dcl.ClientId AND cust.SourceSystem = dcl.SourceSystem	
WHERE cust.ListDate >= @StartDate   
	AND dcl.ClientId NOT LIKE 'DC%P' 
	AND dcl.ClientId NOT IN ('SNBCEP', 'EMPBSP','EMPCMF','EMPIFP')  
	

-----First RPC Calls and left message ever  -------

DROP TABLE	IF EXISTS  #first_ever_call

SELECT           
			  c.ClientId
			, c.KeyCustomer
			, fct.KeySourceSystem
			, min(rc.Call_Date) as First_Ever_Call
			, c.ClientParent
			, c.consumerid
			, c.initialbalance	
			, c.sourcesystem
							
			INTO #first_ever_call 
	
	 FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
			  inner join
		 DW_MSTR_DM.dbo.RadiusCall rc (nolock) on fct.SessionId = rc.Session_Id 
		 and rc.Service_Name not like '%HTI%'
		 and (fct.IsRPC = 1 or rc.Livevox_Result like '%Left Message%')     
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem 
		 inner join
		 #calls c on fct.KeyCustomer = c.KeyCustomer 		 	
		group by
		       c.ClientId 
			 , c.KeyCustomer
			 , fct.KeySourceSystem
			 , c.ClientParent
			 , c.consumerid
			 , c.initialbalance	
			 , c.sourcesystem

------2) list of Keycustomers calls now be first ever calls made to the customer from 7/1/22-------

drop table if exists #calls2
select        fc.KeyCustomer
            , fc.KeySourceSystem
            , fc.First_Ever_Call
            , fc.ClientId
            , fc.ClientParent
            , fc.consumerid
			, fc.initialbalance	
			, fc.sourcesystem
into #calls2
from #first_ever_call fc
inner join #calls c on c.KeyCustomer = fc.KeyCustomer
and c.Call_Date = fc.First_Ever_Call
and c.KeySourceSystem = fc.KeySourceSystem


-------3) Of remaining keycustomers, only keep the ones that didn't SIF/PIF within 5 days
---a. (paidamount-initial_balance=0 or sifamt>0) and lastpaymentdate within 5 days of first call date from #calls2

drop table if exists #calls3

select 
               c2.keycustomer
             , dc.customerid
			 , dc.StatusCode
             , dc.ClientId
			 , dc.CurrentBalance as Balance        
			 , c2.ClientParent
             , c2.KeySourceSystem
			 , c2.First_Ever_Call
			 , Name_on_Account = CONCAT(dc.FirstName,' ' ,dc.LastName) 
             , dc.ListDate
			 , Is_PIF = CASE WHEN dc.InitialBalance = dc.PaidOnAccountAmt
               THEN 1 ELSE 0 END
			 , Is_SIF = CASE WHEN dc.SIFAmt is not null then 1 
			           when dc.SIFAmt > 0 then 1
					   else 0
					   end 
			, dc.LastPaymentDate
			, c2.consumerid
			, c2.initialbalance	
			, c2.sourcesystem

			 into #calls3 

from #calls2 c2
inner join dw_mstr_dm.dbo.DimSourceSystem (nolock) dss
on c2.KeySourceSystem = dss.KeySourceSystem
left join [DW_MSTR_DM].[dbo].[DimCustomer] dc (nolock)
on c2.KeyCustomer = dc.KeyCustomer

-------No PIF and SIF within 5 days from #calls3----

delete from #calls3 
where is_pif + is_sif >= 1 
and datediff(day, First_Ever_Call, LastPaymentDate) <= 5

---4) First ever IDN letters sent to customers from #calls3-----

drop table if exists #idl
select              
               c3.keycustomer
			 , dcu.KeyCustomer as keycustomer_2
			 , c3.SourceSystem
			 , min(dcu.idl_date) as Letter_Sent_Date

into #idl     

from #calls3 c3
        inner join
	 dw_mstr_dm.dbo.dimcustomer dcu (nolock) on c3.consumerid = dcu.consumerid
	                                        and c3.clientid = dcu.clientid
	                                        and c3.sourcesystem = dcu.sourcesystem

  group by
                 c3.keycustomer
			   , dcu.KeyCustomer
			   , c3.SourceSystem

 
--------exceptions- No IDL letter sent within 5 days of call made to customer-------------

drop table if exists #exceptions

select   ltr_expt_id = 1
       , c3.customerid
       , c3.ConsumerId
       , c3.StatusCode
	   , c3.ListDate
	   , c3.ClientId
	   , c3.ClientParent as Client_Name
	   , c3.First_Ever_Call as Contact_Date
	   , c3.Name_on_Account
	   , c3.Balance
	   , c3.Is_PIF
	   , c3.Is_SIF
	   , c3.LastPaymentDate
	   , idl.Letter_Sent_Date 
	   , dss.SourceSystem
	   , c3.KeySourceSystem

	   into #exceptions

from #calls3 c3 
left join  DW_MSTR_DM.dbo.DimSourceSystem dss
on c3.KeySourceSystem = dss.KeySourceSystem
left join #idl idl
on idl.KeyCustomer = c3.KeyCustomer
where (datediff(day, c3.First_Ever_Call, idl.Letter_Sent_Date) > 5 OR idl.letter_sent_date is null)
---removed test customerids
and not (c3.CustomerId in ('11644','1224851','1687','1028999','14892599') and c3.KeySourceSystem in (1,2,3))

/*
union

----exceptions- no letter send within 45 days of IDL letter sent----

drop table if exists #exceptions

select         ltr_expt_id = 2
             , c3.customerid
             , c3.ConsumerId
             , c3.StatusCode
			 , c3.ListDate
	         , c3.ClientId
			 , c3.ClientParent as Client_Name
	         , c3.First_Ever_Call as Contact_Date
			 , c3.Name_on_Account
	         , Balance = NULL
	         , Is_PIF = NULL
	         , Is_SIF = NULL
	         , LastPaymentDate = NULL
	         , idl.Letter_Sent_Date 
			 , idl.CalendarDate_lead1 as Next_LTR_Sent_Date
             , idl.LetterType
             , idl.LetterTypeDescription
			 , dss.SourceSystem
	         , c3.KeySourceSystem
			 
into #exceptions

from #calls3 c3
left join DW_MSTR_DM.dbo.DimSourceSystem dss
on c3.KeySourceSystem = dss.KeySourceSystem
left join #idl idl
on idl.KeyCustomer = c3.KeyCustomer
where idl.LetterType not in
('CM001','RS001','RS002','RS005','RS007','RSNY1')
and idl.LetterType not like 'IDN%'
and datediff(day,idl.Letter_Sent_Date, idl.CalendarDate_lead1) <= 45
and not (c3.CustomerId in ('11644','1224851','1687','1028999','14892599') and c3.KeySourceSystem in (1,2,3))

*/

-----------Adding 0's into CLIENT_ANALYTICS.[dbo].[fact_letter_excpt_CRM_level_count] for all time----------------------------

INSERT INTO CLIENT_ANALYTICS.[dbo].[fact_letter_excpt_CRM_level_count]

(contact_date
,keysourcesystem
,ltr_expt_id
,Count_of_Exceptions
,Insert_Date)

SELECT contact_date
,keysourcesystem
,ltr_expt_id
,Count_of_Exceptions
,Insert_Date

FROM
(
SELECT #t.day AS contact_date
,#t.keysourcesystem AS keysourcesystem
,#t.ltr_expt_id AS ltr_expt_id
,ISNULL(SUM(E.Count_of_Exceptions),0) AS Count_of_Exceptions
,GETDATE() AS Insert_Date
,c.No_of_Calls
FROM #t

LEFT JOIN 
(SELECT CAST(exc.contact_date AS DATE) contact_date
,exc.keysourcesystem
,exc.ltr_expt_id
,COUNT(*) AS Count_of_Exceptions
FROM #exceptions exc 
group by CAST(exc.contact_date AS DATE)
,exc.keysourcesystem
,exc.ltr_expt_id
) E

ON CAST(e.contact_date AS DATE) = #t.day
	AND e.keysourcesystem = #t.keysourcesystem
	AND e.ltr_expt_id = #t.ltr_expt_id

LEFT JOIN 
(
SELECT 
dt.CalendarDate AS calldate
, fct.KeySourceSystem
, COUNT(*) AS No_of_Calls 
FROM  DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
--inner join
--		 DW_MSTR_DM.dbo.RadiusCall rc (nolock) on fct.SessionId=rc.Session_Id 
--		 and rc.Call_Date >= @StartDate
inner join
DW_MSTR_DM.dbo.DimDate dt (NOLOCK) 
ON fct.KeyDate_CallDate = dt.KeyDate
WHERE dt.CalendarDate  between @startdate and getdate()-1
---and (fct.IsRPC = 1 or rc.Livevox_Result like '%Left Message%')
----and rc.Service_Name not like '%HTI%'
GROUP BY dt.CalendarDate  , fct.KeySourceSystem 
)c
ON #t.day = CAST(c.calldate AS DATE) 
	AND #t.keysourcesystem = c.KeySourceSystem

GROUP BY 
#t.day 
,#t.keysourcesystem
,#t.ltr_expt_id   
,c.No_of_Calls
)f
WHERE ISNULL(f.No_of_Calls,0) > 0

------insert data into table CLIENT_ANALYTICS.dbo.fact_Letter_excpt at customer level-----

INSERT INTO CLIENT_ANALYTICS.dbo.fact_Letter_excpt
(
               ltr_expt_id 
             , CUSTOMER_ID
             , ConsumerId
             , Status_Code
			 , List_Date
	         , CLIENT_ID
			 , Client_Name
			 , KeySourceSystem
			 , Sourcesystem
	         , Contact_Date
			 , Name_on_Account
	         , Balance 
	         , Is_SIF 
	         , Is_PIF 
	         , LastPaymentDate 
	         , Letter_Sent_Date 
			 , insert_date			 		
		   )
	
	SELECT   
	           exc.ltr_expt_id 
             , exc.customerid
             , exc.ConsumerId
             , exc.StatusCode
			 , exc.ListDate
	         , exc.ClientId
			 , exc.Client_Name
			 , exc.KeySourceSystem
			 , exc.SourceSystem
	         , exc.Contact_Date
			 , exc.Name_on_Account
	         , exc.Balance 
	         , exc.Is_SIF 
	         , exc.Is_PIF 
	         , exc.LastPaymentDate 
	         , exc.letter_sent_date 
			 , insert_date = getdate()
	        		   
	FROM #exceptions exc
	

END;



GO


