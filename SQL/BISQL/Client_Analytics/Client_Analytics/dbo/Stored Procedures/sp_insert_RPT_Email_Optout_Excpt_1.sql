













-- =============================================
-- Object: dbo.[sp_insert_RPT_Email_Optout_Excpt]
-- Create date: 06/23/2022
--
-- Description: Identify and insert Email OptOut exceptions into RPT_Email_Optout_Excpt_CRM_level_Count and RPT_Email_Optout_Excpt_Customer_level
--
-- History
-- Author Date Description
-- ------------------------------------------------------
-- Parth Dave 06/23/2022 Created
-- =============================================



CREATE PROCEDURE [dbo].[sp_insert_RPT_Email_Optout_Excpt]

@startdatetime datetime = NULL

 ,@end datetime =  NULL

AS



SET NOCOUNT ON;



-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[RPT_Email_Optout_Excpt_CRM_level_Count] for Yesterday------------------------------------------------------------------

DELETE FROM  CLIENT_ANALYTICS.dbo.RPT_Email_Optout_Excpt_CRM_level_Count
WHERE send_date = isnull(@end
                        ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
AND CAST([Insert_Date] AS DATE) =  CAST(GETDATE() AS DATE)

---------Cartesian for yesterday-------------- 

IF OBJECT_ID('tempdb..#t') IS NOT NULL
	  	DROP TABLE #t;

select isnull(@end
                        ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as day, SourceSystem
into #t
from DW_MSTR_DM.dbo.DimSourceSystem (nolock)
where SourceSystem <> 'Other'

-----Remove 'Warmup tes'-----

Select *
into #emailwithoutwarmup
 from [CLIENT_ANALYTICS].[dbo].RPT_email_guid (nolock)
where clientid <> 'Warmup Tes'
and send_date >= isnull(@StartDateTime,GETDATE()) -31 

-------Email Optout exceptions------------

   drop table if exists #exceptions
  
   SELECT 
        b.[send_date]  
       ,b.crm 
	   ,b.clientid
       ,xyz.[CustomerId] 
	   ,b.LTR
	   ,b.email_domain
       ,xyz.[Email]                       
       ,xyz.[OPTOUTdate]
	   ,b.[guid]
	   
   into #exceptions

   from  #emailwithoutwarmup b
   left join DW_MSTR_DM.[dbo].DimSourceSystem d
   on b.crm= d.SourceSystem
  
   LEFT JOIN
   [DW_MSTR_DM].[dbo].[DimCustomerEmail] (nolock)  xyz
   on b.customerid= xyz.CustomerId
   and d.KeySourceSystem = xyz.KeySourceSystem
  and xyz.EmailStatus <> 'Y'
   left join [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup
	on lup.ClientCode= b.ClientId 
	 and (lup.FirstPartyFlag <> 1 
       or lup.FirstPartyFlag is NULL)
  where xyz.OPTOUTdate is not null
   and b.send_date > xyz.OPTOUTdate
   and isnull(b.sent,0) > 0 
	  and b.clientid <> 'Warmup Tes'

   ---and b.send_date >= isnull(@StartDateTime,GETDATE()) -31
   order by b.send_date
	  
------------Calculate count of exceptions at CRM level-----

drop table if exists #groupby_crm

select 
        send_date 
       ,crm
	   , count(*) as count_exceptions

into #groupby_crm

	   from #exceptions
	   group by crm, send_date


-------------checking data on email sent for yesterday----

drop table if exists #emails_sent_for_yesterday

select send_date, crm, sum(sent) as emails_sent
into #emails_sent_for_yesterday
from 
[CLIENT_ANALYTICS].dbo.RPT_email_guid (nolock)
where send_date= cast(isnull(@end
                                     ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as date)
and clientid <> 'Warmup Tes'
group by send_date,crm


------------add Email optout excpetions in new table for reporting at CRM level------

insert into CLIENT_ANALYTICS.dbo.RPT_Email_Optout_Excpt_CRM_level_Count

select send_date, sourcesystem, count_of_exceptions, Insert_Date

from 
(
select #t.day send_date, #t.SourceSystem, isnull(sum(e.count_exceptions), 0) as count_of_exceptions,
GETDATE() as Insert_Date,
c.emails_sent
from #t

left join #groupby_crm e
on cast(e.send_Date as date)= #t.day 
and e.crm=#t.SourceSystem
left join
#emails_sent_for_yesterday c
on #t.day=CAST(c.send_date as date)
and #t.SourceSystem=c.crm
group by #t.day, #t.SourceSystem, c.emails_sent
) f
where isnull(f.emails_sent,0) > 0

--------add email optout exceptions data into new table at detailed level-------

insert into client_analytics.dbo.RPT_Email_Optout_Excpt_Customer_level 
(       [send_date]  
       ,crm 
	   ,clientid
       ,[CustomerId] 
	   ,LTR
	   ,email_domain
       ,[Email_ID]                         
       ,[OPTOUTdate]
	   ,[guid]
	   )

select 
        exc.[send_date]  
       ,exc.crm 
	   ,exc.clientid
       ,exc.[CustomerId] 
	   ,exc.LTR
	   ,exc.email_domain
       ,exc.[Email]                        
       ,exc.[OPTOUTdate]
	   ,exc.[guid]

from  #exceptions exc
left join client_analytics.dbo.RPT_Email_Optout_Excpt_Customer_level excl (nolock)
on  exc.[send_date] = excl.send_date
       and exc.crm = excl.crm
	   and exc.clientid = excl.clientid
       and exc.[CustomerId] = excl.CustomerId
	   and exc.LTR = excl.LTR
	   and exc.email_domain = excl.email_domain
       and exc.[Email] = excl.email_id               
       and exc.[OPTOUTdate] = excl.OPTOUTdate
	   and exc.[guid] = excl.[guid]
       where excl.CustomerId is null
       and excl.[guid] is null
	   order by exc.[send_date]