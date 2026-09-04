USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_insert_fact_dial_excpt_client]    Script Date: 10/28/2022 3:48:02 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






--SET QUOTED_IDENTIFIER ON
--GO



--CREATE PROCEDURE  [dbo].[sp_insert_fact_dial_excpt_client]
ALTER PROCEDURE  [dbo].[sp_insert_fact_dial_excpt_client]
    
	 @StartDateTime DATETIME = NULL,
	 @end datetime = NULL
	
AS
/*
Object: sp_insert_fact_dial_excpt_client

Description: Identify and insert dialer exceptions for Amex new rules into fact_dial_excpt

Author			Date		Description
Amod Ramugade	11/24/2021	Created
Amod Ramugade   08/15/2022  Modified
Amod Ramugade   10/28/2022  Modified
*/

BEGIN
	SET NOCOUNT ON;

	---DECLARE  @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
WHERE calldate = ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id  IN (11,12,13,22,23,24,25,26,27,28) 
AND KeySourceSystem = 3
---AND all_client_flag = 0

-----------------------------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------
	IF OBJECT_ID('tempdb..#t') IS NOT NULL
		DROP TABLE #t;
SELECT ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS day, dss.keysourcesystem
, dde.dlr_expt_id 
INTO #t 
FROM DW_MSTR_DM.dbo.DimSourceSystem dss 
CROSS JOIN 
CLIENT_ANALYTICS.dbo.dim_dial_excpt dde
WHERE 
dde.dlr_expt_id  IN (11,12,13,22,23,24,25,26,27,28) 
AND 
dss.KeySourceSystem = 3
AND dde.all_client_flag = 0

----------------------------------------- Area codes and Zip Codes---------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;

	with areacode					--not used now, but leaving in case need in the future
	as
	(
		select 702 as AreaCode, 'NV' as StateCode union
		select 725, 'NV' union
		select 775, 'NV' union
		select 203, 'CT' union
		select 475, 'CT' union
		select 860, 'CT' union
		select 959, 'CT' union
		select 202, 'DC'
	),

	zip_state
	as
	(
		select 'NV' as customerstate, '88901' as zip union
SELECT 'NV', '88905' union	
SELECT 'NV', '89001' union	
SELECT 'NV', '89002' union	
SELECT 'NV', '89003' union	
SELECT 'NV', '89004' union	
SELECT 'NV', '89005' union	
SELECT 'NV', '89006' union	
SELECT 'NV', '89007' union	
SELECT 'NV', '89008' union	
SELECT 'NV', '89009' union	
SELECT 'NV', '89010' union	
SELECT 'NV', '89011' union	
SELECT 'NV', '89012' union	
SELECT 'NV', '89013' union	
SELECT 'NV', '89014' union	
SELECT 'NV', '89015' union	
SELECT 'NV', '89016' union	
SELECT 'NV', '89017' union	
SELECT 'NV', '89018' union	
SELECT 'NV', '89019' union	
SELECT 'NV', '89020' union	
SELECT 'NV', '89021' union	
SELECT 'NV', '89022' union	
SELECT 'NV', '89023' union	
SELECT 'NV', '89024' union	
SELECT 'NV', '89025' union	
SELECT 'NV', '89026' union	
SELECT 'NV', '89027' union	
SELECT 'NV', '89028' union	
SELECT 'NV', '89029' union	
SELECT 'NV', '89030' union	
SELECT 'NV', '89031' union	
SELECT 'NV', '89032' union	
SELECT 'NV', '89033' union	
SELECT 'NV', '89034' union	
SELECT 'NV', '89036' union	
SELECT 'NV', '89037' union	
SELECT 'NV', '89039' union	
SELECT 'NV', '89040' union	
SELECT 'NV', '89041' union	
SELECT 'NV', '89042' union	
SELECT 'NV', '89043' union	
SELECT 'NV', '89044' union	
SELECT 'NV', '89045' union	
SELECT 'NV', '89046' union	
SELECT 'NV', '89047' union	
SELECT 'NV', '89048' union	
SELECT 'NV', '89049' union	
SELECT 'NV', '89052' union	
SELECT 'NV', '89053' union	
SELECT 'NV', '89054' union	
SELECT 'NV', '89060' union	
SELECT 'NV', '89061' union	
SELECT 'NV', '89067' union	
SELECT 'NV', '89070' union	
SELECT 'NV', '89074' union	
SELECT 'NV', '89077' union	
SELECT 'NV', '89081' union	
SELECT 'NV', '89084' union	
SELECT 'NV', '89085' union	
SELECT 'NV', '89086' union	
SELECT 'NV', '89087' union	
SELECT 'NV', '89101' union	
SELECT 'NV', '89102' union	
SELECT 'NV', '89103' union	
SELECT 'NV', '89104' union	
SELECT 'NV', '89105' union	
SELECT 'NV', '89106' union	
SELECT 'NV', '89107' union	
SELECT 'NV', '89108' union	
SELECT 'NV', '89109' union	
SELECT 'NV', '89110' union	
SELECT 'NV', '89111' union	
SELECT 'NV', '89112' union	
SELECT 'NV', '89113' union	
SELECT 'NV', '89114' union	
SELECT 'NV', '89115' union	
SELECT 'NV', '89116' union	
SELECT 'NV', '89117' union	
SELECT 'NV', '89118' union	
SELECT 'NV', '89119' union	
SELECT 'NV', '89120' union	
SELECT 'NV', '89121' union	
SELECT 'NV', '89122' union	
SELECT 'NV', '89123' union	
SELECT 'NV', '89124' union	
SELECT 'NV', '89125' union	
SELECT 'NV', '89126' union	
SELECT 'NV', '89127' union	
SELECT 'NV', '89128' union	
SELECT 'NV', '89129' union	
SELECT 'NV', '89130' union	
SELECT 'NV', '89131' union	
SELECT 'NV', '89132' union	
SELECT 'NV', '89133' union	
SELECT 'NV', '89134' union	
SELECT 'NV', '89135' union	
SELECT 'NV', '89136' union	
SELECT 'NV', '89137' union	
SELECT 'NV', '89138' union	
SELECT 'NV', '89139' union	
SELECT 'NV', '89140' union	
SELECT 'NV', '89141' union	
SELECT 'NV', '89142' union	
SELECT 'NV', '89143' union	
SELECT 'NV', '89144' union	
SELECT 'NV', '89145' union	
SELECT 'NV', '89146' union	
SELECT 'NV', '89147' union	
SELECT 'NV', '89148' union	
SELECT 'NV', '89149' union	
SELECT 'NV', '89150' union	
SELECT 'NV', '89151' union	
SELECT 'NV', '89152' union	
SELECT 'NV', '89153' union	
SELECT 'NV', '89154' union	
SELECT 'NV', '89155' union	
SELECT 'NV', '89156' union	
SELECT 'NV', '89157' union	
SELECT 'NV', '89158' union	
SELECT 'NV', '89159' union	
SELECT 'NV', '89160' union	
SELECT 'NV', '89161' union	
SELECT 'NV', '89162' union	
SELECT 'NV', '89163' union	
SELECT 'NV', '89164' union	
SELECT 'NV', '89165' union	
SELECT 'NV', '89166' union	
SELECT 'NV', '89169' union	
SELECT 'NV', '89170' union	
SELECT 'NV', '89173' union	
SELECT 'NV', '89177' union	
SELECT 'NV', '89178' union	
SELECT 'NV', '89179' union	
SELECT 'NV', '89180' union	
SELECT 'NV', '89183' union	
SELECT 'NV', '89185' union	
SELECT 'NV', '89191' union	
SELECT 'NV', '89193' union	
SELECT 'NV', '89195' union	
SELECT 'NV', '89199' union	
SELECT 'NV', '89301' union	
SELECT 'NV', '89310' union	
SELECT 'NV', '89311' union	
SELECT 'NV', '89314' union	
SELECT 'NV', '89315' union	
SELECT 'NV', '89316' union	
SELECT 'NV', '89317' union	
SELECT 'NV', '89318' union	
SELECT 'NV', '89319' union	
SELECT 'NV', '89402' union	
SELECT 'NV', '89403' union	
SELECT 'NV', '89404' union	
SELECT 'NV', '89405' union	
SELECT 'NV', '89406' union	
SELECT 'NV', '89407' union	
SELECT 'NV', '89408' union	
SELECT 'NV', '89409' union	
SELECT 'NV', '89410' union	
SELECT 'NV', '89411' union	
SELECT 'NV', '89412' union	
SELECT 'NV', '89413' union	
SELECT 'NV', '89414' union	
SELECT 'NV', '89415' union	
SELECT 'NV', '89418' union	
SELECT 'NV', '89419' union	
SELECT 'NV', '89420' union	
SELECT 'NV', '89421' union	
SELECT 'NV', '89422' union	
SELECT 'NV', '89423' union	
SELECT 'NV', '89424' union	
SELECT 'NV', '89425' union	
SELECT 'NV', '89426' union	
SELECT 'NV', '89427' union	
SELECT 'NV', '89428' union	
SELECT 'NV', '89429' union	
SELECT 'NV', '89430' union	
SELECT 'NV', '89431' union	
SELECT 'NV', '89432' union	
SELECT 'NV', '89433' union	
SELECT 'NV', '89434' union	
SELECT 'NV', '89435' union	
SELECT 'NV', '89436' union	
SELECT 'NV', '89437' union	
SELECT 'NV', '89438' union	
SELECT 'NV', '89439' union	
SELECT 'NV', '89440' union	
SELECT 'NV', '89441' union	
SELECT 'NV', '89442' union	
SELECT 'NV', '89444' union	
SELECT 'NV', '89445' union	
SELECT 'NV', '89446' union	
SELECT 'NV', '89447' union	
SELECT 'NV', '89448' union	
SELECT 'NV', '89449' union	
SELECT 'NV', '89450' union	
SELECT 'NV', '89451' union	
SELECT 'NV', '89452' union	
SELECT 'NV', '89460' union	
SELECT 'NV', '89496' union	
SELECT 'NV', '89501' union	
SELECT 'NV', '89502' union	
SELECT 'NV', '89503' union	
SELECT 'NV', '89504' union	
SELECT 'NV', '89505' union	
SELECT 'NV', '89506' union	
SELECT 'NV', '89507' union	
SELECT 'NV', '89508' union	
SELECT 'NV', '89509' union	
SELECT 'NV', '89510' union	
SELECT 'NV', '89511' union	
SELECT 'NV', '89512' union	
SELECT 'NV', '89513' union	
SELECT 'NV', '89515' union	
SELECT 'NV', '89519' union	
SELECT 'NV', '89520' union	
SELECT 'NV', '89521' union	
SELECT 'NV', '89523' union	
SELECT 'NV', '89533' union	
SELECT 'NV', '89555' union	
SELECT 'NV', '89557' union	
SELECT 'NV', '89570' union	
SELECT 'NV', '89595' union	
SELECT 'NV', '89599' union	
SELECT 'NV', '89701' union	
SELECT 'NV', '89702' union	
SELECT 'NV', '89703' union	
SELECT 'NV', '89704' union	
SELECT 'NV', '89705' union	
SELECT 'NV', '89706' union	
SELECT 'NV', '89711' union	
SELECT 'NV', '89712' union	
SELECT 'NV', '89713' union	
SELECT 'NV', '89714' union	
SELECT 'NV', '89721' union	
SELECT 'NV', '89801' union	
SELECT 'NV', '89802' union	
SELECT 'NV', '89803' union	
SELECT 'NV', '89815' union	
SELECT 'NV', '89820' union	
SELECT 'NV', '89821' union	
SELECT 'NV', '89822' union	
SELECT 'NV', '89823' union	
SELECT 'NV', '89825' union	
SELECT 'NV', '89826' union	
SELECT 'NV', '89828' union	
SELECT 'NV', '89830' union	
SELECT 'NV', '89831' union	
SELECT 'NV', '89832' union	
SELECT 'NV', '89833' union	
SELECT 'NV', '89834' union	
SELECT 'NV', '89835' union	
SELECT 'NV', '89883' union	
select 'CT', '06001' union		
select 'CT', '06002' union
select 'CT', '06010' union	
select 'CT', '06011' union	
select 'CT', '06013' union	
select 'CT', '06013' union	
select 'CT', '06016' union	
select 'CT', '06016' union	
select 'CT', '06018' union	
select 'CT', '06019' union	
select 'CT', '06019' union	
select 'CT', '06020' union	
select 'CT', '06021' union	
select 'CT', '06022' union	
select 'CT', '06023' union	
select 'CT', '06024' union	
select 'CT', '06025' union	
select 'CT', '06026' union	
select 'CT', '06027' union	
select 'CT', '06028' union	
select 'CT', '06029' union	
select 'CT', '06031' union	
select 'CT', '06032' union	
select 'CT', '06033' union	
select 'CT', '06034' union	
select 'CT', '06035' union	
select 'CT', '06037' union	
select 'CT', '06037' union	
select 'CT', '06039' union	
select 'CT', '06040' union	
select 'CT', '06043' union	
select 'CT', '06043' union	
select 'CT', '06045' union	
select 'CT', '06049' union	
select 'CT', '06050' union	
select 'CT', '06051' union	
select 'CT', '06052' union	
select 'CT', '06053' union	
select 'CT', '06057' union	
select 'CT', '06058' union	
select 'CT', '06059' union	
select 'CT', '06060' union	
select 'CT', '06061' union	
select 'CT', '06062' union	
select 'CT', '06063' union	
select 'CT', '06063' union	
select 'CT', '06063' union	
select 'CT', '06064' union	
select 'CT', '06065' union	
select 'CT', '06066' union	
select 'CT', '06067' union	
select 'CT', '06068' union
SELECT 'DC', '20001' union
SELECT 'DC', '20002' union
SELECT 'DC', '20003' union
SELECT 'DC', '20004' union
SELECT 'DC', '20005' union
SELECT 'DC', '20006' union
SELECT 'DC', '20007' union
SELECT 'DC', '20008' union
SELECT 'DC', '20009' union
SELECT 'DC', '20010' union
SELECT 'DC', '20011' union
SELECT 'DC', '20012' union
SELECT 'DC', '20013' union
SELECT 'DC', '20015' union
SELECT 'DC', '20016' union
SELECT 'DC', '20017' union
SELECT 'DC', '20018' union
SELECT 'DC', '20019' union
SELECT 'DC', '20020' union
SELECT 'DC', '20022' union
SELECT 'DC', '20023' union
SELECT 'DC', '20024' union
SELECT 'DC', '20026' union
SELECT 'DC', '20027' union
SELECT 'DC', '20029' union
SELECT 'DC', '20030' union
SELECT 'DC', '20032' union
SELECT 'DC', '20033' union
SELECT 'DC', '20035' union
SELECT 'DC', '20036' union
SELECT 'DC', '20037' union
SELECT 'DC', '20038' union
SELECT 'DC', '20039' union
SELECT 'DC', '20040' union
SELECT 'DC', '20041' union
SELECT 'DC', '20042' union
SELECT 'DC', '20043' union
SELECT 'DC', '20044' union
SELECT 'DC', '20045' union
SELECT 'DC', '20046' union
SELECT 'DC', '20047' union
SELECT 'DC', '20049' union
SELECT 'DC', '20050' union
SELECT 'DC', '20051' union
SELECT 'DC', '20052' union
SELECT 'DC', '20053' union
SELECT 'DC', '20055' union
SELECT 'DC', '20056' union
SELECT 'DC', '20057' union
SELECT 'DC', '20058' union
SELECT 'DC', '20059' union
SELECT 'DC', '20060' union
SELECT 'DC', '20061' union
SELECT 'DC', '20062' union
SELECT 'DC', '20063' union
SELECT 'DC', '20064' union
SELECT 'DC', '20065' union
SELECT 'DC', '20066' union
SELECT 'DC', '20067' union
SELECT 'DC', '20068' union
SELECT 'DC', '20069' union
SELECT 'DC', '20070' union
SELECT 'DC', '20071' union
SELECT 'DC', '20073' union
SELECT 'DC', '20074' union
SELECT 'DC', '20075' union
SELECT 'DC', '20076' union
SELECT 'DC', '20077' union
SELECT 'DC', '20078' union
SELECT 'DC', '20080' union
SELECT 'DC', '20081' union
SELECT 'DC', '20082' union
SELECT 'DC', '20088' union
SELECT 'DC', '20090' union
SELECT 'DC', '20091' union
SELECT 'DC', '20097' union
SELECT 'DC', '20098' union
SELECT 'DC', '20201' union
SELECT 'DC', '20202' union
SELECT 'DC', '20203' union
SELECT 'DC', '20204' union
SELECT 'DC', '20206' union
SELECT 'DC', '20207' union
SELECT 'DC', '20208' union
SELECT 'DC', '20210' union
SELECT 'DC', '20211' union
SELECT 'DC', '20212' union
SELECT 'DC', '20213' union
SELECT 'DC', '20214' union
SELECT 'DC', '20215' union
SELECT 'DC', '20216' union
SELECT 'DC', '20217' union
SELECT 'DC', '20218' union
SELECT 'DC', '20219' union
SELECT 'DC', '20220' union
SELECT 'DC', '20221' union
SELECT 'DC', '20222' union
SELECT 'DC', '20223' union
SELECT 'DC', '20224' union
SELECT 'DC', '20226' union
SELECT 'DC', '20227' union
SELECT 'DC', '20228' union
SELECT 'DC', '20229' union
SELECT 'DC', '20230' union
SELECT 'DC', '20232' union
SELECT 'DC', '20233' union
SELECT 'DC', '20235' union
SELECT 'DC', '20237' union
SELECT 'DC', '20238' union
SELECT 'DC', '20239' union
SELECT 'DC', '20240' union
SELECT 'DC', '20241' union
SELECT 'DC', '20242' union
SELECT 'DC', '20244' union
SELECT 'DC', '20245' union
SELECT 'DC', '20250' union
SELECT 'DC', '20251' union
SELECT 'DC', '20252' union
SELECT 'DC', '20254' union
SELECT 'DC', '20260' union
SELECT 'DC', '20261' union
SELECT 'DC', '20262' union
SELECT 'DC', '20265' union
SELECT 'DC', '20266' union
SELECT 'DC', '20268' union
SELECT 'DC', '20270' union
SELECT 'DC', '20277' union
SELECT 'DC', '20289' union
SELECT 'DC', '20299' union
SELECT 'DC', '20301' union
SELECT 'DC', '20303' union
SELECT 'DC', '20306' union
SELECT 'DC', '20307' union
SELECT 'DC', '20310' union
SELECT 'DC', '20314' union
SELECT 'DC', '20317' union
SELECT 'DC', '20318' union
SELECT 'DC', '20319' union
SELECT 'DC', '20330' union
SELECT 'DC', '20340' union
SELECT 'DC', '20350' union
SELECT 'DC', '20355' union
SELECT 'DC', '20370' union
SELECT 'DC', '20372' union
SELECT 'DC', '20373' union
SELECT 'DC', '20374' union
SELECT 'DC', '20375' union
SELECT 'DC', '20376' union
SELECT 'DC', '20380' union
SELECT 'DC', '20388' union
SELECT 'DC', '20389' union
SELECT 'DC', '20390' union
SELECT 'DC', '20391' union
SELECT 'DC', '20392' union
SELECT 'DC', '20393' union
SELECT 'DC', '20394' union
SELECT 'DC', '20395' union
SELECT 'DC', '20398' union
SELECT 'DC', '20401' union
SELECT 'DC', '20402' union
SELECT 'DC', '20403' union
SELECT 'DC', '20404' union
SELECT 'DC', '20405' union
SELECT 'DC', '20406' union
SELECT 'DC', '20407' union
SELECT 'DC', '20408' union
SELECT 'DC', '20409' union
SELECT 'DC', '20410' union
SELECT 'DC', '20411' union
SELECT 'DC', '20412' union
SELECT 'DC', '20413' union
SELECT 'DC', '20414' union
SELECT 'DC', '20415' union
SELECT 'DC', '20416' union
SELECT 'DC', '20417' union
SELECT 'DC', '20418' union
SELECT 'DC', '20419' union
SELECT 'DC', '20420' union
SELECT 'DC', '20421' union
SELECT 'DC', '20422' union
SELECT 'DC', '20423' union
SELECT 'DC', '20424' union
SELECT 'DC', '20425' union
SELECT 'DC', '20426' union
SELECT 'DC', '20427' union
SELECT 'DC', '20428' union
SELECT 'DC', '20429' union
SELECT 'DC', '20431' union
SELECT 'DC', '20433' union
SELECT 'DC', '20434' union
SELECT 'DC', '20435' union
SELECT 'DC', '20436' union
SELECT 'DC', '20437' union
SELECT 'DC', '20439' union
SELECT 'DC', '20440' union
SELECT 'DC', '20441' union
SELECT 'DC', '20442' union
SELECT 'DC', '20444' union
SELECT 'DC', '20447' union
SELECT 'DC', '20451' union
SELECT 'DC', '20453' union
SELECT 'DC', '20456' union
SELECT 'DC', '20460' union
SELECT 'DC', '20463' union
SELECT 'DC', '20468' union
SELECT 'DC', '20469' union
SELECT 'DC', '20470' union
SELECT 'DC', '20472' union
SELECT 'DC', '20500' union
SELECT 'DC', '20501' union
SELECT 'DC', '20502' union
SELECT 'DC', '20503' union
SELECT 'DC', '20504' union
SELECT 'DC', '20505' union
SELECT 'DC', '20506' union
SELECT 'DC', '20507' union
SELECT 'DC', '20508' union
SELECT 'DC', '20509' union
SELECT 'DC', '20510' union
SELECT 'DC', '20511' union
SELECT 'DC', '20515' union
SELECT 'DC', '20520' union
SELECT 'DC', '20521' union
SELECT 'DC', '20522' union
SELECT 'DC', '20523' union
SELECT 'DC', '20524' union
SELECT 'DC', '20525' union
SELECT 'DC', '20526' union
SELECT 'DC', '20527' union
SELECT 'DC', '20528' union
SELECT 'DC', '20529' union
SELECT 'DC', '20530' union
SELECT 'DC', '20531' union
SELECT 'DC', '20532' union
SELECT 'DC', '20533' union
SELECT 'DC', '20534' union
SELECT 'DC', '20535' union
SELECT 'DC', '20536' union
SELECT 'DC', '20537' union
SELECT 'DC', '20538' union
SELECT 'DC', '20539' union
SELECT 'DC', '20540' union
SELECT 'DC', '20541' union
SELECT 'DC', '20542' union
SELECT 'DC', '20543' union
SELECT 'DC', '20544' union
SELECT 'DC', '20546' union
SELECT 'DC', '20547' union
SELECT 'DC', '20548' union
SELECT 'DC', '20549' union
SELECT 'DC', '20551' union
SELECT 'DC', '20552' union
SELECT 'DC', '20553' union
SELECT 'DC', '20554' union
SELECT 'DC', '20555' union
SELECT 'DC', '20557' union
SELECT 'DC', '20558' union
SELECT 'DC', '20559' union
SELECT 'DC', '20560' union
SELECT 'DC', '20565' union
SELECT 'DC', '20566' union
SELECT 'DC', '20570' union
SELECT 'DC', '20571' union
SELECT 'DC', '20572' union
SELECT 'DC', '20573' union
SELECT 'DC', '20575' union
SELECT 'DC', '20576' union
SELECT 'DC', '20577' union
SELECT 'DC', '20578' union
SELECT 'DC', '20579' union
SELECT 'DC', '20580' union
SELECT 'DC', '20581' union
SELECT 'DC', '20585' union
SELECT 'DC', '20586' union
SELECT 'DC', '20590' union
SELECT 'DC', '20591' union
SELECT 'DC', '20593' union
SELECT 'DC', '20594' union
SELECT 'DC', '20597' union
SELECT 'DC', '20599'
)

----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------
	--IF OBJECT_ID('tempdb..#calls') IS NOT NULL
	--	DROP TABLE #calls;

	SELECT 
			fct.KeyCustomerCall
			, cust.ClientId
			, fct.KeyCustomer
			, cust.CustomerId
			, fct.KeyEmployee
			, dss.KeySourceSystem
			, dss.SourceSystem
			, fct.CallStartTime
			, dt.WeekId
			, fct.DialedPhoneNumber
			, fct.DialedAreaCode
			, fct.SessionId
			, case when fct.IsRPC=1 then 1 else 0 end as contact_flag
			, de.EmployeeId
			, fct.CallSeconds
			, dcl.ClientParent
			, fct.IsRPC
			,rc.Call_Center_Name
	--isrpc_lag1 indicates previous call was an RPC without a requested callback
			, LAG(CASE WHEN rc.Livevox_Result <> 'AGENT - CUST RPC 12' THEN fct.IsRPC ELSE 0 END ,1) OVER(PARTITION BY cust.KeyCustomer ORDER BY fct.CallStartTime) AS isrpc_lag1
	--callstarttime_lag1 for calculating whether call following RPC is within 7 days
			, LAG(fct.CallStartTime,1) OVER(PARTITION BY cust.KeyCustomer ORDER BY fct.CallStartTime) AS callstarttime_lag1
    --left_msg_lag1 for indicates previous call was Left Message
			,LAG(CASE WHEN rc.Livevox_Result IN ('AGENT - CUST 11','AGENT - CUST 2', 'AGENT - Left Message Machine', 'AGENT - Left Message Machine', 'Machine') THEN 1 ELSE 0 END, 1) OVER(PARTITION BY cust.KeyCustomer ORDER BY fct.CallStartTime ) AS left_msg_lag1
	--callstarttime_lag4 for calculating whether called  more than 4 times per day on any given phone number
			, LAG(fct.CallStartTime,4) OVER(PARTITION BY fct.DialedPhoneNumber ORDER BY fct.CallStartTime) AS callstarttime_lag4
	--callstarttime_lag9 for calculating whether called  more than 9 times per day at account level
			, LAG(fct.CallStartTime,9) OVER(PARTITION BY cust.KeyCustomer ORDER BY fct.CallStartTime) AS callstarttime_lag9
    --calls with Dialed NV area codes
			, case when ac.StateCode='NV' then 1 else 0 end as statecode_nv
	--calls with Dialed CT area codes
			, case when ac.StateCode='CT' then 1 else 0 end as statecode_ct
	--calls with Dialed DC area codes
			, case when ac.StateCode='DC' then 1 else 0 end as statecode_dc
	--calls with Dialed NV zip codes
			, case when zs.CustomerState='NV' then 1 else 0 end as custstate_nv
	--calls with Dialed CT zip codes
			, case when zs.CustomerState='CT' then 1 else 0 end as custstate_ct
	--calls with Dialed DC zip codes
			, case when zs.CustomerState='DC' then 1 else 0 end as custstate_dc
			
	INTO #calls 
	
	FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
			  inner join
		 DW_MSTR_DM.dbo.RadiusCall rc (nolock) on fct.SessionId=rc.Session_Id and  rc.Call_Date >= isnull(@StartDateTime,GETDATE()) -31       
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
			  LEFT outer JOIN 
		 DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer and fct.DialedPhoneNumber=RP.PhoneNumber
		      left outer join
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left outer join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		  left join 
		 [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup on lup.ClientCode= cust.ClientId
                                                                                    and cust.SourceSystem= 'AMEX Latitude'
																					and lup.FirstPartyFlag = 0
                                                                                    --and (lup.FirstPartyFlag <> 1 
																					--     or lup.FirstPartyFlag is NULL)
		 left outer join 
		 areacode ac on fct.DialedAreaCode = ac.areacode
		 left outer join
		 zip_state zs on fct.zip = zs.zip
	WHERE dt.CalendarDate >= isnull(@StartDateTime,GETDATE())-31                
	    AND fct.IsOutbound=1
	    AND rc.LV_Client_Name = 'Veldos'
		and rc.service_name not like '%HTI%'
		and lup.FirstPartyFlag = 0
		---and (lup.FirstPartyFlag <> 1 or lup.FirstPartyFlag is NULL)
			
		;



-------Insert new recs for First ever WPCs from past 7 days into Build_Amex_3P_First_Ever_WPC_Calls table-----------------------------------------------------------------------------------
INSERT INTO CLIENT_ANALYTICS.dbo.Build_Amex_3P_First_Ever_WPC_Calls (KeyCustomer, Callstarttime, DialedPhoneNumber, insert_date)
	 	
		SELECT fct.KeyCustomer, MIN(fct.CallStartTime) AS First_Ever_WPC, fct.DialedPhoneNumber , GETDATE()
		FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
			  inner join
		 DW_MSTR_DM.dbo.RadiusCall rc (nolock) on fct.SessionId=rc.Session_Id AND rc.Call_Date >= ISNULL(@StartDateTime,GETDATE())-7
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
			  LEFT outer JOIN 
		 DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer and 	fct.DialedPhoneNumber=RP.PhoneNumber
		      left outer join
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left outer join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem 
		 	  left outer join 
		 [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup on lup.ClientCode= cust.ClientId
                                                                                    and cust.SourceSystem= 'AMEX Latitude'
																					and lup.FirstPartyFlag = 0
                                                                                    --and (lup.FirstPartyFlag <> 1 
																					--     or lup.FirstPartyFlag is NULL)
		 LEFT OUTER JOIN 
		 CLIENT_ANALYTICS.dbo.Build_Amex_3P_First_Ever_WPC_Calls (NOLOCK) w 
		 ON fct.KeyCustomer = w.KeyCustomer 
		 AND  fct.CallStartTime >= w.callstarttime 
		 AND fct.DialedPhoneNumber = w.dialedphonenumber
WHERE dt.CalendarDate >= isnull(@StartDateTime,GETDATE())-7
	    AND  fct.IsOutbound=1
	    AND rc.LV_Client_Name = 'Veldos'
		AND rc.Livevox_Result  = 'AGENT - WRONG NUMBER'
		and rc.service_name not like '%HTI%'
		and lup.FirstPartyFlag = 0	
		---and (lup.FirstPartyFlag <> 1 or lup.FirstPartyFlag is NULL)
		AND w.KeyCustomer IS NULL
GROUP BY 	  fct.KeyCustomer		
			, fct.DialedPhoneNumber
          ;

---------------------------Wrong Numbers on which Customer provided authorization to call again-------------------------------------------------------------------------------------
DELETE w
FROM CLIENT_ANALYTICS.dbo.Build_Amex_3P_First_Ever_WPC_Calls w 
inner join dw_MSTR_DM.dbo.RadiusPhone rp (NOLOCK) 
on w.KeyCustomer = RP.KeyCustomer
 and w.DialedPhoneNumber = RP.PhoneNumber
 and rp.KeySourceSystem = '3'
 and rp.PhoneType IN ('Good', 'Verified', 'Consent', 'Call Only')


	--EXCEPTIONS
	--identify exceptions for each rule
	---WITH exceptions AS
	---(
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
	------dlr_expt_id=11:  Client – Amex – Post-RPC 0 attempts per day
				SELECT 11 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
		INTO #exceptions
		FROM #calls c
		WHERE isrpc_lag1=1
			  AND CAST(c.callstarttime_lag1 AS DATE) = CAST(c.CallStartTime AS DATE)
			  

		UNION
    ----dlr_expt_id=12:  Client – Amex – Post-Left Msg 0 attempts per day	
		SELECT 12 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent   
		FROM #calls c
			WHERE 
			c.left_msg_lag1 = 1
	          AND CAST(c.callstarttime_lag1 AS DATE) = CAST(c.CallStartTime AS DATE)
			  

			  UNION
    ---dlr_expt_id=13:  Client – Amex – Post-WPC 0 attempts for lifetime		
		SELECT 13 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
			  
		FROM #calls c INNER JOIN CLIENT_ANALYTICS.dbo.Build_Amex_3P_First_Ever_WPC_Calls (NOLOCK) w
		ON c.KeyCustomer = w.keycustomer
		AND c.DialedPhoneNumber = w.DialedPhoneNumber
		AND c.CallStartTime > w.CallStartTime
		AND c.DialedPhoneNumber <> 0
		
	---)


	UNION
	------dlr_expt_id=22:  Client – Amex – NV Area Code
				SELECT 22 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
	
		FROM #calls c
		WHERE c.statecode_nv = 1
		AND c.Call_Center_Name IN ('India', 'Non Reg F')
		
		UNION
	------dlr_expt_id=23:  Client – Amex – NV Zip Code
				SELECT 23 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
	
		FROM #calls c
		WHERE c.custstate_nv = 1
		AND c.Call_Center_Name IN ('India', 'Non Reg F')

		UNION
	------dlr_expt_id=24:  Client – Amex – CT Area Code
				SELECT 24 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
	
		FROM #calls c
		WHERE c.statecode_ct = 1
		AND c.Call_Center_Name IN ('India', 'Non Reg F')

		UNION
	------dlr_expt_id=25:  Client – Amex – CT Zip Code
				SELECT 25 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
	
		FROM #calls c
		WHERE c.custstate_ct = 1
		AND c.Call_Center_Name IN ('India', 'Non Reg F')

	UNION
	------dlr_expt_id=26:  Client – Amex – 4 attempts per day by Phone Number
				SELECT 26 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
	
		FROM #calls c
		WHERE CAST(c.callstarttime_lag4 AS DATE) = CAST(c.CallStartTime AS DATE)

		UNION
		------dlr_expt_id=27:  Client – Amex – 9 attempts per day by Account
				SELECT 27 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
	
		FROM #calls c
		WHERE CAST(c.callstarttime_lag9 AS DATE) = CAST(c.CallStartTime AS DATE)
		UNION
		------dlr_expt_id=28:  Client – Amex – DC 4 Attempts and 1 RPC per Week
				SELECT 28 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
	
		FROM #calls c
		WHERE c.statecode_dc+c.custstate_dc>=1     --either area code or custstate=DC
		      and
			  (
  			     --previous RPC is within 7 days
				 (
		          isrpc_lag1=1 
			      and datediff(day,cast(c.callstarttime_lag1 as date),CAST(c.CallStartTime AS DATE))<=7
			     )
			     or
				 --5th previous attempt is within 7 days
				 (
				  datediff(day,cast(c.callstarttime_lag4 as date),CAST(c.CallStartTime AS DATE))<=7
				 )
			  )


---SELECT dlr_excpt_id, count(*)  FROM #exceptions group by dlr_excpt_id order by dlr_excpt_id

		
--------------------------Adding 0's into CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count] for Yesterday----------------------------

INSERT INTO  CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count]
SELECT calldate
,keysourcesystem
,dlr_expt_id
,Count_of_Exceptions
,Insert_Date
FROM
(
SELECT #t.day AS calldate
,#t.keysourcesystem AS keysourcesystem
,#t.dlr_expt_id AS dlr_expt_id
,ISNULL(SUM(E.Count_of_Exceptions),0) AS Count_of_Exceptions
,GETDATE() AS Insert_Date
,c.No_of_Calls
FROM #t
LEFT JOIN 
(SELECT CAST(exc.CallStartTime AS DATE) calldate
,exc.keysourcesystem
,exc.dlr_excpt_id
,COUNT(*) AS Count_of_Exceptions
FROM #exceptions exc 
group by CAST(exc.CallStartTime AS DATE)
,exc.keysourcesystem
,exc.dlr_excpt_id
) E
ON CAST(e.calldate AS DATE) = #t.day
	AND e.keysourcesystem = #t.keysourcesystem
	AND e.dlr_excpt_id = #t.dlr_expt_id
LEFT JOIN 
(
SELECT 
dt.CalendarDate AS calldate
, fct.KeySourceSystem
, COUNT(*) AS No_of_Calls 
FROM  DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
inner join
DW_MSTR_DM.dbo.DimDate dt (NOLOCK) 
ON fct.KeyDate_CallDate = dt.KeyDate
WHERE dt.CalendarDate  = CAST(ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS DATE)
GROUP BY dt.CalendarDate  , fct.KeySourceSystem 
)c
ON #t.day = CAST(c.calldate AS DATE) 
	AND #t.keysourcesystem = c.KeySourceSystem

GROUP BY 
#t.day 
,#t.keysourcesystem
,#t.dlr_expt_id   
,c.No_of_Calls
)f
WHERE ISNULL(f.No_of_Calls,0) > 0


	

------------------------------------Insert Calls Exceptions into CLIENT_ANALYTICS.dbo.fact_dial_excpt------------------------------------------------
INSERT INTO CLIENT_ANALYTICS.dbo.fact_dial_excpt(dlr_excpt_id,keycustomercall,call_history_fact_id,customerid,
	                                          clientid,sourcesystem,calldate,sessionid,insert_date,
	                                          DialedPhoneNumber,CallStartTime,EmployeeID,CallSeconds,ClientParent)		                           	                                        

	SELECT exc.dlr_excpt_id
		   , exc.keycustomercall
		   , exc.call_history_fact_id
		   , exc.customerid
		   , exc.clientid
		   , exc.sourcesystem
		   , exc.calldate
		   , exc.sessionid
		   , exc.insert_date
		   , exc.DialedPhoneNumber
		   , exc.CallStartTime
		   , exc.EmployeeID
		   , exc.CallSeconds
		   , exc.ClientParent
	FROM #exceptions exc
			LEFT OUTER JOIN
		 CLIENT_ANALYTICS.dbo.fact_dial_excpt fde (NOLOCK) ON exc.keycustomercall=fde.keycustomercall
	WHERE fde.keycustomercall IS NULL 
		  AND fde.call_history_fact_id IS NULL   
                           

END;









GO


