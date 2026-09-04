USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[sp_insert_fact_dial_excpt]    Script Date: 1/17/2024 1:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






ALTER PROCEDURE [dbo].[sp_insert_fact_dial_excpt]
	 
	@StartDateTime DATETIME = NULL
	, @end datetime =  NULL
	
AS
/* 
Object: dbo.model_score_utility_model_id7

Description: Identify and insert dialer excep tions into fact_dial_excpt

Author			Date		Description
Mike Campbell	12/13/2019	Created
Mike Campbell	12/26/2019	Suppress NARS per Ted, add clientparent
Mike Campbell	01/03/2020	Suppress clientids: ('SNBCEP','QNC01P')
Mike Campbell	01/15/2020	Modify join to dimclient to included sourcesystem condition; remove join to radiuscall
Mike Campbell	01/17/2020	Assign customer to geography based on zip only
Mike Campbell	2/13/2020	changed date diff calcs to use rounded days instead of timestamps per Ted Miller instruction
Mike Campbell	4/2/2020	Needed to ignore dimcustomer.statuscode='DW_deactivate' records
Mike Campbell	10/1/2020	Brett per Amex suppressing several result codes from evaluation
Mike Campbell	5/24/2021	Brett added NY 2 per sliding 7 exclusion clientids and service_ids
Mike Campbell	9/28/2021	Brett added amex clientids to suppress from NY/WA evaluation
*/

BEGIN
	SET NOCOUNT ON;

--DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
WHERE calldate = isnull(@end ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
--AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id  IN (1,2,3,4,5,6,7,8,29) 
AND KeySourceSystem IN (1,2,3)

-----------------------------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------
	IF OBJECT_ID('tempdb..#t') IS NOT NULL
		DROP TABLE #t;
SELECT isnull(@end
               ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS day
       , dss.keysourcesystem
, dde.dlr_expt_id  
INTO #t 
FROM DW_MSTR_DM.dbo.DimSourceSystem dss 
CROSS JOIN 
CLIENT_ANALYTICS.dbo.dim_dial_excpt dde
WHERE dde.dlr_expt_id  IN (1,2,3,4,5,6,7,8,29) 
AND dss.KeySourceSystem  IN (1,2,3)
AND dde.all_client_flag = 1
----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------
	declare 
		 @model_id  int = 7;


	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;

	with areacode					--not used now, but leaving in case need in the future
	as
	(
		select 206 as AreaCode, 'WA' as StateCode union
		select 253, 'WA' union
		select 360, 'WA' union
		select 425, 'WA' union
		select 509, 'WA' union
		select 304, 'WV' union
		select 681, 'WV' union
		select 212, 'NY' union
		select 347, 'NY' union
		select 646, 'NY' union
		select 718, 'NY' union
		select 917, 'NY' union
		select 339, 'MA' union
		select 351, 'MA' union
		select 413, 'MA' union
		select 508, 'MA' union
		select 617, 'MA' union
		select 774, 'MA' union
		select 781, 'MA' union
		select 857, 'MA' union
		select 978, 'MA' union
		select 503, 'OR' union
		select 971, 'OR' union
		select 541, 'OR' union
		select 458, 'OR' union
		select 202, 'DC'
	),

	zip_state
	as
	(
		select 'MA' as customerstate, '010' as scf union
		select 'MA', '011' union
		select 'MA', '012' union
		select 'MA', '013' union
		select 'MA', '014' union
		select 'MA', '015' union
		select 'MA', '016' union
		select 'MA', '017' union
		select 'MA', '018' union
		select 'MA', '019' union
		select 'MA', '020' union
		select 'MA', '021' union
		select 'MA', '022' union
		select 'MA', '023' union
		select 'MA', '024' union
		select 'MA', '025' union
		select 'MA', '026' union
		select 'MA', '027' union
		select 'MA', '055' union
		select 'NY', '100' union
		select 'NY', '101' union
		select 'NY', '102' union
		select 'NY', '103' union
		select 'NY', '104' union
		select 'NY', '105' union
		select 'NY', '106' union
		select 'NY', '107' union
		select 'NY', '108' union
		select 'NY', '109' union
		select 'NY', '110' union
		select 'NY', '111' union
		select 'NY', '112' union
		select 'NY', '113' union
		select 'NY', '114' union
		select 'NY', '115' union
		select 'NY', '116' union
		select 'NY', '117' union
		select 'NY', '118' union
		select 'NY', '119' union
		select 'NY', '120' union
		select 'NY', '121' union
		select 'NY', '122' union
		select 'NY', '123' union
		select 'NY', '124' union
		select 'NY', '125' union
		select 'NY', '126' union
		select 'NY', '127' union
		select 'NY', '128' union
		select 'NY', '129' union
		select 'NY', '130' union
		select 'NY', '131' union
		select 'NY', '132' union
		select 'NY', '133' union
		select 'NY', '134' union
		select 'NY', '135' union
		select 'NY', '136' union
		select 'NY', '137' union
		select 'NY', '138' union
		select 'NY', '139' union
		select 'NY', '140' union
		select 'NY', '141' union
		select 'NY', '142' union
		select 'NY', '143' union
		select 'NY', '144' union
		select 'NY', '145' union
		select 'NY', '146' union
		select 'NY', '147' union
		select 'NY', '148' union
		select 'NY', '149' union
		select 'WV', '247' union
		select 'WV', '248' union
		select 'WV', '249' union
		select 'WV', '250' union
		select 'WV', '251' union
		select 'WV', '252' union
		select 'WV', '253' union
		select 'WV', '254' union
		select 'WV', '255' union
		select 'WV', '256' union
		select 'WV', '257' union
		select 'WV', '258' union
		select 'WV', '259' union
		select 'WV', '260' union
		select 'WV', '261' union
		select 'WV', '262' union
		select 'WV', '263' union
		select 'WV', '264' union
		select 'WV', '265' union
		select 'WV', '266' union
		select 'WV', '267' union
		select 'WV', '268' union
		select 'OR', '970' union
		select 'OR', '971' union
		select 'OR', '972' union
		select 'OR', '973' union
		select 'OR', '974' union
		select 'OR', '975' union
		select 'OR', '976' union
		select 'OR', '977' union
		select 'OR', '978' union
		select 'OR', '979' union
		select 'WA', '980' union
		select 'WA', '981' union
		select 'WA', '982' union
		select 'WA', '983' union
		select 'WA', '984' union
		select 'WA', '985' union
		select 'WA', '986' union
		select 'WA', '987' union
		select 'WA', '988' union
		select 'WA', '989' union
		select 'WA', '990' union
		select 'WA', '991' union
		select 'WA', '992' union
		select 'WA', '993' union
		select 'WA', '994' union
		select 'DC', '200' union
		select 'DC', '202' union
		select 'DC', '203' union
		select 'DC', '204' union
		select 'DC', '205'
	),

	nyzip
	as
	(
		select '10001' as zip union select '10002' union select '10003' union select '10004' union select '10005' union select '10006' union select '10007' union
		select '10008' union select '10009' union select '10010' union select '10011' union select '10012' union select '10013' union select '10014' union select '10016' union
		select '10017' union select '10018' union select '10019' union select '10020' union select '10021' union select '10022' union select '10023' union select '10024' union
		select '10025' union select '10026' union select '10027' union select '10028' union select '10029' union select '10030' union select '10031' union select '10032' union
		select '10033' union select '10034' union select '10035' union select '10036' union select '10037' union select '10038' union select '10039' union select '10040' union
		select '10041' union select '10043' union select '10044' union select '10045' union select '10055' union select '10060' union select '10065' union select '10069' union
		select '10075' union select '10080' union select '10081' union select '10087' union select '10090' union select '10095' union select '10101' union select '10102' union
		select '10103' union select '10104' union select '10105' union select '10106' union select '10107' union select '10108' union select '10109' union select '10110' union
		select '10111' union select '10112' union select '10113' union select '10114' union select '10115' union select '10116' union select '10117' union select '10118' union
		select '10119' union select '10120' union select '10121' union select '10122' union select '10123' union select '10124' union select '10125' union select '10126' union
		select '10128' union select '10129' union select '10130' union select '10131' union select '10132' union select '10133' union select '10138' union select '10150' union
		select '10151' union select '10152' union select '10153' union select '10154' union select '10155' union select '10156' union select '10157' union select '10158' union
		select '10159' union select '10160' union select '10161' union select '10162' union select '10163' union select '10164' union select '10165' union select '10166' union
		select '10167' union select '10168' union select '10169' union select '10170' union select '10171' union select '10172' union select '10173' union select '10174' union
		select '10175' union select '10176' union select '10177' union select '10178' union select '10179' union select '10185' union select '10199' union select '10203' union
		select '10211' union select '10212' union select '10213' union select '10242' union select '10249' union select '10256' union select '10257' union select '10258' union
		select '10259' union select '10260' union select '10261' union select '10265' union select '10268' union select '10269' union select '10270' union select '10271' union
		select '10272' union select '10273' union select '10274' union select '10275' union select '10276' union select '10277' union select '10278' union select '10279' union
		select '10280' union select '10281' union select '10282' union select '10285' union select '10286' union select '10292' union select '10301' union select '10302' union  
		select '10303' union select '10304' union select '10305' union select '10306' union select '10307' union select '10308' union select '10309' union select '10310' union
		select '10311' union select '10312' union select '10313' union select '10314' union select '10451' union select '10452' union select '10453' union select '10454' union
		select '10455' union select '10456' union select '10457' union select '10458' union select '10459' union select '10460' union select '10461' union select '10462' union
		select '10463' union select '10464' union select '10465' union select '10466' union select '10467' union select '10468' union select '10469' union select '10470' union
		select '10471' union select '10472' union select '10473' union select '10474' union select '10475' union select '10499' union select '11004' union select '11005' union
		select '11101' union select '11102' union select '11103' union select '11104' union select '11105' union select '11106' union select '11109' union select '11120' union
		select '11201' union select '11202' union select '11203' union select '11204' union select '11205' union select '11206' union select '11207' union select '11208' union
		select '11209' union select '11210' union select '11211' union select '11212' union select '11213' union select '11214' union select '11215' union select '11216' union
		select '11217' union select '11218' union select '11219' union select '11220' union select '11221' union select '11222' union select '11223' union select '11224' union
		select '11225' union select '11226' union select '11228' union select '11229' union select '11230' union select '11231' union select '11232' union select '11233' union
		select '11234' union select '11235' union select '11236' union select '11237' union select '11238' union select '11239' union select '11241' union select '11242' union
		select '11243' union select '11245' union select '11247' union select '11248' union select '11249' union select '11251' union select '11252' union select '11254' union
		select '11255' union select '11256' union select '11351' union select '11352' union select '11354' union select '11355' union select '11356' union select '11357' union
		select '11358' union select '11359' union select '11360' union select '11361' union select '11362' union select '11363' union select '11364' union select '11365' union
		select '11366' union select '11367' union select '11368' union select '11369' union select '11370' union select '11371' union select '11372' union select '11373' union
		select '11374' union select '11375' union select '11377' union select '11378' union select '11379' union select '11380' union select '11381' union select '11385' union
		select '11386' union select '11390' union select '11405' union select '11411' union select '11412' union select '11413' union select '11414' union select '11415' union
		select '11416' union select '11417' union select '11418' union select '11419' union select '11420' union select '11421' union select '11422' union select '11423' union
		select '11424' union select '11425' union select '11426' union select '11427' union select '11428' union select '11429' union select '11430' union select '11431' union
		select '11432' union select '11433' union select '11434' union select '11435' union select '11436' union select '11439' union select '11451' union select '11499' union
		select '11690' union select '11691' union select '11692' union select '11693' union select '11694' union select '11695' union select '11697'
	)

	SELECT 
			fct.KeyCustomerCall
			, cust.ClientId
			, case when dss.SourceSystem='Amex Latitude' then isnull(fct.Consumer_ID,Cast(fct.KeyCustomer AS VARCHAR))
			       else Cast(fct.KeyCustomer AS VARCHAR)
				   end as KeyCustomer
			, cust.CustomerId
			, fct.KeyEmployee
			, dss.KeySourceSystem
			, dss.SourceSystem
			, fct.CallStartTime
			, dt.WeekId
			, fct.DialedPhoneNumber
			, fct.DialedAreaCode
			, fct.SessionId
			, case when zs.CustomerState='MA' then 1 else 0 end as custstate_ma
			, case when zs.CustomerState='OR' then 1 else 0 end as custstate_or
			, case when zs.CustomerState='WA' then 1 else 0 end as custstate_wa
			, case when nyz.zip is not null then 1 else 0 end as custstate_ny
			, case when zs.CustomerState='WV' then 1 else 0 end as custstate_wv
			, case when zs.CustomerState='DC' then 1 else 0 end as custstate_dc
			, case when fct.PhonePosition in('Home','HOME PHONE','Other Home','Spouse Home','Supplemental CM Home') then 1 else 0 end as phonetype_home
			, case when fct.PhonePosition in('Other Work','POE PHONE','Spouse Work','Supplemental CM Work','Work') then 1 else 0 end as phonetype_poe
			, case when fct.PhonePosition not in('Home','HOME PHONE','Other Home','Spouse Home','Supplemental CM Home',
												'Other Work','POE PHONE','Spouse Work','Supplemental CM Work','Work')
												 and fct.PhonePosition is not null then 1 else 0 end as phonetype_other
			, case when fct.IsRPC=1 then 1 else 0 end as contact_flag
			, de.EmployeeId
			, fct.CallSeconds
			, dcl.ClientParent
			, rc.service_id
			, rc.call_center_id
            --9/23/22 - Brett adds veldos services, clientids to ignore for NY, WA, MA
			, case when rc.lv_client_name='Veldos'
			            and (
						      rc.service_id in(135415,114754,114755,119491,119495,128014,112692,91911,126822,128011,69751,
                                               69749,69747,112693,91912,126824,119481,77136,77137,112694,91913,126826,119487,
                                               71673,71674,71669,118314,71670,69746,106139,127756,128130,103607,99613,119489,
                                               124858,99684,141602,136404,143848,137438,137443,137439,137444,137440,137445,
                                               137441,137446,137442,137447,137448,137453,137449,137454,137450,137455,137451,
                                               137456,137452,137457,137458,137463,137459,137464,137460,137465,137461,137466,
                                               137462,137467,137468,137473,137469,137474,137470,137475,137471,137476,137472,
                                               137477,137478,137483,137479,137484,137480,137485,137481,137486,137482,137487,
                                               133925,142620,57866)
							  or
							  dcl.clientid in('116ALLR','117ALLR','118ALLR','118MIDR','119ALLR','119IUDR','119MD1R','119MD2R',
                                              '119MD3R','119OOSR','116SYTR','116SYCR','117SYTR','117SYCR','118SYTR','118SYCR',
                                              '119SY0R','119SY1R','119SY2R','119SY3R','119SY4R','119SY5R')
							)
					  then 1 
					  else 0
					  end as ny_wa_ma_suppress_flag 
			, pp.PhoneNumber 
	into #calls		
	FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
			  inner join
		 DW_MSTR_DM.dbo.RadiusCall rc (nolock) on fct.SessionId=rc.Session_Id 
		                                          and rc.Call_Date between isnull(@StartDateTime, GETDATE()) - 31 and isnull(@StartDateTime, GETDATE()) 
												  and fct.KeyDate_CallDate between convert(varchar,cast(isnull(@StartDateTime, GETDATE()) - 31 as date),112) and convert(varchar,cast(isnull(@StartDateTime, GETDATE()) as date),112) 
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
			  LEFT outer JOIN 
		 DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer and 	fct.DialedPhoneNumber=RP.PhoneNumber
			  left outer join 
		 DW_MSTR_DM.dbo.PhonePositions pp WITH (NOLOCK) ON pp.PhoneNumber = fct.DialedPhoneNumber 
														AND pp.PhonePosition IN ('HOME','POE') 
														AND pp.SourceSystem = dss.SourceSystem2  
			  left outer join
		 zip_state zs on fct.zip_scf = zs.scf
		      left outer join         
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left outer join 
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		      left outer join
		 nyzip nyz on fct.zip=nyz.zip
	WHERE rc.Call_Date between isnull(@StartDateTime, GETDATE()) - 31 and isnull(@StartDateTime, GETDATE()) 
		  and fct.KeyDate_CallDate between convert(varchar,cast(isnull(@StartDateTime, GETDATE()) - 31 as date),112) and convert(varchar,cast(isnull(@StartDateTime, GETDATE()) as date),112)
	      and (dss.SourceSystem<>'Amex Latitude' or cust.ClientId not like 'RA%')			--remove Amex 1st Party clientids
          --7/5/22  -  TedM requests all services containing "HTI" (sms) to be suppressed 
		  and rc.service_name not like '%HTI%'
	      and cust.ClientId not in('SNBCEP','QNC01P')
		  and (
				(rc.LV_Client_Name='Veldos' and rc.Livevox_Result not in('AGENT - CUST 9','AGENT - Busy'))
				or
				(rc.LV_Client_Name='RGS-CCS' and rc.Livevox_Result not in('AGENT - CUST 1','AGENT - Busy'))
				or 
				rc.LV_Client_Name not in('Veldos','RGS-CCS')
			   )
		  and fct.KeyCustomer>0		--ignore missing account number recs
		  AND fct.IsOutbound = 1
		  and (
				  (zs.CustomerState in('MA','WA','NY','WV'))
				  or
				  (
					  (zs.CustomerState ='OR')
					  and
					  fct.Phoneposition in ('Other Work','POE PHONE','Spouse Work','Supplemental CM Work','Work')
				  )
			  );

 

	--CALL_SEQ
	--sequencing calls for identifying exceptions thresholds in next CTE
	
	---WITH call_seq
	---AS
	---(
	IF OBJECT_ID('tempdb..#call_seq') IS NOT NULL
		DROP TABLE #call_seq;
	SELECT DISTINCT * 
		, CASE WHEN custstate_ma=1 AND phonetype_home=1
				THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, dialedphonenumber, custstate_ma, phonetype_home ORDER BY callstarttime)
				END AS rank_ma_home
		, CASE WHEN custstate_ma=1 AND phonetype_poe=1
				THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, dialedphonenumber, custstate_ma, phonetype_poe ORDER BY callstarttime)
				END AS rank_ma_poe
		, CASE WHEN custstate_ma=1 AND phonetype_other=1
				THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, dialedphonenumber, custstate_ma, phonetype_other ORDER BY callstarttime)
				END AS rank_ma_other
		, CASE WHEN custstate_ma=1 
				THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_ma ORDER BY callstarttime)
				END AS rank_ma 
			, CASE WHEN custstate_or=1 AND phonetype_poe=1
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_or, phonetype_poe ORDER BY callstarttime)
					END AS rank_or_poe
			, CASE WHEN custstate_wa=1 
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_wa ORDER BY callstarttime)
					END AS rank_wa
			, CASE WHEN custstate_ny=1 
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_ny ORDER BY callstarttime)
					END AS rank_ny
			, CASE WHEN custstate_wv=1 AND contact_flag=1
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, weekid, custstate_wv, contact_flag ORDER BY callstarttime)
					END AS rank_wv_contacts
			, CASE WHEN custstate_wv=1 
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, weekid, custstate_wv ORDER BY callstarttime)
					END AS rank_wv_attempts
			, CASE WHEN custstate_dc=1 
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, weekid, custstate_dc ORDER BY callstarttime)
					END AS rank_dc
			, DATEDIFF(DAY, prev1_call_start_time, callstarttime) interval1  
			, DATEDIFF(DAY, prev2_call_start_time, callstarttime) interval2  
			, DATEDIFF(DAY, prev3_call_start_time, callstarttime) interval3  
			, DATEDIFF(DAY, prev4_call_start_time, callstarttime) interval4  
	INTO #call_seq 
	FROM (
		SELECT *, 
			LAG (CallStartTime, 1) OVER (PARTITION BY KeyCustomer ORDER BY CallStartTime) prev1_call_start_time, 
			LAG (CallStartTime, 2) OVER (PARTITION BY KeyCustomer ORDER BY CallStartTime) prev2_call_start_time, 
			LAG (CallStartTime, 3) OVER (PARTITION BY KeyCustomer ORDER BY CallStartTime) prev3_call_start_time, 
			LAG (CallStartTime, 34) OVER (PARTITION BY KeyCustomer ORDER BY CallStartTime) prev4_call_start_time 
		FROM (
		SELECT distinct * 
			FROM #calls 
			) tI ) t  
	---),

	          
	--EXCEPTIONS
	--identify exceptions for each rule
	--in SQL 2008 there's no LAG function so have to join CALL_SEQ set to earlier calls based on their sequence nbr for each rule set
	
	---exceptions AS
	---(
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlf_expt_id=1:  MA - 2 attempts per 7 sliding for all phones
		SELECT 1 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
        INTO #exceptions
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_ma - cs2.rank_ma = 2
							 --AND cs1.DialedPhoneNumber = cs2.DialedPhoneNumber
		WHERE cs1.rank_ma IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 2 calls ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 2 calls ago
			  and cs1.ny_wa_ma_suppress_flag=0
		UNION

		--dlf_expt_id=2:  MA - 1 attempts per 30 sliding for POE
		SELECT 2 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_ma_poe-cs2.rank_ma_poe=1
							 and cs1.DialedPhoneNumber = cs2.DialedPhoneNumber
		WHERE cs1.rank_ma_poe IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime) < 30	-- < 30 days diff between current call and 1 call ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 1 call ago
			  and cs1.ny_wa_ma_suppress_flag = 0
		UNION

		--dlf_expt_id=3:  MA - 2 attempts per 30 sliding for non-home, non-POE
		SELECT 3 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer = cs2.keycustomer
							 AND cs1.rank_ma_other - cs2.rank_ma_other = 2
							 AND cs1.DialedPhoneNumber = cs2.DialedPhoneNumber
		WHERE cs1.rank_ma_other IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime) < 30	-- < 30 days diff between current call and 2 calls ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 2 calls ago
			  AND cs1.ny_wa_ma_suppress_flag=0 
			  AND cs1.PhoneNumber IS NULL  
		UNION

		--dlf_expt_id=4:  OR - 1 attempts per 30 sliding for POE
		SELECT 4 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_or_poe-cs2.rank_or_poe=1
		WHERE cs1.rank_or_poe IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<30	-- < 30 days diff between current call and 1 call ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 1 call ago
		UNION

		--dlf_expt_id=5:  WA - 3 attempts per 7 sliding
		SELECT 5 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_wa-cs2.rank_wa=3
		WHERE cs1.rank_wa IS NOT NULL 
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 3 calls ago
			  --1stparty excluded per BrettL 5/24/21, many 3rd party added 9/28/21
			  AND cs1.clientid NOT IN('11NLDGR','121ETXA','121EBXA','121CBXA','RAIAM','RAICM','RAIEM','RAIKM','RAILM','RAIMM',
                                      'RAIJM','RRAIJM','RAIHM','RAIBM','RAIDM','RAPLM','RAITM','RAINM','RAIPM','12EC2FA','12EC2CA',
                                      '12ECOTA','12ECOXA','12EC2AA','12ECOCA','12ECOFA','12EC2TA','12EACNA','RA2XM','RA1XM','RA1NM',
                                      'RA2NM','RA6XM','RARXM','RA7XM','RA3XM','RA3NM','RA5NM','RA4QM','RA5FM','RA6AM','RA5GM','RA4PM',
                                      'RA4CM','RA6NM','RA5OM','RA8MM','RA5MM','RA4MM','RA3MM','RA5JM','RA65M','RA66M','RA67M','RA6JM',
                                      'RA7AM','RA7KM','RA7MM','RA6MM','RA7PM','RA68M','RA6RM','RA6FM','RA7RM','RA8JM','RA8SM','RA1YM',
                                      'RA2YM','RA6YM','RARYM','RA7YM','RA3YM','RA4YM','RA5YM','RA5PM','RA4NM','RA4LM','RA7GM','RA7WM',
                                      'RA7QM','RA2BM','RA1BM','RA6CM','RA7BM','RA3CM','RARAM','RA5CM','RA4AM','RA4BM','RA5BM','RA4DM',
                                      'RA7EM','RA7CM','RA7JM','RA2TM','RA1TM','RA6TM','RA7TM','RA3TM','RARTM','RA5TM','RA4TM','RA77M',
                                      'RA4RM','RA5SM','RA4SM','RA78M','RA79M','121CBFA','121LTFA','121CBCA','121LTCA','121LTXA',
									  '121EBFA','121LBFA','121ETCA','121CBTA','121LTTA','121CBXA','121EBCA','121EBTA','121EBXA',
									  '121ETTA','121ETXA','121ETFA','121LBCA','121LBTA','121LBXA','1221BCA','122LTCA','12260XA',
									  '12260CA','12260FA','122LTFA','122LTXA','1221BXA','122LTTA','1221BFA','122LBCA','1221TCA',
									  '121DSPA','1221TXA','122LBXA','122LBFA','122LBTA','1221TFA','122OTHA','123LTTA','123LBTA',
									  '123D2AA','123LTCA','123D2CA','123D2FA','123LBCA','123LBXA','123LTXA','123LBFA','122EJSA',
									  '123LTFA','121PLAA','121HBCA','121HBTA','121HBXA','121HBFA','122ACNA','R1J2M','R1FVM','RA25V',
									  'RA4IV','RA35V','RA4UV','RA4OV','RA36V','RA43V','RA49V','RA46V','RA15V','RABMV','117ALLR',
									  '118ALLR','119ALLR','119IUDR','119OOSR','111OBMR','111LBMR','118MIDR','119MD1R','119MD3R',
									  '116ALLR','11NLDGR','119MD2R','11POHIR','11PCHIR','11PLHIR')  
			  AND cs1.service_id NOT IN (110176,110175,110177,110180,149277,149272,149278,149273,149279,149274,149280,149275,149281,149276,148736)  --1stparty excluded per BrettL 5/24/21
			  AND cs1.call_center_id<>8421  --1stparty excluded per BrettL 6/1/21
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 3 calls ago
			  and cs1.ny_wa_ma_suppress_flag=0
		UNION

		--dlf_expt_id=6:  NY - 2 attempts per 7 sliding
		SELECT 6 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_ny-cs2.rank_ny=2
		WHERE cs1.rank_ny IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 2 calls ago
			  --1stparty excluded per BrettL 5/24/21, many 3rd party added 9/28/21
			  AND cs1.clientid NOT IN('11NLDGR','121ETXA','121EBXA','121CBXA','RAIAM','RAICM','RAIEM','RAIKM','RAILM','RAIMM',
                                      'RAIJM','RRAIJM','RAIHM','RAIBM','RAIDM','RAPLM','RAITM','RAINM','RAIPM','12EC2FA','12EC2CA',
                                      '12ECOTA','12ECOXA','12EC2AA','12ECOCA','12ECOFA','12EC2TA','12EACNA','RA2XM','RA1XM','RA1NM',
                                      'RA2NM','RA6XM','RARXM','RA7XM','RA3XM','RA3NM','RA5NM','RA4QM','RA5FM','RA6AM','RA5GM','RA4PM',
                                      'RA4CM','RA6NM','RA5OM','RA8MM','RA5MM','RA4MM','RA3MM','RA5JM','RA65M','RA66M','RA67M','RA6JM',
                                      'RA7AM','RA7KM','RA7MM','RA6MM','RA7PM','RA68M','RA6RM','RA6FM','RA7RM','RA8JM','RA8SM','RA1YM',
                                      'RA2YM','RA6YM','RARYM','RA7YM','RA3YM','RA4YM','RA5YM','RA5PM','RA4NM','RA4LM','RA7GM','RA7WM',
                                      'RA7QM','RA2BM','RA1BM','RA6CM','RA7BM','RA3CM','RARAM','RA5CM','RA4AM','RA4BM','RA5BM','RA4DM',
                                      'RA7EM','RA7CM','RA7JM','RA2TM','RA1TM','RA6TM','RA7TM','RA3TM','RARTM','RA5TM','RA4TM','RA77M',
                                      'RA4RM','RA5SM','RA4SM','RA78M','RA79M','121CBFA','121LTFA','121CBCA','121LTCA','121LTXA',
									  '121EBFA','121LBFA','121ETCA','121CBTA','121LTTA','121CBXA','121EBCA','121EBTA','121EBXA',
									  '121ETTA','121ETXA','121ETFA','121LBCA','121LBTA','121LBXA','1221BCA','122LTCA','12260XA',
									  '12260CA','12260FA','122LTFA','122LTXA','1221BXA','122LTTA','1221BFA','122LBCA','1221TCA',
									  '121DSPA','1221TXA','122LBXA','122LBFA','122LBTA','1221TFA','122OTHA','123LTTA','123LBTA',
									  '123D2AA','123LTCA','123D2CA','123D2FA','123LBCA','123LBXA','123LTXA','123LBFA','122EJSA',
									  '123LTFA','121PLAA','121HBCA','121HBTA','121HBXA','121HBFA','122ACNA','R1J2M','R1FVM','RA25V',
									  'RA4IV','RA35V','RA4UV','RA4OV','RA36V','RA43V','RA49V','RA46V','RA15V','RABMV','117ALLR',
									  '118ALLR','119ALLR','119IUDR','119OOSR','111OBMR','111LBMR','118MIDR','119MD1R','119MD3R',
									  '116ALLR','11NLDGR','119MD2R','11POHIR','11PCHIR','11PLHIR')  
			  AND cs1.service_id NOT IN (110176,110175,110177,110180,149272,149278,149273,149279,149274,149280,149275,149281,149276,148736)  --1stparty excluded per BrettL 5/24/21
			  AND cs1.call_center_id<>8421  --1stparty excluded per BrettL 6/1/21
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 2 calls ago
			  and cs1.ny_wa_ma_suppress_flag=0
		UNION

		--dlf_expt_id=7:  WV - 2 contacts per calendar week
		SELECT 7 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_wv_contacts-cs2.rank_wv_contacts=2
		WHERE cs1.rank_wv_contacts IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < Calendar week not more than 2 contacts
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < Calendar week not more than 2 contacts
		UNION

		--dlf_expt_id=8:  WV - 10 attempts per calendar week
		SELECT 8 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_wv_attempts-cs2.rank_wv_attempts=10
		WHERE cs1.rank_wv_attempts IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < Calendar week not more than 10 attempts
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < Calendar week not more than 10 attempts
		UNION

		--dlf_expt_id=29:  DC - 3 attempts per 7 sliding
		SELECT 29 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_dc-cs2.rank_dc=3
		WHERE cs1.rank_dc IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 3 calls ago
			  --1stparty excluded per BrettL 5/24/21, many 3rd party added 9/28/21
			  AND cs1.clientid NOT IN('11NLDGR','121ETXA','121EBXA','121CBXA','RAIAM','RAICM','RAIEM','RAIKM','RAILM','RAIMM',
                                      'RAIJM','RRAIJM','RAIHM','RAIBM','RAIDM','RAPLM','RAITM','RAINM','RAIPM','12EC2FA','12EC2CA',
                                      '12ECOTA','12ECOXA','12EC2AA','12ECOCA','12ECOFA','12EC2TA','12EACNA','RA2XM','RA1XM','RA1NM',
                                      'RA2NM','RA6XM','RARXM','RA7XM','RA3XM','RA3NM','RA5NM','RA4QM','RA5FM','RA6AM','RA5GM','RA4PM',
                                      'RA4CM','RA6NM','RA5OM','RA8MM','RA5MM','RA4MM','RA3MM','RA5JM','RA65M','RA66M','RA67M','RA6JM',
                                      'RA7AM','RA7KM','RA7MM','RA6MM','RA7PM','RA68M','RA6RM','RA6FM','RA7RM','RA8JM','RA8SM','RA1YM',
                                      'RA2YM','RA6YM','RARYM','RA7YM','RA3YM','RA4YM','RA5YM','RA5PM','RA4NM','RA4LM','RA7GM','RA7WM',
                                      'RA7QM','RA2BM','RA1BM','RA6CM','RA7BM','RA3CM','RARAM','RA5CM','RA4AM','RA4BM','RA5BM','RA4DM',
                                      'RA7EM','RA7CM','RA7JM','RA2TM','RA1TM','RA6TM','RA7TM','RA3TM','RARTM','RA5TM','RA4TM','RA77M',
                                      'RA4RM','RA5SM','RA4SM','RA78M','RA79M','121CBFA','121LTFA','121CBCA','121LTCA','121LTXA',
									  '121EBFA','121LBFA','121ETCA','121CBTA','121LTTA','121CBXA','121EBCA','121EBTA','121EBXA',
									  '121ETTA','121ETXA','121ETFA','121LBCA','121LBTA','121LBXA','1221BCA','122LTCA','12260XA',
									  '12260CA','12260FA','122LTFA','122LTXA','1221BXA','122LTTA','1221BFA','122LBCA','1221TCA',
									  '121DSPA','1221TXA','122LBXA','122LBFA','122LBTA','1221TFA','122OTHA','123LTTA','123LBTA',
									  '123D2AA','123LTCA','123D2CA','123D2FA','123LBCA','123LBXA','123LTXA','123LBFA','122EJSA',
									  '123LTFA','121PLAA','121HBCA','121HBTA','121HBXA','121HBFA','122ACNA','R1J2M','R1FVM','RA25V',
									  'RA4IV','RA35V','RA4UV','RA4OV','RA36V','RA43V','RA49V','RA46V','RA15V','RABMV','117ALLR',
									  '118ALLR','119ALLR','119IUDR','119OOSR','111OBMR','111LBMR','118MIDR','119MD1R','119MD3R',
									  '116ALLR','11NLDGR','119MD2R','11POHIR','11PCHIR','11PLHIR')  
			  AND cs1.service_id NOT IN(110176,110175,110177,110180)  --1stparty excluded per BrettL 5/24/21
			  AND cs1.call_center_id<>8421  --1stparty excluded per BrettL 6/1/21
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 3 calls ago
			  --and cs1.ny_wa_ma_suppress_flag=0   -- not applicable for DC





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
WHERE dt.CalendarDate  = CAST(isnull(@end
                                     ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) 
							   AS DATE)
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



--------------------------------------------------Insert to fact_dial_excpt--------------------------------------------------
	INSERT INTO CLIENT_ANALYTICS.dbo.fact_dial_excpt(dlr_excpt_id,keycustomercall,call_history_fact_id,customerid,clientid,sourcesystem,calldate,sessionid,insert_date,
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
