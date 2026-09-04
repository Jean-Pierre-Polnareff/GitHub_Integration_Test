USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_insert_fact_dial_excpt_FACS_call_attempts]    Script Date: 8/1/2023 10:25:26 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







ALTER  PROCEDURE [dbo].[sp_insert_fact_dial_excpt_FACS_call_attempts]
	
	@StartDateTime DATETIME = NULL
	 

AS
/* 
Object: dbo.sp_insert_fact_dial_excpt_FACS_call_attempts

Description: Identify and insert call attempts for dialer exceptions from FACS portal into fact_dial_expt_call_attempts

Author			Date		Description
Amod Ramugade	03/14/2023	Created

*/

BEGIN
	SET NOCOUNT ON;

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_expt_call_attempts] for last 31 days------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_expt_call_attempts]
WHERE calldate  > isnull(@StartDateTime,GETDATE())-31
AND dlr_excpt_id   IN (1,3,5,6,7,8,29) 
AND KeySourceSystem = 4

----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;

	with areacode
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
			--fct.CALL_HISTORY_FACT_ID
			--, 
			fct.CLIENT_ID as clientid
			--, fct.CUSTOMER_ID as customerid
			, KeySourceSystem = 4
			, 'FACS' as SourceSystem
			, CAST(fct.CALL_DATE AS DATE) callDate ---+ CAST(fct.call_start_time as datetime) as callstarttime
			--, DATEPART(week,call_date) as WeekId
			--, fct.CALL_OUT_PHONE_NUMBER as DialedPhoneNumber
			--, left(fct.CALL_OUT_PHONE_NUMBER,3) as DialedAreaCode
			, case when cust.CUSTOMER_STATE='MA' or ac.StateCode='MA' then 1 else 0 end as custstate_ma
			, case when cust.CUSTOMER_STATE='OR' or ac.StateCode='OR' then 1 else 0 end as custstate_or
			, case when cust.CUSTOMER_STATE='WA' or ac.StateCode='WA' then 1 else 0 end as custstate_wa
			, case when nyz.zip is not null then 1 else 0 end as custstate_ny
			, case when cust.CUSTOMER_STATE='WV' or ac.StateCode='WV' then 1 else 0 end as custstate_wv
			, case when cust.CUSTOMER_STATE='DC' or ac.StateCode='DC' then 1 else 0 end as custstate_dc
			, case when fct.PHONE_TYPE in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE') then 1 else 0 end as phonetype_home
			, case when fct.PHONE_TYPE in('DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE') then 1 else 0 end as phonetype_poe
			, case when fct.PHONE_TYPE not in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE','DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE')
												 and fct.PHONE_TYPE is not null 
												 and len(rtrim(fct.phone_type))>0 then 1 else 0 end as phonetype_other
			, case when fct.RIGHT_PARTY_CONTACT='Y' then 1 else 0 end as contact_flag
			---, fct.EMPLOYEE_ID as EmployeeID
			---, fct.CALL_DURATION_SECONDS as CallSeconds
			, tcs.Parent as ClientParent
			, count(*) count_of_recs
	into #calls		
	FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT fct (NOLOCK)
			  inner join 
		 DW_MSTR_DM.dbo.LU_CUSTOMER cust (NOLOCK)ON fct.CUSTOMER_ID = cust.CUSTOMER_ID
			  LEFT outer JOIN 
		 AreaCode ac ON left(fct.CALL_OUT_PHONE_NUMBER,3) = ac.AreaCode
		      inner join
		 DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK) on cust.CLIENT_ID=tcs.Client_ID
			  left outer join
		 zip_state zs on fct.zip_scf = zs.scf
		      left outer join
		 nyzip nyz on fct.zip=nyz.zip
	WHERE fct.CALL_DATE > isnull(@StartDateTime,GETDATE())-31
		  and fct.CUSTOMER_ID not in(0,12345)		--ignore missing account number recs
		  AND fct.CALL_TYPE <> 'IN'
		  and ISNULL(fct.Data_Source,'') = 'NGLV'
		  and (
				  (zs.customerstate in('MA','WA','NY','WV') or ac.StateCode in('MA','WA','NY','WV'))
				  or
				  (
					  (zs.customerstate ='OR' or ac.StateCode ='OR')
					  and
					  fct.PHONE_TYPE in('C POE PHONE')
				  )
			  )
			  			  
			  GROUP BY 
			  fct.CLIENT_ID 
			--, fct.CUSTOMER_ID as customerid
			--, KeySourceSystem = 4
			--, 'FACS' as SourceSystem
			, CAST(fct.CALL_DATE AS DATETIME) --callDate ---+ CAST(fct.call_start_time as datetime) as callstarttime
			--, DATEPART(week,call_date) as WeekId
			--, fct.CALL_OUT_PHONE_NUMBER as DialedPhoneNumber
			--, left(fct.CALL_OUT_PHONE_NUMBER,3) as DialedAreaCode
			, case when cust.CUSTOMER_STATE='MA' or ac.StateCode='MA' then 1 else 0 end 
			, case when cust.CUSTOMER_STATE='OR' or ac.StateCode='OR' then 1 else 0 end 
			, case when cust.CUSTOMER_STATE='WA' or ac.StateCode='WA' then 1 else 0 end 
			, case when nyz.zip is not null then 1 else 0 end 
			, case when cust.CUSTOMER_STATE='WV' or ac.StateCode='WV' then 1 else 0 end 
			, case when cust.CUSTOMER_STATE='DC' or ac.StateCode='DC' then 1 else 0 end 
			, case when fct.PHONE_TYPE in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE') then 1 else 0 end 
			, case when fct.PHONE_TYPE in('DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE') then 1 else 0 end 
			, case when fct.PHONE_TYPE not in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE','DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE')
												 and fct.PHONE_TYPE is not null 
												 and len(rtrim(fct.phone_type))>0 then 1 else 0 end 
			, case when fct.RIGHT_PARTY_CONTACT='Y' then 1 else 0 end 
			---, fct.EMPLOYEE_ID as EmployeeID
			---, fct.CALL_DURATION_SECONDS as CallSeconds
			, tcs.Parent 
			  
			  ;

---SELECT * FROM #calls order by calldate


	--EXCEPTIONS CALL ATTEMPTS
	--identify call attempts for each exception rule
		
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlf_expt_id=1:  MA - 2 attempts per 7 sliding for home phone
		SELECT 1 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
		INTO #exceptions
		FROM #calls
		WHERE custstate_ma=1 
		----AND phonetype_home=1

		UNION

		--12/8/21 this exception disabled per Keith Sweatt

/*
		--dlf_expt_id=2:  MA - 1 attempts per 30 sliding for POE
		SELECT 2 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
		FROM #calls
		WHERE custstate_ma=1  AND phonetype_poe=1

		UNION
*/

		--dlf_expt_id=3:  MA - 2 attempts per 30 sliding for non-home, non-POE
		SELECT 3 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
		FROM #calls
		WHERE custstate_ma=1 AND phonetype_other=1
			  
		UNION


		--12/8/21 this exception disabled per Keith Sweatt

/*
		--dlf_expt_id=4:  OR - 1 attempts per 30 sliding for POE
		SELECT 4 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
		FROM #calls
		WHERE custstate_or=1 AND phonetype_poe=1

		UNION
*/

		--dlf_expt_id=5:  WA - 3 attempts per 7 sliding
		SELECT 5 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
		FROM #calls
		WHERE custstate_wa=1
			
		UNION

		--dlf_expt_id=6:  NY - 2 attempts per 7 sliding
		SELECT 6 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
		FROM #calls
		WHERE custstate_ny=1 

		UNION

		--dlf_expt_id=7:  WV - 2 contacts per calendar week
		SELECT 7 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
		FROM #calls
		WHERE custstate_wv=1 AND contact_flag=1
			
		UNION

		--dlf_expt_id=8:  WV - 10 attempts per calendar week
		SELECT 8 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
		FROM #calls
		WHERE custstate_wv=1 

        UNION

		--dlf_expt_id=29:  DC - 3 attempts per 7 sliding
		SELECT 29 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
		FROM #calls
		WHERE custstate_dc=1 



----------------------------------------------------------Insert to fact_dial_expt_call_attempts--------------------------------------------------------
	  INSERT INTO  client_analytics.dbo.fact_dial_expt_call_attempts (dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
		  )
		  SELECT 
		  dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   ,count_of_recs
			   FROm #exceptions
END;
GO


