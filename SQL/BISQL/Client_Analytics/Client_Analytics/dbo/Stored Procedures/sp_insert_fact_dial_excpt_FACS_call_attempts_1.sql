






CREATE  PROCEDURE [dbo].[sp_insert_fact_dial_excpt_FACS_call_attempts]
	
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
            
			-- State flags
            
			, case when cust.CUSTOMER_STATE='MA' or ac.StateCode='MA' then 1 else 0 end as custstate_ma
			, case when cust.CUSTOMER_STATE='OR' or ac.StateCode='OR' then 1 else 0 end as custstate_or
			, case when cust.CUSTOMER_STATE='WA' or ac.StateCode='WA' then 1 else 0 end as custstate_wa
			, case when nyz.zip is not null then 1 else 0 end as custstate_ny
			, case when cust.CUSTOMER_STATE='WV' or ac.StateCode='WV' then 1 else 0 end as custstate_wv
			, case when cust.CUSTOMER_STATE='DC' or ac.StateCode='DC' then 1 else 0 end as custstate_dc
/*			
			, case when fct.PHONE_TYPE in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE') then 1 else 0 end as phonetype_home
			, case when fct.PHONE_TYPE in('DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE') then 1 else 0 end as phonetype_poe
			, case when fct.PHONE_TYPE not in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE','DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE')
												 and fct.PHONE_TYPE is not null 
												 and len(rtrim(fct.phone_type))>0 then 1 else 0 end as phonetype_other
*/
			-- Phone type flags (config-driven)
            , CASE WHEN pt.PhoneType = 'HOME' THEN 1 ELSE 0 END AS phonetype_home
            , CASE WHEN pt.PhoneType = 'POE'  THEN 1 ELSE 0 END AS phonetype_poe
			, CASE WHEN pt.PhoneType IS NULL and fct.PHONE_TYPE is not null and len(rtrim(fct.PHONE_TYPE))>0 then 1 else 0 end as phonetype_other
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
		 CLIENT_ANALYTICS.dbo.AreaCodes ac WITH (NOLOCK) ON left(fct.CALL_OUT_PHONE_NUMBER,3) = ac.AreaCode AND ac.Is_FACS = 1
		      inner join
		 DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK) on cust.CLIENT_ID=tcs.Client_ID
			  left outer join
		 CLIENT_ANALYTICS.dbo.zip_scf zs WITH (NOLOCK) on fct.zip_scf = zs.scf
		      left outer join
		 CLIENT_ANALYTICS.dbo.zipcodes nyz WITH (NOLOCK) on fct.zip=nyz.zip AND nyz.customerstate = 'NY'
		 -- Config joins
              LEFT JOIN 
		 CLIENT_ANALYTICS.dbo.Dial_Excpt_PhoneType pt WITH (NOLOCK) ON fct.PHONE_TYPE = pt.PhonePosition AND pt.Is_FACS = 1
		--     LEFT JOIN 
		--CLIENT_ANALYTICS.dbo.Dial_Excpt_State_Rules csr WITH (NOLOCK) ON zs.CustomerState = csr.state_code OR ac.StateCode = csr.state_code 
				     LEFT JOIN 
		CLIENT_ANALYTICS.dbo.Dial_Excpt_State_Rules csrZIP WITH (NOLOCK) ON zs.CustomerState = csrZIP.state_code 
				     LEFT JOIN 
		CLIENT_ANALYTICS.dbo.Dial_Excpt_State_Rules csrAREA WITH (NOLOCK) ON  ac.StateCode = csrAREA.state_code

	WHERE fct.CALL_DATE > isnull(@StartDateTime,GETDATE())-31
		  and fct.CUSTOMER_ID not in(0,12345)		--ignore missing account number recs
		  AND fct.CALL_TYPE <> 'IN'
		  and ISNULL(fct.Data_Source,'') = 'NGLV'
/*
		  and (
				  (zs.customerstate in('MA','WA','NY','WV','DC') or ac.StateCode in('MA','WA','NY','WV','DC'))
				  or
				  (
					  (zs.customerstate ='OR' or ac.StateCode ='OR')
					  and
					  fct.PHONE_TYPE in('C POE PHONE')
				  )
			  )
*/
        -- State rules (config-driven)
        AND (
            csrZIP.allow_flag = 1	OR csrAREA.allow_flag = 1							             --allow_flag is 1 for zs.CustomerState in('MA','WA','NY','WV','DC')
            OR ( (csrZIP.poe_only = 1 OR csrAREA.poe_only = 1) AND pt.PhonePosition = 'C POE PHONE')   --right now, poe_only flag is 1 only for csr.state_code = 'OR'
        )
			  			  
			  GROUP BY 
			--fct.CALL_HISTORY_FACT_ID
			--, 
			fct.CLIENT_ID 
			--, fct.CUSTOMER_ID as customerid
			--, KeySourceSystem = 4
			--, 'FACS' as SourceSystem
			, CAST(fct.CALL_DATE AS DATETIME) --callDate ---+ CAST(fct.call_start_time as datetime) as callstarttime
			--, DATEPART(week,call_date) as WeekId
			--, fct.CALL_OUT_PHONE_NUMBER as DialedPhoneNumber
			--, left(fct.CALL_OUT_PHONE_NUMBER,3) as DialedAreaCode
            
			-- State flags           
			, case when cust.CUSTOMER_STATE='MA' or ac.StateCode='MA' then 1 else 0 end 
			, case when cust.CUSTOMER_STATE='OR' or ac.StateCode='OR' then 1 else 0 end 
			, case when cust.CUSTOMER_STATE='WA' or ac.StateCode='WA' then 1 else 0 end 
			, case when nyz.zip is not null then 1 else 0 end 
			, case when cust.CUSTOMER_STATE='WV' or ac.StateCode='WV' then 1 else 0 end 
			, case when cust.CUSTOMER_STATE='DC' or ac.StateCode='DC' then 1 else 0 end 
/*			
			, case when fct.PHONE_TYPE in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE') then 1 else 0 end 
			, case when fct.PHONE_TYPE in('DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE') then 1 else 0 end 
			, case when fct.PHONE_TYPE not in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE','DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE')
												 and fct.PHONE_TYPE is not null 
												 and len(rtrim(fct.phone_type))>0 then 1 else 0 end 
*/
			-- Phone type flags (config-driven)
            , CASE WHEN pt.PhoneType = 'HOME' THEN 1 ELSE 0 END
            , CASE WHEN pt.PhoneType = 'POE'  THEN 1 ELSE 0 END
			, CASE WHEN pt.PhoneType IS NULL and fct.PHONE_TYPE is not null and len(rtrim(fct.PHONE_TYPE))>0 then 1 else 0 end
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