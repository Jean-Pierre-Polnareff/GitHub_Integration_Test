

CREATE PROCEDURE [dbo].[sp_insert_fact_dial_excpt_cfpb_facs_call_Attempts]
	
	@StartDateTime DATETIME = NULL
	

AS
/* 
Object: sp_insert_fact_dial_excpt_cfpb_facs_call_attempts

Description: Identify and insert call attempts for dialer exceptions for CFPB new rules from FACS portal into fact_dial_expt_call_attempts

Author			Date		Description
Amod Ramugade	03/14/2023	Created
*/

BEGIN
	SET NOCOUNT ON;

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_expt_call_attempts] for last 31 days------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_expt_call_attempts]
WHERE calldate  > isnull(@StartDateTime,GETDATE())-31
AND dlr_excpt_id  IN (9,10) 
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
			, CAST(fct.CALL_DATE AS DATE) calldate
			--, DATEPART(week,call_date) as WeekId
			--, fct.CALL_OUT_PHONE_NUMBER as DialedPhoneNumber
			--, left(fct.CALL_OUT_PHONE_NUMBER,3) as DialedAreaCode
			--, case when cust.CUSTOMER_STATE='MA' or ac.StateCode='MA' then 1 else 0 end as custstate_ma
			--, case when cust.CUSTOMER_STATE='OR' or ac.StateCode='OR' then 1 else 0 end as custstate_or
			--, case when cust.CUSTOMER_STATE='WA' or ac.StateCode='WA' then 1 else 0 end as custstate_wa
			--, case when nyz.zip is not null then 1 else 0 end as custstate_ny
			--, case when cust.CUSTOMER_STATE='WV' or ac.StateCode='WV' then 1 else 0 end as custstate_wv
			--, case when fct.PHONE_TYPE in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE') then 1 else 0 end as phonetype_home
			--, case when fct.PHONE_TYPE in('DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE') then 1 else 0 end as phonetype_poe
			--, case when fct.PHONE_TYPE not in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE','DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE')
			--									 and fct.PHONE_TYPE is not null 
			--									 and len(rtrim(fct.phone_type))>0 then 1 else 0 end as phonetype_other
			--, case when fct.RIGHT_PARTY_CONTACT='Y' then 1 else 0 end as contact_flag
			--, fct.EMPLOYEE_ID as EmployeeID
			--, fct.CALL_DURATION_SECONDS as CallSeconds
			, tcs.Parent as ClientParent
	--12/13 - Keith says RPF/RNF contact_codes make a callback within 7 ok
			, fct.isAdjRPC as IsRPC
			, fct.contact_code
			, count(*) as count_of_recs
	into #calls		
	FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT fct (NOLOCK)
			  inner join 
		 DW_MSTR_DM.dbo.LU_CUSTOMER cust (NOLOCK)ON fct.CUSTOMER_ID = cust.CUSTOMER_ID
		      inner join
		 DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK) on cust.CLIENT_ID=tcs.Client_ID
	WHERE fct.CALL_DATE > isnull(@StartDateTime,GETDATE())-31
		  and fct.CUSTOMER_ID not in(0,12345)		--ignore missing account number recs
		  AND fct.CALL_TYPE <> 'IN'
		  and ISNULL(fct.Data_Source,'') = 'NGLV'
		 --11/30/21 TedM/Keith add 3 term codes to suppress
		  AND fct.TERMINATION_CODE NOT IN('3T','BU','FX')
/*
		 --all clients eligible for calls 11/30+
	      AND (
		       fct.CALL_DATE>='11/30/21'		  
		  --12/13/21 per BobR, remove testing period from reporting process
		  /*
		       OR
		  --checking for specific clients during specific dates
		       (
			     (fct.CALL_DATE>='10/11/21' AND fct.CLIENT_ID='WEC1')
				 OR
                 (fct.CALL_DATE>='11/1/21' AND tcs.parent='Onemain')
			   )
		  */
			  )
*/
		 GROUP BY 
		 fct.CLIENT_ID 
			--, fct.CUSTOMER_ID as customerid
			--, KeySourceSystem 
			--, 'FACS' 
			, CAST(fct.CALL_DATE AS DATE) 
			--, DATEPART(week,call_date) as WeekId
			--, fct.CALL_OUT_PHONE_NUMBER as DialedPhoneNumber
			--, left(fct.CALL_OUT_PHONE_NUMBER,3) as DialedAreaCode
			--, case when cust.CUSTOMER_STATE='MA' or ac.StateCode='MA' then 1 else 0 end as custstate_ma
			--, case when cust.CUSTOMER_STATE='OR' or ac.StateCode='OR' then 1 else 0 end as custstate_or
			--, case when cust.CUSTOMER_STATE='WA' or ac.StateCode='WA' then 1 else 0 end as custstate_wa
			--, case when nyz.zip is not null then 1 else 0 end as custstate_ny
			--, case when cust.CUSTOMER_STATE='WV' or ac.StateCode='WV' then 1 else 0 end as custstate_wv
			--, case when fct.PHONE_TYPE in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE') then 1 else 0 end as phonetype_home
			--, case when fct.PHONE_TYPE in('DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE') then 1 else 0 end as phonetype_poe
			--, case when fct.PHONE_TYPE not in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE','DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE')
			--									 and fct.PHONE_TYPE is not null 
			--									 and len(rtrim(fct.phone_type))>0 then 1 else 0 end as phonetype_other
			--, case when fct.RIGHT_PARTY_CONTACT='Y' then 1 else 0 end as contact_flag
			--, fct.EMPLOYEE_ID as EmployeeID
			--, fct.CALL_DURATION_SECONDS as CallSeconds
			, tcs.Parent 
	--12/13 - Keith says RPF/RNF contact_codes make a callback within 7 ok
			, fct.isAdjRPC 
			, fct.contact_code
		 
		  ;


	--EXCEPTIONS CALL ATTEMPTS
	--identify call attempts for each exception rule


	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlr_expt_id=9:  CFPB - 7 attempts per 7 sliding
		SELECT 9 AS dlr_excpt_id
			   , c.clientid
			   , c.keysourcesystem
			   , c.sourcesystem
			   , c.calldate
			   , c.ClientParent
			   , c.count_of_recs
        INTO #exceptions
		FROM #calls c
		

		UNION
		--dlr_expt_id=10:  CFPB - Post-RPC 0 attempts 7 days
		SELECT 10 AS dlr_excpt_id
			   , c.clientid
			   , c.keysourcesystem
			   , c.sourcesystem
			   , c.calldate
			   , c.ClientParent
			   , c.count_of_recs
		FROM #calls c
		    where   c.IsRPC=1
				 and right(c.contact_code,1)<>'F'
	

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
			   FROM #exceptions

END;