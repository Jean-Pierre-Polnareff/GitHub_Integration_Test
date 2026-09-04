





--SET QUOTED_IDENTIFIER ON
--GO




CREATE PROCEDURE  [dbo].[sp_insert_Client_Amex_Non_RegF_dials_to_cfpb]
    
	 @StartDateTime DATETIME = NULL
	 
	
AS
/*
Object: sp_insert_Client_Amex_Non_RegF_dials_to_cfpb

Description: Identify and insert Amex 1P and 3P calls dialed to Reg F Customers from Non Reg F Call Centers 

Author			Date		Description
Amod Ramugade	01/20/2026	Created

*/
 
 BEGIN
	SET NOCOUNT ON;

	--DECLARE  @StartDateTime DATETIME = NULL;

----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------
	
		DROP TABLE IF EXISTS #calls;

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
			, dcl.ClientParentGroup AS ClientParent
			, fct.IsRPC
			, rc.Call_Center_Name
			, lup.FirstPartyFlag
			, ISNULL(dcp.ProductType,'') AS ProductType
			, CASE WHEN RIGHT(cust.consumerid,3) = 'USD' THEN cust.consumerid + (CASE WHEN lup.FirstPartyFlag =1 THEN '-1P' ELSE '-3P' END) ELSE cust.ConsumerId END ConsumerID
			
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
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left outer join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		 		      LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.DimCustomerProduct dcp (NOLOCK) on cust.KeyCustomer=dcp.KeyCustomer 
		  left join 
		 [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup on lup.ClientCode= cust.ClientId
                                                                                    and cust.SourceSystem= 'AMEX Latitude'
         WHERE ISNULL(dcp.ProductType,'') NOT IN ('EX','SB','SM','SR','AB','HC','HP','BT','CB','CC','CP','CR','DV')
	AND dt.CalendarDate >= isnull(@StartDateTime,GETDATE())-31                
	    AND fct.IsOutbound=1
	    AND rc.LV_Client_Name = 'Veldos'
		and rc.service_name not like '%HTI%'
		AND rc.Livevox_Result not like '%SMS%'
		AND rc.Call_Center_Name IN ('Non Reg F', 'Non Reg F 1st party')
		;

		  --------------------------------------------------Insert to Client_Amex_Non_RegF_dials_to_cfpb--------------------------------------------------

	INSERT INTO CLIENT_ANALYTICS.dbo.Client_Amex_Non_RegF_dials_to_cfpb(keycustomercall,customerid,clientid,keysourcesystem,sourcesystem,calldate,sessionid,insert_date,
	                                               DialedPhoneNumber,CallStartTime,EmployeeID,CallSeconds,ClientParent,FirstPartyFlag,Call_Center_Name,ProductType,ConsumerID)
	SELECT 
			 
			     c.keycustomercall
			   , c.customerid
			   , c.clientid
			   , c.KeySourceSystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
		       , c.DialedPhoneNumber
		       , c.CallStartTime
		       , c.EmployeeID
		       , c.CallSeconds
		       , c.ClientParent
			   , c.FirstPartyFlag
			   , c.Call_Center_Name
			   , c.ProductType
			   , c.ConsumerID

		   FROM #calls c 
			LEFT OUTER JOIN
		 CLIENT_ANALYTICS.dbo.Client_Amex_Non_RegF_dials_to_cfpb canrdc (NOLOCK) 
		 ON c.keycustomercall = canrdc.keycustomercall
	WHERE canrdc.keycustomercall IS NULL;
		  -- WHERE c.Call_Center_Name IN ('Non Reg F', 'Non Reg F 1st party')
END;