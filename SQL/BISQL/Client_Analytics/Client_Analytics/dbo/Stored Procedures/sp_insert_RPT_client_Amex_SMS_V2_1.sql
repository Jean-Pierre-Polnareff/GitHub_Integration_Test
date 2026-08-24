



CREATE PROCEDURE [dbo].[sp_insert_RPT_client_Amex_SMS_V2]

AS
/* 
Object: sp_insert_RPT_client_Amex_SMS_V2

Description: Remove last 15 call_dates from RPT_client_Amex_SMS_V2 and reinsert

Author			Date		Description
Mike Campbell	09/10/2021	Created
*/

BEGIN
	SET NOCOUNT ON;

	DECLARE @start DATE=CAST(GETDATE()-15 AS DATE);

	--#calls
	--grab all SMS, and all inbound calls resulting from an SMS for reporting period
		DROP TABLE IF EXISTS #calls

		SELECT fcc.*
			   , rc.Call_Date AS sms_date
	           , DATEPART(HOUR,rc.Call_Connect_Time_CT) AS sms_hour_CT
			   , CASE WHEN rc.Livevox_Result='SMS MT Delivered' THEN 1 ELSE 0 END AS sms_delivered_flag
			   , CASE WHEN rc.Livevox_Result='SMS MT Failed' THEN 1 ELSE 0 END AS sms_failed_flag
			   , CASE WHEN rc.Livevox_Result LIKE '%Consumer responded Stop to text%' THEN 1 ELSE 0 END AS sms_stopped_flag
			   , CASE WHEN rc.Service_Name = 'Amex Prime Click to SMS Outbound' THEN 1 ELSE 0 end AS sms_servicename_flag
			   , CASE WHEN rc.Service_Name LIKE '103_Inbound_SMS' THEN 1 ELSE 0 END AS inbound_sms_call_flag
		INTO #calls
		FROM DW_MSTR_DM.dbo.RadiusCall rc (NOLOCK)
			   JOIN
			 DW_MSTR_DM.dbo.FactCustomerCall fcc (NOLOCK) ON rc.Session_Id=fcc.SessionId
		WHERE rc.Call_Date >=@start
			  AND rc.LV_Client_Name = 'Veldos'
			  AND rc.Service_Name IN('Amex Prime Click to SMS Outbound','103_Inbound_SMS')

		CREATE INDEX kc ON #calls(KeyCustomer)
		CREATE INDEX kdcd ON #calls(KeyDate_CallDate)
		CREATE INDEX kcc ON #calls(keycustomercall)


	--#fcc_anycall
	--loosely attribute 1st connect, following an SMS, that's within 7 days for an account
		DROP TABLE IF EXISTS #fcc_anycall

		SELECT DISTINCT 
			   s.sms_date
			   , s.KeyCustomer
			   , s.KeyCustomerCall AS sms_KeyCustomerCall
			   , fcc.KeyDate_CallDate
			   , fcc.IsConnect
			   , fcc.IsRPC
			   , fcc.IsPromise
			   , fcc.KeyCustomerCall
			   , fcc.KeyDate_CallDate-s.KeyDate_CallDate AS date_call_from_sms
			   , ROW_NUMBER() OVER(PARTITION BY fcc.KeyCustomerCall ORDER BY fcc.KeyDate_CallDate-s.KeyDate_CallDate) rank_call_fcc
			   , ROW_NUMBER() OVER(PARTITION BY s.KeyCustomerCall ORDER BY fcc.KeyDate_CallDate-s.KeyDate_CallDate) rank_call_sms
			   , rc.Livevox_Result
			   , rc.Service_Name
			   , rc.Phone_Dialed
			   , rc.Phone_Number
		INTO #fcc_anycall
		FROM #calls s
			   LEFT JOIN
			 DW_MSTR_DM.dbo.FactCustomerCall fcc (NOLOCK) ON s.KeyCustomer=fcc.KeyCustomer
										 AND fcc.KeyDate_CallDate-s.KeyDate_CallDate BETWEEN 0 AND 7   --within 7 days from SMS
										 AND s.sms_delivered_flag=1
										 AND s.KeyCustomerCall<>fcc.KeyCustomerCall
			   LEFT JOIN
			 DW_MSTR_DM.dbo.RadiusCall rc (NOLOCK) ON fcc.SessionId=rc.Session_Id
		WHERE fcc.KeyCustomer>0
			  AND fcc.KeyDate_CallDate>0
			  AND s.sms_servicename_flag=1


	--qry above too slow when searching on IsConnect so deleting after fact
		DELETE FROM #fcc_anycall WHERE isconnect=0


	--remove duplicate call assigns to multiple SMS sent by choosing SMS closest to call
		--first remove recs where call from FCC is assigned to multiple SMS by choosing closest
		DELETE FROM #fcc_anycall WHERE rank_call_fcc>1
		--second remove recs where SMS joins to multiple calls by choosing the closest
		DELETE FROM #fcc_anycall WHERE rank_call_sms>1


	--fcc_tfn
	--attribute SMS services inbound responses to originating SMS
		DROP TABLE IF EXISTS #fcc_tfn

		SELECT DISTINCT 
			   s.sms_date
			   , s.KeyCustomer
			   , s.KeyCustomerCall AS sms_KeyCustomerCall
			   , s2.KeyDate_CallDate
			   , s2.IsConnect
			   , s2.IsRPC
			   , s2.IsPromise
			   , s2.KeyCustomerCall
			   , s2.KeyDate_CallDate-s.KeyDate_CallDate AS date_call_from_sms
			   , ROW_NUMBER() OVER(PARTITION BY s2.KeyCustomerCall ORDER BY s2.KeyDate_CallDate-s.KeyDate_CallDate) rank_call_s2
			   , ROW_NUMBER() OVER(PARTITION BY s.KeyCustomerCall ORDER BY s2.KeyDate_CallDate-s.KeyDate_CallDate) rank_call_sms
			   , NULL AS Livevox_Result
			   , NULL AS Service_Name
			   , NULL AS Phone_Dialed
			   , NULL AS Phone_Number
		INTO #fcc_tfn
		FROM #calls s
			   JOIN
			 #calls s2 ON s.KeyCustomer=s2.KeyCustomer
						  AND s.KeyCustomerCall<>s2.KeyCustomerCall
						  AND s2.KeyDate_CallDate-s.KeyDate_CallDate BETWEEN 0 AND 7
		WHERE s2.inbound_sms_call_flag=1


	--remove duplicate call assigns to multiple SMS sent by choosing SMS closest to call
		--first remove recs where call from FCC is assigned to multiple SMS by choosing closest
		DELETE FROM #fcc_tfn WHERE rank_call_s2>1
		--second remove recs where SMS joins to multiple calls by choosing the closest
		DELETE FROM #fcc_tfn WHERE rank_call_sms>1

	--#fcc
	--combine loosely attributed call responses with inbound SMS attributed call responses
		DROP TABLE IF EXISTS #fcc

		SELECT DISTINCT ISNULL(fa.sms_date,ft.sms_date) AS sms_date
			   , ISNULL(fa.KeyCustomer,ft.KeyCustomer) AS KeyCustomer
			   , ISNULL(fa.sms_KeyCustomerCall,ft.sms_KeyCustomerCall) AS sms_KeyCustomerCall
			   , ISNULL(fa.IsConnect,ft.IsConnect) AS IsConnect
			   , ISNULL(fa.IsRPC,ft.IsRPC) AS IsRPC
			   , ISNULL(fa.IsPromise,ft.IsPromise) AS IsPromise
			   , ISNULL(fa.KeyCustomerCall,ft.KeyCustomerCall) AS response_KeyCustomerCall
			   , ISNULL(fa.date_call_from_sms,ft.date_call_from_sms) AS date_call_from_sms
		INTO #fcc
		FROM #fcc_anycall fa
				FULL JOIN
			 #fcc_tfn ft ON fa.sms_KeyCustomerCall=ft.sms_KeyCustomerCall

	--#conv_anycall
	--conversions within 15 days from loosely attributed RPCs
		DROP TABLE IF EXISTS #conv_anycall

		SELECT f.sms_date
			   , f.sms_KeyCustomerCall
			   , f.KeyCustomer
			   , fcp.KeyCustomerPayment
			   , fcp.KeyDate_PaymentDate
			   , f.KeyDate_CallDate
			   , fcp.PaymentAppliedAmt
			   , fcp.KeyDate_PaymentDate-f.KeyDate_CallDate AS datediff_pay_call
			   , ROW_NUMBER() OVER(PARTITION BY fcp.KeyCustomerPayment ORDER BY fcp.KeyDate_PaymentDate-f.KeyDate_CallDate) AS rank_pay
		INTO #conv_anycall
		FROM #fcc_anycall f 
			   LEFT JOIN
			 DW_MSTR_DM.dbo.FactCustomerPayment fcp (NOLOCK) ON f.KeyCustomer=fcp.KeyCustomer
										 AND fcp.KeyDate_PaymentDate-f.KeyDate_CallDate BETWEEN 0 AND 15   --within 15 days from SMS
			   LEFT JOIN
			 DW_MSTR_DM.dbo.DimPaymentType dpt (NOLOCK) ON fcp.KeyPaymentType=dpt.KeyPaymentType
			   --LEFT JOIN
			-- DW_MSTR_DM.dbo.FactCustomerPostdate fcpd (NOLOCK) ON s.KeyCustomer=fcpd.KeyCustomer
			   --                                                   AND fcpd.KeyDate_PromiseDueDate>fcp.KeyDate_PaymentDate  --after any payments already counted above
		WHERE f.IsRPC=1 
			  AND f.KeyCustomer IS NOT NULL 
			  AND dpt.PaymentType NOT IN('DA','DAR')
			  AND (dpt.PaymentCategory<>'Adjustment' OR dpt.PaymentCategory IS NULL)

	--remove duplicate pay assigns to multiple SMS sent by choosing payment closest to call
		DELETE FROM #conv_anycall WHERE rank_pay>1


	--#conv_tfn
	--conversions within 15 days from TFN calls
		DROP TABLE IF EXISTS #conv_tfn

		SELECT f.sms_date
			   , f.KeyCustomerCall AS sms_KeyCustomerCall
			   , f.KeyCustomer
			   , fcp.KeyCustomerPayment
			   , fcp.KeyDate_PaymentDate
			   , f.KeyDate_CallDate
			   , fcp.PaymentAppliedAmt
			   , fcp.KeyDate_PaymentDate-f.KeyDate_CallDate AS datediff_pay_call
			   , ROW_NUMBER() OVER(PARTITION BY fcp.KeyCustomerPayment ORDER BY fcp.KeyDate_PaymentDate-f.KeyDate_CallDate) AS rank_pay
		INTO #conv_tfn
		FROM #calls f 
			   LEFT JOIN
			 DW_MSTR_DM.dbo.FactCustomerPayment fcp (NOLOCK) ON f.KeyCustomer=fcp.KeyCustomer
										 AND fcp.KeyDate_PaymentDate-f.KeyDate_CallDate BETWEEN 0 AND 15   --within 15 days from SMS
			   LEFT JOIN
			 DW_MSTR_DM.dbo.DimPaymentType dpt (NOLOCK) ON fcp.KeyPaymentType=dpt.KeyPaymentType
			   --LEFT JOIN
			-- DW_MSTR_DM.dbo.FactCustomerPostdate fcpd (NOLOCK) ON s.KeyCustomer=fcpd.KeyCustomer
			   --                                                   AND fcpd.KeyDate_PromiseDueDate>fcp.KeyDate_PaymentDate  --after any payments already counted above
		WHERE f.inbound_sms_call_flag=1 
			  AND f.KeyCustomer IS NOT NULL 
			  AND dpt.PaymentType NOT IN('DA','DAR')
			  AND (dpt.PaymentCategory<>'Adjustment' OR dpt.PaymentCategory IS NULL)

	--remove duplicate pay assigns to multiple SMS sent by choosing payment closest to call
		DELETE FROM #conv_tfn WHERE rank_pay>1

	--#conv
	--combine payments from anycall and inbound sms TFN
		DROP TABLE IF EXISTS #conv

		SELECT DISTINCT ISNULL(ca.sms_date,ct.sms_date) AS sms_date
			   , ISNULL(ca.KeyCustomer,ct.KeyCustomer) AS KeyCustomer
			   , ISNULL(ca.sms_KeyCustomerCall,ct.sms_KeyCustomerCall) AS sms_KeyCustomerCall
			   , ISNULL(ca.KeyCustomerPayment,ct.KeyCustomerPayment) AS KeyCustomerPayment
			   , ISNULL(ca.KeyDate_PaymentDate,ct.KeyDate_PaymentDate) AS KeyDate_PaymentDate
			   , ISNULL(ca.KeyDate_CallDate,ct.KeyDate_CallDate) AS KeyDate_CallDate
			   , ISNULL(ca.PaymentAppliedAmt,ct.PaymentAppliedAmt) AS PaymentAppliedAmt
		INTO #conv
		FROM #conv_anycall ca
			   FULL JOIN
			 #conv_tfn ct ON ca.KeyCustomerPayment=ct.KeyCustomerPayment


	--future promise dollars for conversions above
		DROP TABLE IF EXISTS #prom

		SELECT c.sms_KeyCustomerCall
			   , c.KeyCustomer
			   , c.KeyCustomerPayment
			   , SUM(fcpd.PromiseDueAmt) AS future_promise_dollars
		INTO #prom
		FROM #conv c
			   JOIN
			 DW_MSTR_DM.dbo.FactCustomerPostdate fcpd (NOLOCK) ON c.KeyCustomer=fcpd.KeyCustomer
																  AND fcpd.KeyDate_PromiseDueDate>c.KeyDate_PaymentDate  --after any payments already counted above
		GROUP BY c.sms_KeyCustomerCall
				 , c.KeyCustomer     
				 , c.KeyCustomerPayment


	--summary
		DELETE from CLIENT_ANALYTICS.dbo.RPT_client_Amex_SMS_V2
		 WHERE sms_date>=@start;

		INSERT INTO CLIENT_ANALYTICS.dbo.RPT_client_Amex_SMS_V2
		(
		  sms_date,
		  KeyClient,
		  KeySourceSystem,
		  sms_hour_CT,
		  sms_delivered,
		  sms_failed,
		  sms_stopped,
		  call_connects,
		  call_rpcs,
		  call_promises,
		  conversions,
		  conversion_dollars,
		  future_promise_dollars,
		  InsertDate
		)
		SELECT s.sms_date
               , s.KeyClient
	           , s.KeySourceSystem
	           , s.sms_hour_CT
			   , SUM(s.sms_delivered_flag) AS sms_delivered
			   , SUM(s.sms_failed_flag) AS sms_failed
			   , SUM(s.sms_stopped_flag) AS sms_stopped
			   , SUM(f.IsConnect) AS call_connects
			   , SUM(f.IsRPC) AS call_rpcs
			   , SUM(f.IsPromise) AS call_promises
			   , COUNT(c.KeyCustomerPayment) AS conversions
			   , sum(c.PaymentAppliedAmt) AS conversion_dollars
			   , sum(p.future_promise_dollars) AS future_promise_dollars
			   , GETDATE() AS InsertDate
		FROM #calls s
			   LEFT JOIN
			 #conv c ON s.KeyCustomerCall=c.sms_KeyCustomerCall
			   LEFT JOIN
			 #prom p ON s.KeyCustomerCall=p.sms_KeyCustomerCall
			   LEFT join
			 #fcc f ON s.KeyCustomerCall=f.sms_KeyCustomerCall
		WHERE s.inbound_sms_call_flag=0
		GROUP BY s.sms_date
                 , s.KeyClient
	             , s.KeySourceSystem
	             , s.sms_hour_CT


END;
GO



GO



GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO



GO



GO



GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_insert_RPT_client_Amex_SMS_V2] TO [CORP\aughodake]
    AS [dbo];

