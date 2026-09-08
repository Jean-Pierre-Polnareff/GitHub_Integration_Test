

CREATE PROCEDURE [dbo].[sp_INS_RPT_Web_Waterfall_New_Monthly_Table_Pivot]
    @startdate DATE = NULL
AS
BEGIN

SET NOCOUNT ON;

SET @startdate = ISNULL(@startdate, DATEADD(dd,-60,CAST(GETDATE() AS DATE)));

DELETE FROM CLIENT_ANALYTICS.dbo.RPT_Web_Waterfall_New_Monthly_Table_Pivot
WHERE CAST(CapturedOn AS DATE) >= @startdate;


DROP TABLE IF EXISTS #RPT_Web_Waterfall_New_Monthly_Table;
DROP TABLE IF EXISTS #t;
DROP TABLE IF EXISTS #FlagFix;
DROP TABLE IF EXISTS #PivotFinalTable;


SELECT *
INTO #RPT_Web_Waterfall_New_Monthly_Table
FROM CLIENT_ANALYTICS.dbo.RPT_Web_Waterfall_New_Monthly_Table (NOLOCK)
WHERE CAST(CapturedOn AS DATE) >= @startdate;


SELECT 
    MetricId
   ,[Key]
   ,CapturedOn
   ,[Count]
   ,ReferenceNumber
   ,Payments
   ,ClientId
   ,ClientParent
   ,IpAddress
   ,SessionId
   ,UTM_Source
   ,UTM_Campaign
   ,SourceSystem
   ,CustomerID
   ,PaySeriesAmount
   ,PaySeriesCount
   ,ISNULL([AXP Payment Programs],0) AS is_Axp_payment_programs
   ,ISNULL([Checks],0) AS is_checks
   ,ISNULL([Comaker],0) AS is_comaker
   ,ISNULL([Credit Cards],0) AS is_credit_cards
   ,ISNULL([Debit Cards],0) AS is_debit_cards
   ,ISNULL([Email],0) AS is_email
   ,ISNULL([Frictionless],0) AS is_frictionless
   ,ISNULL([Future Payment $ Set Up],0) AS is_future_payment_dollar_set_up
   ,ISNULL([Future Payments Set Up],0) AS is_future_payments_set_up
   ,ISNULL([installment_submitted],0) AS is_installment_submitted
   ,ISNULL([one_time_payment_submitted],0) AS is_one_time_payment_submitted
   ,ISNULL([pay_in_full_submitted],0) AS is_pay_in_full_submitted
   ,ISNULL([PIN],0) AS is_pin
   ,ISNULL([Primary],0) AS is_primary
   ,ISNULL([reocurring_submitted],0) AS is_recurring_submitted
   ,ISNULL([settlement_submitted],0) AS is_settlement_submitted
   ,ISNULL([SMS],0) AS is_sms
   ,ISNULL([SSN/DoB Failed],0) AS is_ssn_dob_failed
   ,ISNULL([SSN/DoB Succeeded],0) AS is_ssn_dob_succeeded
   ,ISNULL([Unspecified],0) AS is_unspecified
INTO #t
FROM (
    SELECT *,
           1 AS flag
    FROM #RPT_Web_Waterfall_New_Monthly_Table
) AS src
PIVOT (
    MAX(flag)
    FOR Campaign IN (
        [AXP Payment Programs]
        ,[Checks]
        ,[Comaker]
        ,[Credit Cards]
        ,[Debit Cards]
        ,[Email]
        ,[Frictionless]
        ,[Future Payment $ Set Up]
        ,[Future Payments Set Up]
        ,[installment_submitted]
        ,[one_time_payment_submitted]
        ,[pay_in_full_submitted]
        ,[PIN]
        ,[Primary]
        ,[reocurring_submitted]
        ,[settlement_submitted]
        ,[SMS]
        ,[SSN/DoB Failed]
        ,[SSN/DoB Succeeded]
        ,[Unspecified]
    )
) AS pvt;

CREATE CLUSTERED INDEX IX_t_SessionId
ON #t (SessionId);

WITH FlagFix AS       -----------------Flag fixing is done because some sessionids we getting generic after midnight 
(
    SELECT *,
           MAX(is_Axp_payment_programs)   OVER (PARTITION BY SessionId) AS fix_is_Axp_payment_programs
          ,MAX(is_checks)                 OVER (PARTITION BY SessionId) AS fix_is_checks
          ,MAX(is_comaker)                OVER (PARTITION BY SessionId) AS fix_is_comaker
          ,MAX(is_credit_cards)           OVER (PARTITION BY SessionId) AS fix_is_credit_cards
          ,MAX(is_debit_cards)            OVER (PARTITION BY SessionId) AS fix_is_debit_cards
          ,MAX(is_email)                  OVER (PARTITION BY SessionId) AS fix_is_email
          ,MAX(is_frictionless)           OVER (PARTITION BY SessionId) AS fix_is_frictionless
          ,MAX(is_future_payment_dollar_set_up) OVER (PARTITION BY SessionId) AS fix_is_future_payment_dollar_set_up
          ,MAX(is_future_payments_set_up) OVER (PARTITION BY SessionId) AS fix_is_future_payments_set_up
          ,MAX(is_installment_submitted)  OVER (PARTITION BY SessionId) AS fix_is_installment_submitted
          ,MAX(is_one_time_payment_submitted) OVER (PARTITION BY SessionId) AS fix_is_one_time_payment_submitted
          ,MAX(is_pay_in_full_submitted)  OVER (PARTITION BY SessionId) AS fix_is_pay_in_full_submitted
          ,MAX(is_pin)                    OVER (PARTITION BY SessionId) AS fix_is_pin
          ,MAX(is_primary)                OVER (PARTITION BY SessionId) AS fix_is_primary
          ,MAX(is_recurring_submitted)    OVER (PARTITION BY SessionId) AS fix_is_recurring_submitted
          ,MAX(is_settlement_submitted)   OVER (PARTITION BY SessionId) AS fix_is_settlement_submitted
          ,MAX(is_sms)                    OVER (PARTITION BY SessionId) AS fix_is_sms
          ,MAX(is_ssn_dob_failed)         OVER (PARTITION BY SessionId) AS fix_is_ssn_dob_failed
          ,MAX(is_ssn_dob_succeeded)      OVER (PARTITION BY SessionId) AS fix_is_ssn_dob_succeeded
          ,MAX(is_unspecified)            OVER (PARTITION BY SessionId) AS fix_is_unspecified
    FROM #t
)
SELECT
    MetricId,
    [Key],
    CapturedOn,
    [Count],
    ReferenceNumber,
    Payments,
    ClientId,
    ClientParent,
    IpAddress,
    SessionId,
    UTM_Source,
    UTM_Campaign,
    SourceSystem,
    CustomerID,
    PaySeriesAmount,
    PaySeriesCount,
    MAX(fix_is_Axp_payment_programs)   AS is_Axp_payment_programs,
    MAX(fix_is_checks)                 AS is_checks,
    MAX(fix_is_comaker)                AS is_comaker,
    MAX(fix_is_credit_cards)           AS is_credit_cards,
    MAX(fix_is_debit_cards)            AS is_debit_cards,
    MAX(fix_is_email)                  AS is_email,
    MAX(fix_is_frictionless)           AS is_frictionless,
    MAX(fix_is_future_payment_dollar_set_up) AS is_future_payment_dollar_set_up,
    MAX(fix_is_future_payments_set_up) AS is_future_payments_set_up,
    MAX(fix_is_installment_submitted)  AS is_installment_submitted,
    MAX(fix_is_one_time_payment_submitted) AS is_one_time_payment_submitted,
    MAX(fix_is_pay_in_full_submitted)  AS is_pay_in_full_submitted,
    MAX(fix_is_pin)                    AS is_pin,
    MAX(fix_is_primary)                AS is_primary,
    MAX(fix_is_recurring_submitted)    AS is_recurring_submitted,
    MAX(fix_is_settlement_submitted)   AS is_settlement_submitted,
    MAX(fix_is_sms)                    AS is_sms,
    MAX(fix_is_ssn_dob_failed)         AS is_ssn_dob_failed,
    MAX(fix_is_ssn_dob_succeeded)      AS is_ssn_dob_succeeded,
    MAX(fix_is_unspecified)            AS is_unspecified,
    CASE WHEN MAX(fix_is_email)=1 THEN 'Email'
         WHEN MAX(fix_is_sms)=1 THEN 'SMS'
         WHEN MAX(fix_is_unspecified)=1 THEN 'Generic'
         ELSE '' END AS [Channel],
    CASE WHEN MAX(fix_is_checks)=1 THEN 'Checks'
         WHEN MAX(fix_is_debit_cards)=1 THEN 'Debit Cards'
         WHEN MAX(fix_is_credit_cards)=1 THEN 'Credit Cards'
         ELSE 'Generic' END AS [Payment Method],
    CASE WHEN MAX(fix_is_comaker)=1 THEN 'Comaker'
         ELSE 'Primary' END AS [User],
    CASE WHEN MAX(fix_is_ssn_dob_succeeded)=1 THEN 'SSN/DOB'
         WHEN MAX(fix_is_ssn_dob_failed)=1 THEN 'SSN/DOB'
         WHEN MAX(fix_is_frictionless)=1 THEN 'Frictionless'
         WHEN MAX(fix_is_pin)=1 THEN 'PIN'
         ELSE 'Generic' END AS [PIN Method]
INTO #PivotFinalTable
FROM FlagFix
GROUP BY
    MetricId
   ,[Key]
   ,CapturedOn
   ,[Count]
   ,ReferenceNumber
   ,Payments
   ,ClientId
   ,ClientParent
   ,IpAddress
   ,SessionId
   ,UTM_Source
   ,UTM_Campaign
   ,SourceSystem
   ,CustomerID
   ,PaySeriesAmount
   ,PaySeriesCount;


----Finding out Lost SessionIds based on CustomerID using LAG within 60 Min time window which are basically Pinmethods as Generic and has payments

CREATE CLUSTERED INDEX IX_PivotTest_Session
ON #PivotFinalTable (CustomerID, SessionId, CapturedOn);


WITH SessionSummary AS
(
    SELECT  
        CustomerID
       ,SessionId
       ,MIN(CapturedOn) AS CapturedOn
       ,SUM(Payments) AS Payments
       ,MAX([PIN Method]) AS [PIN Method]
       ,MAX(Channel) AS Channel
    FROM #PivotFinalTable
    GROUP BY CustomerID, SessionId
),

AllSessions AS
(
    SELECT *,
        LAG(SessionId) OVER (PARTITION BY CustomerID ORDER BY CapturedOn, SessionId) AS PrevSessionId
       ,LAG(CapturedOn) OVER (PARTITION BY CustomerID ORDER BY CapturedOn, SessionId) AS PrevCapturedOn
       ,LAG(Channel) OVER (PARTITION BY CustomerID ORDER BY CapturedOn, SessionId) AS PrevChannel
       ,LAG([PIN Method]) OVER (PARTITION BY CustomerID ORDER BY CapturedOn, SessionId) AS PrevPINMethod
    FROM SessionSummary
),

SessionsToFix AS
(
    SELECT DISTINCT
        SessionId
       ,CustomerID
       ,PrevChannel
       ,PrevPINMethod
    FROM AllSessions
    WHERE [PIN Method] = 'Generic'
      AND Payments > 0
      AND CapturedOn <= DATEADD(MINUTE, 60, PrevCapturedOn)
)

-----Updating those SessionIds with their Previous Channel and PIN Method
UPDATE t
SET 
    t.Channel = f.PrevChannel,
    t.[PIN Method] = f.PrevPINMethod
FROM #PivotFinalTable t
INNER JOIN SessionsToFix f
    ON t.SessionId = f.SessionId
   AND t.CustomerID = f.CustomerID;


----------------------------------------------INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Web_Waterfall_New_Monthly_Table_Pivot-----------------------------------------------------
INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Web_Waterfall_New_Monthly_Table_Pivot
(
		  [MetricId] 
		 ,[Key] 
		 ,[CapturedOn] 
		 ,[Count] 
		 ,[ReferenceNumber]
		 ,[Payments] 
		 ,[ClientId] 
		 ,[ClientParent]
		 ,[IpAddress] 
		 ,[SessionId] 
		 ,[UTM_Source] 
		 ,[UTM_Campaign]
		 ,[SourceSystem]
		 ,[CustomerID]
		 ,[PaySeriesAmount] 
		 ,[PaySeriesCount]
		 ,[is_Axp_payment_programs]
		 ,[is_checks]
		 ,[is_comaker]
		 ,[is_credit_cards] 
		 ,[is_debit_cards]
		 ,[is_email]
		 ,[is_frictionless]
		 ,[is_future_payment_dollar_set_up]
		 ,[is_future_payments_set_up]
		 ,[is_installment_submitted]
		 ,[is_one_time_payment_submitted]
		 ,[is_pay_in_full_submitted]
		 ,[is_pin]
		 ,[is_primary]
		 ,[is_recurring_submitted]
		 ,[is_settlement_submitted]
		 ,[is_sms] 
		 ,[is_ssn_dob_failed] 
		 ,[is_ssn_dob_succeeded]
		 ,[is_unspecified]
		 ,[Channel]
		 ,[Payment Method]
		 ,[User]
		 ,[PIN Method]
	  )
	 SELECT 
	    [MetricId] 
		,[Key] 
		,[CapturedOn] 
		,[Count] 
		,[ReferenceNumber]
		,[Payments] 
		,[ClientId] 
		,[ClientParent]
		,[IpAddress] 
		,[SessionId] 
		,[UTM_Source] 
		,[UTM_Campaign]
		,[SourceSystem]
		,[CustomerID]
		,[PaySeriesAmount] 
		,[PaySeriesCount]
		,[is_Axp_payment_programs]
		,[is_checks]
		,[is_comaker]
		,[is_credit_cards] 
		,[is_debit_cards]
		,[is_email]
		,[is_frictionless]
		,[is_future_payment_dollar_set_up]
		,[is_future_payments_set_up]
		,[is_installment_submitted]
		,[is_one_time_payment_submitted]
		,[is_pay_in_full_submitted]
		,[is_pin]
		,[is_primary]
		,[is_recurring_submitted]
		,[is_settlement_submitted]
		,[is_sms] 
		,[is_ssn_dob_failed] 
		,[is_ssn_dob_succeeded]
		,[is_unspecified]
		,[Channel]
		,[Payment Method]
		,[User]
		,[PIN Method]

		FROM #PivotFinalTable
		
	;

END;
