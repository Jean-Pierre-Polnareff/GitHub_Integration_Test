



/*******************************************************************************************************/
/******************************************************************************************************/
/***************************None Facs - call duration at least 30 seconds*****************************/
/****************************************************************************************************/
/***************************************************************************************************/

--drop PROCEDURE RPT_Daily_calls_for_callminer
CREATE PROCEDURE [dbo].[sp_ins_RPT_calls_for_callminer] AS 

SET NOCOUNT ON;

DECLARE @call_date DATETIME
SET @call_date=CAST(DATEADD(DAY,-5,GETDATE()) AS DATE )

DECLARE @last24month DATETIME
SET @last24month=CAST(DATEADD(month,-24,GETDATE()) AS DATE )

--6/8/21, per Bob/Pulkit, don't remove/replace recent calls.  Let history stack up so Pulkit can find updates on his side
----   Also... remove any calls over 90 days old
DELETE  FROM CLIENT_ANALYTICS.dbo.RPT_calls_for_callminer WHERE Call_date < DATEADD(DAY,-90,CAST(GETDATE() AS DATE))

/**************************************************************************************************************************************************/
------------------------------------------Step 1. Find all customer who call duration at least 30 seconds in past 90 days  ------------
--TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_calls_for_callminer
IF OBJECT_ID('TempDB..#list_customer') IS NOT NULL
DROP TABLE #list_customer;


SELECT fcc.keycustomer,
       dcu.CustomerId,
       CallStartTime,
       dd.CalendarDate,
       SessionId,
       IsRPC,
       KeySourceSystem,
       fcc.DialedPhoneNumber,
       fcc.IsPromise ,
       ClientId,
       SourceSystem ,
	   CurrentBalance,
	   StatusCode,
	   DATEDIFF(YEAR,BirthDate,GETDATE()) AS BirthDate
INTO #list_customer 
                    FROM DW_MSTR_DM.dbo.FactCustomerCall fcc (NOLOCK)
                    JOIN DW_MSTR_DM.dbo.DimDate dd (NOLOCK) ON fcc.KeyDate_CallDate=dd.KeyDate 
                    LEFT JOIN  DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON fcc.KeyCustomer=dcu.KeyCustomer
				
WHERE fcc.CallSeconds >=30 AND dd.CalendarDate>@call_date


CREATE INDEX #kc ON #list_customer(KeyCustomer);
---------------------------------------------find lastest RPC Date------------------------------------------------------------------------------
IF OBJECT_ID('TempDB..#RPC') IS NOT NULL
DROP TABLE #RPC;

SELECT fcc.keycustomer,fcc.CallStartTime, fcc.IsRPC, CASE WHEN fcc.IsRPC=1 THEN dd.CalendarDate ELSE NULL END AS LastRPCDate INTO #RPC
FROM DW_MSTR_DM.dbo.FactCustomerCall fcc (NOLOCK) JOIN DW_MSTR_DM.dbo.DimDate dd ON fcc.KeyDate_CallDate=dd.KeyDate WHERE fcc.IsRPC=1 and fcc.CallSeconds > =30  
AND dd.CalendarDate>@last24month 


IF OBJECT_ID('TempDB..#LastRPC') IS NOT NULL
DROP TABLE #LastRPC;

SELECT lct.keycustomer,lct.CallStartTime,MAX(rpc.LastRPCDate) AS LastRPCDate 
              INTO #LastRPC FROM #list_customer lct 
              LEFT JOIN #RPC rpc  ON lct.KeyCustomer=rpc.KeyCustomer 
              WHERE  rpc.LastRPCDate< CalendarDate
              GROUP BY lct.KeyCustomer ,lct.CallStartTime 


------------------------------------------Step 2. Find all customer lastest Letter Date and Letter Date less than Call Date  ----------
IF OBJECT_ID('TempDB..#Letter') IS NOT NULL
DROP TABLE #Letter;


SELECT fcl.KeyCustomer, dd.CalendarDate AS letterdate , 
         CASE WHEN KeySourceSystem='1' THEN 'MEDPROD'
              WHEN KeySourceSystem='4' THEN 'FACS'
			  WHEN KeySourceSystem='3' THEN 'AMEX Latitude'
			  WHEN KeySourceSystem='2' THEN 'ThirdProd' 
			  ELSE 'None'END AS KeySourceSystem
			  INTO #Letter 
			  FROM DW_MSTR_DM.dbo.factcustomerletter fcl (NOLOCK) JOIN DW_MSTR_DM.dbo.DimDate dd (NOLOCK)  ON fcl.KeyDate_MailDate=dd.KeyDate
			  WHERE dd.CalendarDate >@last24month
			  

IF OBJECT_ID('TempDB..#lastLetterDate') IS NOT NULL
DROP TABLE #lastLetterDate;

SELECT  lc.KeyCustomer ,lc.CalendarDate ,MAX(lt.letterdate) AS LetterDate , lc.KeySourceSystem 
              INTO #lastLetterDate 
              FROM #list_customer  lc LEFT JOIN  #Letter lt ON lc.KeyCustomer=lt.KeyCustomer  AND lc.SourceSystem=lt.KeySourceSystem
              WHERE lt.letterdate<lc.CalendarDate  
			  GROUP BY lc.KeyCustomer,lc.CalendarDate,lc.KeySourceSystem
				


--------------------------------Step 3. Find all customer lastest Payment Date and Collections and Payment Date less than Call Date  -----

IF OBJECT_ID('TempDB..#pay') IS NOT NULL
DROP TABLE #pay;

SELECT CASE WHEN crm='MedProd Artiva' THEN 'MEDPROD'
              WHEN crm='FACS' THEN 'FACS'
			  WHEN crm='AMEX Latitude' THEN 'AMEX Latitude'
			  WHEN crm='ThirdProd Artiva' THEN 'ThirdProd'
			  ELSE 'None'END AS SourceSystem ,CUSTOMER_ID,CLIENT_ID, pymt_date=CAST(pymt_date AS date), total_collections,PYMT_TYPE
INTO #pay  FROM CLIENT_ANALYTICS.dbo.RPT_payment_detail (NOLOCK) WHERE pymt_date>@last24month


IF OBJECT_ID('TempDB..#payment') IS NOT NULL
DROP TABLE #payment;


SELECT * INTO #payment 
              FROM (
                     SELECT *, ROW_NUMBER() OVER(PARTITION BY t1.CustomerId , t1.CalendarDate, t1.ClientId,t1.SourceSystem ORDER BY t1.PaymentDate desc ) AS row_num 
                     FROM 
                      (
                          SELECT lc.CustomerId ,lc.CalendarDate,lc.ClientId,lc.SourceSystem,pyt.pymt_date AS PaymentDate,pyt.total_collections AS TotalCollection ,pymt_type AS Payment_Type  FROM #list_customer  lc LEFT JOIN #pay pyt 
                          ON lc.CustomerId=pyt.CUSTOMER_ID AND lc.ClientId=pyt.CLIENT_ID AND lc.SourceSystem=pyt.SourceSystem
                          WHERE pyt.pymt_date<lc.CalendarDate
			           )  AS t1 
			       ) AS t2 WHERE t2.row_num=1


IF OBJECT_ID('TempDB..#pay_sameday') IS NOT NULL
DROP TABLE #pay_sameday;



SELECT * INTO #pay_sameday
              FROM (
                     SELECT *, ROW_NUMBER() OVER(PARTITION BY t1.CustomerId , t1.CalendarDate, t1.ClientId,t1.SourceSystem ORDER BY t1.PaymentDate desc ) AS row_num 
                     FROM 
                      (
                          SELECT lc.CustomerId ,lc.CalendarDate,lc.ClientId,lc.SourceSystem,pyt.pymt_date AS PaymentDate,pyt.total_collections AS TotalCollection ,pymt_type AS Payment_Type  FROM #list_customer  lc LEFT JOIN #pay pyt 
                          ON lc.CustomerId=pyt.CUSTOMER_ID AND lc.ClientId=pyt.CLIENT_ID AND lc.SourceSystem=pyt.SourceSystem
                          WHERE pyt.pymt_date=lc.CalendarDate
			           )  AS t1 
			       ) AS t2 WHERE t2.row_num=1


------------------------------------------Step 4. Find all customer lastest Email Date and Date less than Call Date  -----------------------------
IF OBJECT_ID('TempDB..#email') IS NOT NULL
DROP TABLE #email;

SELECT lcf.KeyCustomer, CalendarDate, lcf.ClientId, MAX(EmailDate) as EmailDate,MAX(Email) AS Email,lcf.KeySourceSystem
      INTO #email
      FROM #list_customer lcf LEFT JOIN

       (
         SELECT KeyCustomer,ClientId, CaptureDate as EmailDate,Email ,
         CASE WHEN KeySourceSystem='1' THEN 'MEDPROD'
              WHEN KeySourceSystem='4' THEN 'FACS'
			  WHEN KeySourceSystem='3' THEN 'AMEX Latitude'
			  WHEN KeySourceSystem='2' THEN 'ThirdProd'
			  ELSE 'None' END AS KeySourceSystem FROM DW_MSTR_DM.dbo.DimCustomerEmail (NOLOCK)
       ) AS em ON lcf.KeyCustomer=em.KeyCustomer AND lcf.ClientId=em.ClientId AND lcf.SourceSystem=em.KeySourceSystem
	  WHERE em.EmailDate<lcf.CalendarDate AND em.EmailDate >@last24month
      GROUP BY lcf.KeyCustomer, lcf.CalendarDate,lcf.ClientId,lcf.KeySourceSystem




------------------------------------------Step 5. list all None Facs Customer info  -----------------------------

INSERT INTO CLIENT_ANALYTICS.dbo.RPT_calls_for_callminer
SELECT  
      fcc.KeyCustomer,
      fcc.CustomerId,
      fcc.CallStartTime,
      fcc.CalendarDate AS Call_Date,
      fcc.SessionId,
      rpc.LastRPCDate AS LastRPC_Date,
      CASE WHEN eml.Email IS NOT NULL THEN 1 ELSE 0 END AS Email_flag,
      lld.LetterDate AS LastLetter_Date,
      dcl.PaperType AS Product_Type,
      dcl.ClientParent AS Creditor_Name,
      fcc.StatusCode AS Status_Code,
      fcc.CurrentBalance AS Balance,
      fcc.BirthDate AS Age,
      rp.PhoneType AS Phone_Status_Code,
      CASE WHEN pyt.PaymentDate<fcc.CalendarDate THEN pyt.TotalCollection ELSE null END AS LastAmountPaid,
      fcc.IsPromise, 
      CASE WHEN py.TotalCollection >0 OR fcc.IsPromise=1 THEN 1 ELSE 0 END AS Paid_flag,
      pyt.Payment_Type AS Mode_Payment,
      GETDATE() AS Upload_Date

FROM #list_customer fcc 
					 LEFT JOIN DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) ON fcc.ClientId=dcl.ClientId AND fcc.SourceSystem=dcl.SourceSystem
					 LEFT JOIN #email eml ON fcc.KeyCustomer=eml.KeyCustomer AND fcc.ClientId=eml.ClientId AND fcc.KeySourceSystem=eml.KeySourceSystem AND fcc.CalendarDate=eml.CalendarDate
					 LEFT JOIN #lastLetterDate lld ON fcc.KeyCustomer=lld.KeyCustomer AND fcc.KeySourceSystem=lld.KeySourceSystem AND fcc.CalendarDate=lld.CalendarDate
					 LEFT JOIN DW_MSTR_DM.dbo.RadiusPhone rp (NOLOCK) ON fcc.KeyCustomer=rp.KeyCustomer AND fcc.ClientId=rp.ClientId AND fcc.DialedPhoneNumber=rp.PhoneNumber
					 LEFT JOIN #payment pyt ON fcc.CustomerId=pyt.CustomerId AND fcc.CalendarDate=pyt.CalendarDate AND fcc.ClientId=pyt.ClientId AND fcc.SourceSystem=pyt.SourceSystem 
					 LEFT JOIN #LastRPC rpc ON fcc.KeyCustomer=rpc.KeyCustomer AND fcc.CallStartTime=rpc.CallStartTime
					 LEFT JOIN #pay_sameday py ON fcc.CustomerId=py.CUSTOMERID AND fcc.ClientId=py.CLIENTID AND fcc.SourceSystem=py.SourceSystem AND fcc.CalendarDate=py.PaymentDate 




/******************************************************************************************************/
/******************************* Facs- call duration at least 30 seconds******************************/
/****************************************************************************************************/		


------------------------------------------Step 1. Find all customer who call duration at least 30 seconds in past 90 days  ------------

IF OBJECT_ID('TempDB..#list_customer_facs') IS NOT NULL
DROP TABLE #list_customer_facs;

SELECT 
chf.CUSTOMER_ID, 
CALL_DATE,
CALL_START_TIME,
CALL_DURATION_SECONDS,
chf.CLIENT_ID,
chf.CALL_IN_PHONE_NUMBER,
chf.IsPromise,
chf.IsAdjRPC,
 DATEDIFF(YEAR,luc.BIRTH_DATE,GETDATE()) AS BIRTH_DATE,
'FACS'AS SourceSystem ,
luc.STATUS_CODE
INTO #list_customer_facs FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT chf (NOLOCK)
                         LEFT JOIN DW_MSTR_DM.dbo.LU_CUSTOMER luc  (NOLOCK)
						 ON chf.CUSTOMER_ID=luc.CUSTOMER_ID
                         WHERE CALL_DATE>@call_date AND chf.CALL_DURATION_SECONDS>=30



CREATE INDEX #cs ON #list_customer_facs(CUSTOMER_ID);

---------------------------------------------find lastest RPC Date----------------------------------------------------------------------------------

IF OBJECT_ID('TempDB..#RPC_facs') IS NOT NULL
DROP TABLE #RPC_facs;

SELECT CUSTOMER_ID,CONCAT(CAST(CALL_DATE AS DATE) , ' ', CALL_START_TIME)  AS CallStartTime, IsAdjRPC, CASE WHEN IsAdjRPC=1 THEN CALL_DATE ELSE NULL END AS LastRPCDate INTO #RPC_facs
FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT (NOLOCK)  WHERE IsAdjRPC=1 and CALL_DURATION_SECONDS > =30  AND CALL_DATE>@last24month 


IF OBJECT_ID('TempDB..#LastRPC_facs') IS NOT NULL
DROP TABLE #LastRPC_facs;


SELECT lct.CUSTOMER_ID,CONCAT(CAST(CALL_DATE AS DATE) , ' ', CALL_START_TIME)  AS CallStartTime,MAX(rpc.LastRPCDate) AS LastRPCDate 
INTO #LastRPC_facs FROM #list_customer_facs  lct LEFT JOIN #RPC_facs rpc  ON lct.CUSTOMER_ID=rpc.CUSTOMER_ID WHERE  rpc.LastRPCDate< lct.CALL_DATE
GROUP BY lct.CUSTOMER_ID ,CONCAT(CAST(CALL_DATE AS DATE) , ' ', CALL_START_TIME)  


------------------------------------------Step 2. Find all customer lastest Letter Date and Letter Date less than Call Date  ----------


IF OBJECT_ID('TempDB..#Letter_facs') IS NOT NULL
DROP TABLE #Letter_facs;

SELECT CUSTOMER_ID,CALL_DATE,CLIENT_ID,MAX(letterdate) AS letterdate  INTO #Letter_facs FROM 
(
SELECT lcf.CUSTOMER_ID, lcf.CALL_DATE,fcl.MAIL_DATE AS letterdate ,fcl.CLIENT_ID FROM 
#list_customer_facs lcf
LEFT JOIN DW_MSTR_DM.dbo.LETTER_FACT fcl (NOLOCK)
ON lcf.CUSTOMER_ID=fcl.customer_id AND lcf.CLIENT_ID=fcl.CLIENT_ID  
WHERE fcl.MAIL_DATE<lcf.CALL_DATE AND fcl.MAIL_DATE >@last24month) AS t1  GROUP BY CUSTOMER_ID,CALL_DATE,CLIENT_ID



------------------------------------------Step 3. Find all customer lastest Payment Date and Collections and Payment Date less than Call Date  -----

IF OBJECT_ID('TempDB..#payment_facs') IS NOT NULL
DROP TABLE #payment_facs;

SELECT * INTO #payment_facs
              FROM (
                     SELECT *, ROW_NUMBER() OVER(PARTITION BY t1.Customer_Id , t1.CALL_DATE, t1.Client_Id,t1.SourceSystem ORDER BY t1.PaymentDate desc ) AS row_num 
                     FROM 
                      (
                          SELECT lc.Customer_Id ,lc.CALL_DATE,lc.Client_Id,lc.SourceSystem,pyt.pymt_date AS PaymentDate,pyt.total_collections AS TotalCollection ,pymt_type AS Payment_Type  FROM #list_customer_facs  lc LEFT JOIN #pay pyt 
                          ON lc.Customer_Id=pyt.CUSTOMER_ID AND lc.Client_Id=pyt.CLIENT_ID AND lc.SourceSystem=pyt.SourceSystem
                          WHERE pyt.pymt_date<lc.CALL_DATE
			           )  AS t1 
			       ) AS t2 WHERE t2.row_num=1


IF OBJECT_ID('TempDB..#pay_sameday_facs') IS NOT NULL
DROP TABLE #pay_sameday_facs;



SELECT * INTO #pay_sameday_facs
              FROM (
                     SELECT *, ROW_NUMBER() OVER(PARTITION BY t1.Customer_Id , t1.CALL_DATE, t1.Client_Id,t1.SourceSystem ORDER BY t1.PaymentDate desc ) AS row_num 
                     FROM 
                      (
                          SELECT lc.Customer_Id ,lc.CALL_DATE,lc.Client_Id,lc.SourceSystem,pyt.pymt_date AS PaymentDate,pyt.total_collections AS TotalCollection ,pymt_type AS Payment_Type  FROM #list_customer_facs  lc LEFT JOIN #pay pyt 
                          ON lc.Customer_Id=pyt.CUSTOMER_ID AND lc.Client_Id=pyt.CLIENT_ID AND lc.SourceSystem=pyt.SourceSystem
                          WHERE pyt.pymt_date=lc.CALL_DATE
			           )  AS t1 
			       ) AS t2 WHERE t2.row_num=1




------------------------------------------Step 4. Find all customer lastest Email Date and Date less than Call Date  -----------------------------

IF OBJECT_ID('TempDB..#email_facs') IS NOT NULL
DROP TABLE #email_facs;

SELECT  
lcf.Customer_Id, lcf.CALL_DATE, lcf.Client_Id, MAX(EmailDate) AS EmailDate,MAX(Email) AS Email,lcf.SourceSystem INTO #email_facs
FROM #list_customer_facs lcf LEFT JOIN
(
SELECT CustomerId,ClientId, CaptureDate AS EmailDate,Email ,
         CASE WHEN KeySourceSystem='1' THEN 'MEDPROD'
              WHEN KeySourceSystem='4' THEN 'FACS'
			  WHEN KeySourceSystem='3' THEN 'AMEX Latitude'
			  WHEN KeySourceSystem='2' THEN 'ThirdProd'
			  ELSE 'None'END AS KeySourceSystem FROM DW_MSTR_DM.dbo.DimCustomerEmail (NOLOCK)
			  ) AS em 
			  ON lcf.CUSTOMER_ID=em.CustomerId AND lcf.Client_Id=em.ClientId AND lcf.SourceSystem=em.KeySourceSystem
			  WHERE em.EmailDate<lcf.CALL_DATE AND em.EmailDate>@last24month
GROUP BY lcf.Customer_Id, lcf.CALL_DATE,lcf.Client_Id,lcf.SourceSystem


------------------------------------------Step 5. list all Facs Customer info  -----------------------------

INSERT INTO CLIENT_ANALYTICS.dbo.RPT_calls_for_callminer
SELECT  
       NULL AS KeyCustomer,
       fcc.CUSTOMER_ID AS customerid,
       CONCAT(CAST(fcc.CALL_DATE AS DATE) , ' ', fcc.CALL_START_TIME)  AS CallStartTime,
       fcc.CALL_DATE AS Call_Date,
       'NA' AS SessionId,
       rpc.LastRPCDate AS LastRPC_Date,
       CASE WHEN eml.Email IS NOT NULL THEN 1 ELSE 0 END AS Email_flag,
       lld.LetterDate AS LastLetter_Date,
       tbl.PaperType AS Product_Type,
       tbl.Parent AS Creditor_Name,
       fcc.Status_Code AS Status_Code ,
       (obf.INITIAL_BALANCE-obf.AMT_PAID_ON_ACCOUNT) AS Balance,
       fcc.BIRTH_DATE  AS Age,
       rp.PHONE_FLAG_VALUE AS Phone_Status_Code,
       CASE WHEN pyt.PaymentDate<fcc.CALL_DATE THEN pyt.TotalCollection ELSE NULL END AS LastAmountPaid,
       fcc.IsPromise,
       CASE WHEN py.TotalCollection >0 OR fcc.IsPromise=1 THEN 1 ELSE 0 END AS Paid_flag,
       pyt.Payment_Type AS Mode_Payment,
       GETDATE() AS Upload_Date

FROM #list_customer_facs fcc 
					 LEFT JOIN DW_MSTR_DM.dbo.TblClientStreams tbl (NOLOCK) ON fcc.Client_ID = tbl.CLIENT_ID
					 LEFT JOIN DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT obf (NOLOCK) ON fcc.CUSTOMER_ID=obf.CUSTOMER_ID 
					 LEFT JOIN #email_facs eml ON fcc.CUSTOMER_ID=eml.CUSTOMER_ID AND fcc.Client_Id=eml.Client_Id AND fcc.SourceSystem=eml.SourceSystem AND fcc.CALL_DATE=eml.CALL_DATE
					 LEFT JOIN #Letter_facs lld ON fcc.CUSTOMER_ID=lld.CUSTOMER_ID AND fcc.CALL_DATE=lld.CALL_DATE
					 LEFT JOIN (SELECT CUSTOMER_ID, CONCAT( SUBSTRING( PHONE_FIELD_VALUE,1,3),SUBSTRING( PHONE_FIELD_VALUE,5,3),SUBSTRING( PHONE_FIELD_VALUE,9,4)) AS PHONE_FIELD_VALUE ,PHONE_FLAG_VALUE FROM DW_MSTR_DM.dbo.CUST_PHONE_HIST (NOLOCK)) AS rp 
					 ON fcc.CUSTOMER_ID=rp.CUSTOMER_ID AND fcc.CALL_IN_PHONE_NUMBER=rp.PHONE_FLAG_VALUE 
					 LEFT JOIN #payment_facs pyt ON fcc.Customer_Id=pyt.Customer_Id AND fcc.Client_Id=pyt.Client_Id AND fcc.SourceSystem=pyt.SourceSystem AND fcc.CALL_DATE=pyt.CALL_DATE
					 LEFT JOIN #LastRPC_facs rpc ON fcc.CUSTOMER_ID=rpc.CUSTOMER_ID AND CONCAT(CAST(fcc.CALL_DATE AS DATE) , ' ', fcc.CALL_START_TIME)=rpc.CallStartTime
					 LEFT JOIN #pay_sameday_facs py ON fcc.Customer_Id=py.CUSTOMER_ID AND fcc.Client_Id=py.CLIENT_ID AND fcc.SourceSystem=py.SourceSystem AND fcc.CALL_DATE=py.PaymentDate
GO



GO



GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO



GO



GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_RPT_calls_for_callminer] TO [CORP\aramugade]
    AS [dbo];

