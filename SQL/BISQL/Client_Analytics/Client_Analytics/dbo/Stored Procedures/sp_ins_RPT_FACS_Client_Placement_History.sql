CREATE PROCEDURE  [dbo].[sp_ins_RPT_FACS_Client_Placement_History]
AS
------------------------------------CREATE VIEW DW_MSTR_DM.[dbo].[vw_Todds_RPT_PlacementHistory]
------------------------------------AS
/*
Object:			DW_MSTR_DM.dbo.vw_Todds_RPT_PlacementHistory
Description:	Provides data for report ClientPlacementHistoryBuster.xlsx

Author			Date		Description
Todd Sobiech	11/29/2011	Created
Lara Zuleger	04/07/2016	Updated formatting, leverage new table dbo.Tbl_AMP_ScoreGrouping
Heidi Oldham	03/15/2018	use CTE to pull only rolling 18 mths
Amod Ramugade   05/09/2022  Added account level details and converted the view into a stored proc which inserts the data in a reporting table
*/

BEGIN

DROP TABLE  if exists #t_cust;
WITH Date_CTE (StartDate)
AS
(
SELECT StartDate = CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-18, 0) AS DATE)
)
SELECT cust.customer_id
,cust.customer_state 
,Dt.CALNDR_DT
,CL.CLIENT_DESC
       ,CL.CLIENT_CLASS
       ,Cust.CLIENT_ID
       ,ST.Client_Stream
       ,ST.PaperType
       ,DT.Month_ID
       ,Cust.LIST_DATE
       ,Cust.EVER_PREVIOUS_ACCOUNT
       ,Cust.EVER_PREVIOUS_PAYER_ACCOUNT
       ,COInd = CASE WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 0
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < .5
             THEN 'A - <6mos'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= .5
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 1
             THEN 'B -6mos-12mos'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 1
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 1.5
             THEN 'C -12mos-18mos'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 1.5
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 2
             THEN 'D -18mos-24mos'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 2
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 3
             THEN 'E -2yr-3yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 3
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 4
             THEN 'F -3yr-4yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 4
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 5
             THEN 'G -4yr-5yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 5
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 6
             THEN 'H -5yr-6yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 6
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 7
             THEN 'I -6yr-7yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 7
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 8
             THEN 'J -7yr-8yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 8
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 9
             THEN 'K -8yr-9yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 9
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 10
             THEN 'L -9yr-10yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 10
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 11
             THEN 'M -10yr-11yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 11
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 12
             THEN 'N -11yr-12yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 12
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 13
             THEN 'O -12yr-13yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 13
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 14
             THEN 'P -13yr-14yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 14
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 15
             THEN 'Q -14yr-15yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 15
             THEN 'R -15+ years'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 0
             THEN 'S -Missing Info'
        END
       ,AMP_Flagship_Twentile = CASE WHEN RTS.AMP_Score >= 76
                                            AND RTS.AMP_Score < 101 THEN 'A-76-100'
                                       WHEN RTS.AMP_Score >= 68
                                            AND RTS.AMP_Score < 76 THEN 'B-68-75'
                                       WHEN RTS.AMP_Score >= 62
                                            AND RTS.AMP_Score < 68 THEN 'C-62-67'
                                       WHEN RTS.AMP_Score >= 56
                                            AND RTS.AMP_Score < 62 THEN 'D-56-61'
                                       WHEN RTS.AMP_Score >= 52
                                            AND RTS.AMP_Score < 56 THEN 'F-52-55'
                                       WHEN RTS.AMP_Score >= 48
                                            AND RTS.AMP_Score < 52 THEN 'G-48-51'
                                       WHEN RTS.AMP_Score >= 44
                                            AND RTS.AMP_Score < 48 THEN 'H-44-47'
                                       WHEN RTS.AMP_Score >= 40
                                            AND RTS.AMP_Score < 44 THEN 'I-40-43'
                                       WHEN RTS.AMP_Score >= 36
                                            AND RTS.AMP_Score < 40 THEN 'J-36-39'
                                       WHEN RTS.AMP_Score >= 33
                                            AND RTS.AMP_Score < 36 THEN 'K-33-35'
                                       WHEN RTS.AMP_Score >= 30
                                            AND RTS.AMP_Score < 33 THEN 'L-30-32'
                                       WHEN RTS.AMP_Score >= 27
                                            AND RTS.AMP_Score < 30 THEN 'M-27-29'
                                       WHEN RTS.AMP_Score >= 24
                                            AND RTS.AMP_Score < 27 THEN 'N-24-26'
                                       WHEN RTS.AMP_Score >= 21
                                            AND RTS.AMP_Score < 24 THEN 'O-21-23'
                                       WHEN RTS.AMP_Score >= 18
                                            AND RTS.AMP_Score < 21 THEN 'P-18-20'
                                       WHEN RTS.AMP_Score >= 15
                                            AND RTS.AMP_Score < 18 THEN 'Q-15-17'
                                       WHEN RTS.AMP_Score >= 13
                                            AND RTS.AMP_Score < 15 THEN 'R-13-14'
                                       WHEN RTS.AMP_Score >= 10
                                            AND RTS.AMP_Score < 13 THEN 'S-10-12'
                                       WHEN RTS.AMP_Score >= 7
                                            AND RTS.AMP_Score < 10 THEN 'T-7-9'
                                       WHEN RTS.AMP_Score >= 0
                                            AND RTS.AMP_Score < 7 THEN 'U-1-6'
                                       ELSE 'V-NOScore'
                                  END
       ,AMP_Combined_Twentile = ISNULL(tasg.AmpScoreGrouping,'U-NOScore')
	   ,RTS.SSN_Flag
       ,Cust.STATUS_CODE
       ,Active_Ind = CASE WHEN Cust.STATUS_CODE IN ('9000','9999') THEN 0 ELSE 1 END
       ,Bal_Ranges = CASE WHEN BAL.INITIAL_BALANCE > 0
                  AND BAL.INITIAL_BALANCE < 500 THEN 'A-0-$499'
             WHEN BAL.INITIAL_BALANCE >= 500
                  AND BAL.INITIAL_BALANCE < 1000 THEN 'B-$500-$999'
             WHEN BAL.INITIAL_BALANCE >= 1000
                  AND BAL.INITIAL_BALANCE < 2000 THEN 'C-$1000-$1999'
             WHEN BAL.INITIAL_BALANCE >= 2000
                  AND BAL.INITIAL_BALANCE < 3000 THEN 'D-$2000-$2999'
             WHEN BAL.INITIAL_BALANCE >= 3000 THEN 'E-$3000+'
					END 
      ,Worked_Acct_Ind = CASE WHEN LS.counter > 0 THEN 1 ELSE 0 END
       ,Counter = SUM(1)
       ,Placement_Ct = COUNT(Cust.CUSTOMER_ID) 
       ,Placement_Amt = SUM(BAL.INITIAL_BALANCE)
	   ,cust.CLIENT_ACCOUNT_NUMBER
	   ,cust.CHARGE_OFF_DATE	   
	   ,bal.LAST_PYMT_DATE  
	   ,bal.AMT_PAID_ON_ACCOUNT
	   
       ,Accts_with_Letter = SUM(LS.counter) 
	   INTO #t_cust
FROM    DW_MSTR_DM.dbo.LU_CUSTOMER Cust (NOLOCK) 
INNER JOIN Date_CTE cte
		ON cust.LIST_DATE >= cte.StartDate
INNER JOIN DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT BAL (NOLOCK)
        ON Cust.CUSTOMER_ID = BAL.CUSTOMER_ID
INNER JOIN DW_MSTR_DM.dbo.LU_DATE DT (NOLOCK)
        ON Cust.LIST_DATE = DT.CALNDR_DT
INNER JOIN DW_MSTR_DM.dbo.LU_CLIENT CL (NOLOCK)
        ON Cust.CLIENT_ID = CL.CLIENT_ID
LEFT OUTER JOIN DW_MSTR_DM.dbo.TblClientStreams ST (NOLOCK)
        ON Cust.CLIENT_ID = ST.Client_ID
LEFT OUTER JOIN DW_MSTR_DM.dbo.Tbl_AMP_Score_Suite RTS (NOLOCK)
        ON Cust.CUSTOMER_ID = RTS.Customer_ID
LEFT OUTER JOIN DW_MSTR_DM.dbo.Tbl_AMP_ScoreGrouping tasg (NOLOCK)
		ON RTS.Amp_Combined_Score = tasg.Amp_Combined_Score
		
LEFT OUTER JOIN (SELECT CUSTOMER_ID
                       ,1 AS counter
                 FROM   DW_MSTR_DM.dbo.LETTER_FACT fct (NOLOCK)
                 INNER JOIN Date_CTE cte (NOLOCK)
                 ON fct.MAIL_DATE >= cte.StartDate
                 --WHERE  MAIL_DATE >= '2015-01-01'
                 GROUP BY CUSTOMER_ID) LS
        ON Cust.CUSTOMER_ID = LS.Customer_ID
--WHERE   Cust.LIST_DATE >= '2015-01-01'
GROUP BY cust.customer_id
,cust.customer_state
,DT.CALNDR_DT
,CL.CLIENT_DESC
       ,CL.CLIENT_CLASS
       ,Cust.CLIENT_ID
       ,ST.Client_Stream
       ,ST.PaperType
       ,DT.Month_ID
       ,Cust.LIST_DATE
       ,Cust.EVER_PREVIOUS_ACCOUNT
       ,Cust.EVER_PREVIOUS_PAYER_ACCOUNT
       ,CASE WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 0
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < .5
             THEN 'A - <6mos'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= .5
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 1
             THEN 'B -6mos-12mos'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 1
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 1.5
             THEN 'C -12mos-18mos'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 1.5
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 2
             THEN 'D -18mos-24mos'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 2
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 3
             THEN 'E -2yr-3yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 3
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 4
             THEN 'F -3yr-4yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 4
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 5
             THEN 'G -4yr-5yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 5
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 6
             THEN 'H -5yr-6yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 6
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 7
             THEN 'I -6yr-7yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 7
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 8
             THEN 'J -7yr-8yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 8
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 9
             THEN 'K -8yr-9yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 9
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 10
             THEN 'L -9yr-10yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 10
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 11
             THEN 'M -10yr-11yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 11
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 12
             THEN 'N -11yr-12yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 12
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 13
             THEN 'O -12yr-13yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 13
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 14
             THEN 'P -13yr-14yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 14
                  AND DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 15
             THEN 'Q -14yr-15yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 >= 15
             THEN 'R -15+ years'
             WHEN DATEDIFF(DAY,ISNULL(Cust.CHARGE_OFF_DATE,'1/1/2040'),ISNULL(Cust.LIST_DATE,'1/1/2050')) / 360.00 < 0
             THEN 'S -Missing Info'
        END
       ,CASE WHEN RTS.AMP_Score >= 76
                  AND RTS.AMP_Score < 101 THEN 'A-76-100'
             WHEN RTS.AMP_Score >= 68
                  AND RTS.AMP_Score < 76 THEN 'B-68-75'
             WHEN RTS.AMP_Score >= 62
                  AND RTS.AMP_Score < 68 THEN 'C-62-67'
             WHEN RTS.AMP_Score >= 56
                  AND RTS.AMP_Score < 62 THEN 'D-56-61'
             WHEN RTS.AMP_Score >= 52
                  AND RTS.AMP_Score < 56 THEN 'F-52-55'
             WHEN RTS.AMP_Score >= 48
                  AND RTS.AMP_Score < 52 THEN 'G-48-51'
             WHEN RTS.AMP_Score >= 44
                  AND RTS.AMP_Score < 48 THEN 'H-44-47'
             WHEN RTS.AMP_Score >= 40
                  AND RTS.AMP_Score < 44 THEN 'I-40-43'
             WHEN RTS.AMP_Score >= 36
                  AND RTS.AMP_Score < 40 THEN 'J-36-39'
             WHEN RTS.AMP_Score >= 33
                  AND RTS.AMP_Score < 36 THEN 'K-33-35'
             WHEN RTS.AMP_Score >= 30
                  AND RTS.AMP_Score < 33 THEN 'L-30-32'
             WHEN RTS.AMP_Score >= 27
                  AND RTS.AMP_Score < 30 THEN 'M-27-29'
             WHEN RTS.AMP_Score >= 24
                  AND RTS.AMP_Score < 27 THEN 'N-24-26'
             WHEN RTS.AMP_Score >= 21
                  AND RTS.AMP_Score < 24 THEN 'O-21-23'
             WHEN RTS.AMP_Score >= 18
                  AND RTS.AMP_Score < 21 THEN 'P-18-20'
             WHEN RTS.AMP_Score >= 15
                  AND RTS.AMP_Score < 18 THEN 'Q-15-17'
             WHEN RTS.AMP_Score >= 13
                  AND RTS.AMP_Score < 15 THEN 'R-13-14'
             WHEN RTS.AMP_Score >= 10
                  AND RTS.AMP_Score < 13 THEN 'S-10-12'
             WHEN RTS.AMP_Score >= 7
                  AND RTS.AMP_Score < 10 THEN 'T-7-9'
             WHEN RTS.AMP_Score >= 0
                  AND RTS.AMP_Score < 7 THEN 'U-1-6'
             ELSE 'V-NOScore'
        END
       ,ISNULL(tasg.AmpScoreGrouping,'U-NOScore')
       ,RTS.SSN_Flag
       ,Cust.STATUS_CODE
       ,CASE WHEN Cust.STATUS_CODE IN ('9000','9999') THEN 0 ELSE 1 END
       ,CASE WHEN BAL.INITIAL_BALANCE > 0
                  AND BAL.INITIAL_BALANCE < 500 THEN 'A-0-$499'
             WHEN BAL.INITIAL_BALANCE >= 500
                  AND BAL.INITIAL_BALANCE < 1000 THEN 'B-$500-$999'
             WHEN BAL.INITIAL_BALANCE >= 1000
                  AND BAL.INITIAL_BALANCE < 2000 THEN 'C-$1000-$1999'
             WHEN BAL.INITIAL_BALANCE >= 2000
                  AND BAL.INITIAL_BALANCE < 3000 THEN 'D-$2000-$2999'
             WHEN BAL.INITIAL_BALANCE >= 3000 THEN 'E-$3000+'
        END
      ,CASE WHEN LS.counter > 0 THEN 1 ELSE 0 END
	   ,cust.CLIENT_ACCOUNT_NUMBER
	   ,cust.CHARGE_OFF_DATE
	   ,bal.LAST_PYMT_DATE
	   ,bal.AMT_PAID_ON_ACCOUNT




TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_FACS_Client_Placement_History
INSERT INTO CLIENT_ANALYTICS.dbo.RPT_FACS_Client_Placement_History SELECT * FROM #t_cust
---SELECT TOP 0 * INTO CLIENT_ANALYTICS.dbo.RPT_FACS_Client_Placement_History FROM #t_cust

END;

GO