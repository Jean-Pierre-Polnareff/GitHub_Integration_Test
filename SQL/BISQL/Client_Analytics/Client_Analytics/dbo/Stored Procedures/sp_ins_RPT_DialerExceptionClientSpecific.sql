

CREATE PROCEDURE [dbo].[sp_ins_RPT_DialerExceptionClientSpecific]
AS
/*
Description: Populates daily Dialer Exception Report daily. Includes a blank row to the report displays if no exceptions.
inserts into CLIENT_ANALYTICS.dbo.RPT_DialerExceptionClientSpecific

Change History:
Author				Date			Description
Sankeerth Mamidi 	06/16/2025		Initial creation

*/
BEGIN

DECLARE @Startdate DATE = CAST(DATEADD(DAY, -8, GETDATE()) AS DATE);


DELETE FROM [CLIENT_ANALYTICS].dbo.RPT_DialerExceptionClientSpecific
WHERE CAST(CallDateTime AS DATE) >= @Startdate;

--TRUNCATE TABLE [CLIENT_ANALYTICS].dbo.RPT_DialerExceptionClientSpecific

INSERT INTO [CLIENT_ANALYTICS].dbo.RPT_DialerExceptionClientSpecific
(
     Call_History_Fact_Id 
	,CustomerId 
	,Client
	,ClientId 
	,ClientStream 
	,Parent
	,Vertical
	,CustomerState 
	,CallDateTime 
	,CallWeek
	,CallHour
	,CallDurationSeconds 
	,CallType 
	,ContactCode 
	,TerminationCode 
	,EmployeeId 
	,IsMobilePhone 
	,PhoneTypeCode 
	,PhoneType 
	,CallOutPhoneNumber 
	,AreaCode 
	,SourceSystem 
	,KeyException
	,ExceptionName
	,CallSequence
	,CallSequenceCustDay 
	,CallSequenceCustWeek
	,CallSequenceCustPhNumberDay 
	,CallSequenceCustPhNumberWeek
	,CustomerDay
	,CustomerPhNumberDay
	,CustomerPhNumberWeek
	,NoAutoToMEAfter5
	,NoCallsCAPOEFirst20Days
	,WVMax2CallsPerWeek
	,NoCallsToNYMAWA
	,ThreeHourGap
	,ThirtyMinuteGap
	,HasException
	)

	SELECT 
	 cs.Call_History_Fact_Id 
	,cs.CustomerId 
	,cs.Client
	,cs.ClientId 
	,cs.ClientStream 
	,s.Parent
	,cs.Vertical
	,cs.CustomerState 
	,cs.CallDateTime 
	,cs.CallWeek
	,cs.CallHour
	,cs.CallDurationSeconds 
	,cs.CallType 
	,cs.ContactCode 
	,cs.TerminationCode 
	,cs.EmployeeId 
	,cs.IsMobilePhone 
	,cs.PhoneTypeCode 
	,cs.PhoneType 
	,cs.CallOutPhoneNumber 
	,cs.AreaCode 
	,cs.SourceSystem 
	,cs.KeyException
	,cs.ExceptionName
	,cs.CallSequence
	,cs.CallSequenceCustDay 
	,cs.CallSequenceCustWeek
	,cs.CallSequenceCustPhNumberDay 
	,cs.CallSequenceCustPhNumberWeek
	,cs.CustomerDay
	,cs.CustomerPhNumberDay
	,cs.CustomerPhNumberWeek
	,cs.NoAutoToMEAfter5
	,cs.NoCallsCAPOEFirst20Days
	,cs.WVMax2CallsPerWeek
	,cs.NoCallsToNYMAWA
	,cs.ThreeHourGap
	,cs.ThirtyMinuteGap
	,HasException = CASE WHEN CustomerDay > 0 
		OR CustomerPhNumberDay > 0
		OR CustomerPhNumberWeek > 0
		OR NoAutoToMEAfter5 > 0
		OR NoCallsCAPOEFirst20Days > 0
		OR WVMax2CallsPerWeek > 0
		OR NoCallsToNYMAWA > 0
		OR ThreeHourGap > 0
		OR ThirtyMinuteGap > 0 THEN 1 ELSE 0 END	
		
	FROM DW_MSTR_DM.dbo.DialerExceptionClientSpecific cs (NOLOCK)

	JOIN DW_MSTR_DM.dbo.TblClientStreams s (NOLOCK)
		ON cs.ClientId = s.Client_ID
	WHERE  CAST(cs.CallDateTime AS DATE) >= @Startdate
	AND
	CustomerId IN 
	(SELECT CustomerId 
		FROM DW_MSTR_DM.dbo.DialerExceptionClientSpecific (NOLOCK)
		WHERE CustomerDay > 0 
		OR CustomerPhNumberDay > 0
		OR CustomerPhNumberWeek > 0
		OR NoAutoToMEAfter5 > 0
		OR NoCallsCAPOEFirst20Days > 0
		OR WVMax2CallsPerWeek > 0
		OR NoCallsToNYMAWA > 0
		OR ThreeHourGap > 0
		OR ThirtyMinuteGap > 0
		GROUP BY CustomerId)
	AND cs.Client <> 'TEST CLIENT'
	AND CAST(cs.CallDateTime AS DATE) >= @Startdate
		UNION
	SELECT 
	'' AS Call_History_Fact_Id 
	,'' AS CustomerId 
	,cs.Client AS Client
	,'' AS ClientId 
	,'' AS ClientStream 
	,'' AS Parent
	,cs.Vertical AS Vertical
	,'' AS CustomerState 
	,'' AS CallDateTime 
	,'' AS CallWeek
	,'' AS CallHour
	,'' AS CallDurationSeconds 
	,'' AS CallType 
	,'' AS ContactCode 
	,'' AS TerminationCode 
	,'' AS EmployeeId 
	,'' AS IsMobilePhone 
	,'' AS PhoneTypeCode 
	,'' AS PhoneType 
	,'' AS CallOutPhoneNumber 
	,'' AS AreaCode 
	,'' AS SourceSystem 
	,'' AS KeyException
	,'' AS ExceptionName
	,'' AS CallSequence
	,'' AS CallSequenceCustDay 
	,'' AS CallSequenceCustWeek
	,'' AS CallSequenceCustPhNumberDay 
	,'' AS CallSequenceCustPhNumberWeek
	,'' AS CustomerDay
	,'' AS CustomerPhNumberDay
	,'' AS CustomerPhNumberWeek
	,'' AS NoAutoToMEAfter5
	,'' AS NoCallsCAPOEFirst20Days
	,'' AS WVMax2CallsPerWeek
	,'' AS NoCallsToNYMAWA
	,'' AS ThreeHourGap
	,'' AS ThirtyMinuteGap
	,HasException = 0
	FROM DW_MSTR_DM.dbo.DialerExceptionClientSpecific cs (NOLOCK)
	WHERE cs.Client <> 'TEST CLIENT' 
		AND CAST(cs.CallDateTime AS DATE) >=  @Startdate 

	GROUP BY cs.Client, cs.Vertical;
 END;