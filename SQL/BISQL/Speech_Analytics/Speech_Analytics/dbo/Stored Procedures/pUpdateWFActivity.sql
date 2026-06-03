-- =============================================
-- Author:		Vladislav Pilipets
-- Create date: 8/17/22
-- Description:	Update workflow activity  
-- =============================================
--Guid session_id, long activity_id, string error_msg, int isProcessed, string end_time
CREATE PROCEDURE [dbo].[pUpdateWFActivity] 
	@session_id		VARCHAR(50), 
	@activity_id	BIGINT, 
	@error_msg		VARCHAR(2000) = null, 
	@isProcessed	BIT = 0, 
	@end_time		DATETIME = null
AS
BEGIN
	SET NOCOUNT ON 

	UPDATE a 
    SET error_msg = @error_msg, IsProcessed = @isProcessed, end_time = @end_time 
    FROM Workflow_Activity a 
    WHERE activity_id = @activity_id AND session_id = @session_id;

	IF @isProcessed = 1 
	BEGIN 
		-- if this is the last activity then close workflow 
		UPDATE s 
		SET IsProcessed = 1 
		FROM Workflow_Activities aa 
			JOIN Workflow_Activity a ON a.activity_name = aa.activity_name and aa.IsActive = 1 
			LEFT JOIN Workflow_Activities aa1 ON aa1.sort = (aa.sort + 1) and aa1.IsActive = 1
			JOIN Workflow_State  s on s.session_id = a.session_id 
		WHERE a.activity_id = @activity_id  
			AND s.IsProcessed = 0 
			AND aa1.activity_name IS NULL 

		IF @@ROWCOUNT > 0 
			SELECT @session_id session_id, 1 IsProcessed
		ELSE 
			SELECT @session_id session_id, 0 IsProcessed
	END 
	ELSE 
	BEGIN 
		SELECT @session_id session_id, 0 IsProcessed 
	END 
END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateWFActivity] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateWFActivity] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateWFActivity] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateWFActivity] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateWFActivity] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateWFActivity] TO [CORP\aramugade]
    AS [dbo];

