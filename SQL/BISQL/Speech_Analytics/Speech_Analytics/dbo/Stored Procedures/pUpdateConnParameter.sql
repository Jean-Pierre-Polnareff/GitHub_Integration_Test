-- =============================================
-- Author:		Vladislav Pilipets
-- Create date: 2022-09-15
-- Description:	change in connectoin attributes value 
--			- permissions
-- =============================================
CREATE PROCEDURE pUpdateConnParameter
	@parameter		varchar(50), 
	@value			varchar(2048)
AS
BEGIN
	SET NOCOUNT ON;

    IF EXISTS (SELECT 1 
				FROM Speech_Analytics.dbo.cm_connattr WITH (NOLOCK) 
				WHERE parameter = @parameter)
	BEGIN 
		-- EXISTING 
		UPDATE C 
		SET attributes = @value 
		FROM Speech_Analytics.dbo.cm_connattr c WITH (NOLOCK) 
				WHERE parameter = @parameter
	END 
	ELSE 
	BEGIN 
		-- NEW 
		INSERT INTO Speech_Analytics.dbo.cm_connattr 
		(parameter, attributes) 
		VALUES
		(@parameter, @value)
	END 
END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateConnParameter] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateConnParameter] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateConnParameter] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateConnParameter] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateConnParameter] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pUpdateConnParameter] TO [CORP\aramugade]
    AS [dbo];

