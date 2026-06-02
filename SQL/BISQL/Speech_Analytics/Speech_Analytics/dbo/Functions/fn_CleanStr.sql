-- ============================================================================
-- Author:		Vladislav Pilipets
-- Create date: 2023-10-19
-- Description: clean strings of 
--				1. comma; 2. carriage return; 3. line feed 
-- ============================================================================
CREATE FUNCTION fn_CleanStr
(
	@value nvarchar(1000) 
)
RETURNS nvarchar(1000) 
AS
BEGIN
	-- Declare the return variable here
	DECLARE @Result nvarchar(1000)

	-- Add the T-SQL statements to compute the return value here
	SELECT @Result = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
              LTRIM(RTRIM(@value)), CHAR(9), ' '), CHAR(10), ' '), CHAR(11), ' '), CHAR(12), ' '), CHAR(13), ' '), NCHAR(10), ' '), NCHAR(13), ' ')))

	-- Return the result of the function
	RETURN @Result

END
GO
GRANT EXECUTE
    ON OBJECT::[dbo].[fn_CleanStr] TO [CORP\pjain]
    AS [dbo];

