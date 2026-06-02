CREATE VIEW [dbo].[VW__Comment_API]
AS
	SELECT c.* 
	FROM [dbo].[CM_Comment_API] c WITH (NOLOCK)
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[VW__Comment_API] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW__Comment_API] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW__Comment_API] TO [CORP\pjain]
    AS [dbo];

