-- ===================================================
-- Author:		Vladislav Pilipets
-- Create date: 12/19/22
-- Description:	Get attribute for current namespace 
-- ===================================================
CREATE PROCEDURE [dbo].[pGetNamespaceAttribute]
	@namespace varchar(50), 
	@parameter varchar(50) 
AS
BEGIN
	SET NOCOUNT ON;

	SELECT attributes  
	FROM [dbo].[cm_connattr] 
	WHERE namespace = @namespace  
		AND parameter = @parameter  

END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetNamespaceAttribute] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetNamespaceAttribute] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetNamespaceAttribute] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetNamespaceAttribute] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetNamespaceAttribute] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetNamespaceAttribute] TO [CORP\aramugade]
    AS [dbo];

