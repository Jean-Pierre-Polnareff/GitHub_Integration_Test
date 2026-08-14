
-- Create the stored procedure
CREATE PROCEDURE [dbo].[Sp_UpdateStatsAndLoadDetails_CLIENT_ANALYTICS]
AS
BEGIN
    SET NOCOUNT ON;
 
    -- Step 1: Insert tables with the last updated statistics into the destination table
INSERT INTO DW_RETENTION.dbo.DB_Table_Stats (DBname, TableName, Statupdatedate, TableSize)
SELECT db_name() AS DBname,
t.name AS TableName,
       MAX(sp.last_updated) AS Statupdatedate,
       SUM(a.total_pages) * 8.0 / 1024 AS TableSize
FROM sys.tables t
LEFT JOIN sys.stats s ON t.object_id = s.object_id
OUTER APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
INNER JOIN sys.partitions p ON t.object_id = p.object_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
GROUP BY t.name;
 
-- Step 2: Update statistics with FULLSCAN for all tables in the specified database
DECLARE @SQL NVARCHAR(MAX) = '';
 
SELECT @SQL = @SQL + ' USE ' + QUOTENAME(d.name) + '; ' +
               'DECLARE @Table NVARCHAR(255); ' +
               'DECLARE @Stat NVARCHAR(255); ' +
               'DECLARE StatsCursor CURSOR FOR ' +
'SELECT QUOTENAME(t.name), QUOTENAME(s.name) ' +
'FROM ' + QUOTENAME(d.name) + '.sys.tables t ' +
'JOIN ' + QUOTENAME(d.name) + '.sys.stats s ON t.object_id = s.object_id; ' +
               'OPEN StatsCursor; ' +
               'FETCH NEXT FROM StatsCursor INTO @Table, @Stat; ' +
               'WHILE @@FETCH_STATUS = 0 ' +
               'BEGIN ' +
               '    EXEC(''UPDATE STATISTICS '' + @Table + '' '' + @Stat + '' WITH FULLSCAN;''); ' +
               '    FETCH NEXT FROM StatsCursor INTO @Table, @Stat; ' +
               'END ' +
               'CLOSE StatsCursor; ' +
               'DEALLOCATE StatsCursor; '
FROM sys.databases d
WHERE d.name = 'CLIENT_ANALYTICS';  -- Replace with your actual database name
 
EXEC sp_executesql @SQL;



END