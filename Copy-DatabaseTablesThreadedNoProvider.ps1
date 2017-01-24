
#This example will copy an entire schema of tables from a source location to a (series) of destinatons.
#Note that unlike the source script this is based on, there is currently no parameter support.
#Values are hardcoded on the following lines:
# 178 - 181 for souce server/database/schema
# 244 - 248 for target(s) servers/databases/schemas
#You will need the PowerShell provider (sqlps/sqlserver loaded for this to work!)

$runspacePool = [RunspaceFactory]::CreateRunspacePool(1, 4)
$runspacePool.ApartmentState = "MTA"
$runspacePool.Open()

$threads = @()

$codeContainer = {
    param(
        [string] $dServer,
        [string] $dInstance,
        [string] $dDatabase,
        [string] $dSchema,
        [string] $dWorkingDirectory,
        [bool] $dNoCheckConstraints,
        [PSCustomObject[]] $mdm
    )


    import-module sqlserver
    $VerbosePreference = "Continue"

    $destinationSQLCmdServerInstance = $dServer
    if ($dInstance -ne "DEFAULT") { $destinationSQLCmdServerInstance += "\" + $dInstance }
    
    Write-Verbose "Connecting to server $destinationSQLCmdServerInstance..."
    $serverObject = New-Object Microsoft.SQLServer.Management.Smo.Server
    $serverObject.ConnectionContext.MultipleActiveResultSets = $true
    $serverObject.ConnectionContext.ServerInstance = $destinationSQLCmdServerInstance

    $dPath = "SQLSERVER:\SQL\" + $dServer + "\" + $dInstance + "\Databases\" + $dDatabase + "\tables"

    $db = $serverObject.Databases | Where-Object {$_.Name -eq $dDatabase}
    $db.Tables.Refresh()
    $tables = $db.Tables | Where-Object {$_.Schema -eq $dSchema}
    $sbviews = $db.Views | Where-Object {$_.IsSchemaBound -eq $true}

    
    foreach ($t in $tables)
    {
        $t.ForeignKeys.Refresh()
    }

    $permissions = @()
    $foreignKeys = @()
    $indexes = @()
    $timestamp = Get-Date -UFormat "%Y%m%d_%H%M%S"
    $dropFileName = $dWorkingDirectory + "\CopyDatabaseTables_DropFile_" + $dServer + "_" + $dDatabase + "_" + $timestamp + ".sql"
    $workFileName = $dWorkingDirectory + "\CopyDatabaseTables_WorkFile_" + $dServer + "_" + $dDatabase + "_" + $timestamp + ".sql"
    $fkWorkFileName = $dWorkingDirectory + "\CopyDatabaseTables_FKWorkFile_" + $dServer+ "_" + $dDatabase+ "_" + $timestamp + ".sql"
    
    $dropOptions = New-Object Microsoft.SqlServer.Management.Smo.Scripter($serverObject)
    $dropOptions.options.ScriptDrops = $true
    $dropOptions.options.IncludeIfNotExists = $true
    $scriptingOptions = New-Object Microsoft.SqlServer.Management.Smo.Scripter($serverObject)
    $scriptingOptions.options.IncludeIfNotExists = $true
    $scriptingOptions.options.DriPrimaryKey = $true

    $fkscriptingOptions = New-Object Microsoft.SqlServer.Management.Smo.Scripter($serverObject)
    if ($noCheckConstraints -eq $true) { $fkscriptingOptions.options.DriWithNoCheck = $true }

    Write-Verbose "Scripting code to drop schema bound views..."
    foreach ($sbv in $sbviews)
    {
        $currentView = $sbv.Schema + "." + $sbv.Name
        $dropCode = $dropOptions.Script($sbv)
        $dropCode | Out-File $dropFileName -Append
    }

    Write-Verbose "Scripting drop commands for existing tables..."
    foreach ($m in $mdm)
    {
        $currentTable = $db.Tables | Where-Object {$_.Name -eq $m.Name -and $_.Schema -eq $m.Schema}
        if ($currentTable) {
            $dropCode = $dropOptions.Script($currentTable)
            $dropCode | Out-File $dropFileName -Append
        }
        $m.Script | Out-File $workFileName -Append
    }

    Write-Verbose "Scripting commands to recreate schema vound views..."
    foreach ($sbv in $sbviews)
    {
        $currentView = $sbv.Schema + "." + $sbv.Name
        $tblCode = $scriptingOptions.Script($sbv)
        $tblCode | Out-File $workFileName -Append
    }

    ForEach ($m in $mdm) 
    {
        $currentTable = $db.Tables | Where-Object {$_.Name -eq $m.Name -and $_.Schema -eq $m.Schema}
        if ($currentTable) {
    	    $currentTable.ForeignKeys.Refresh()
            $objectPermissions += $currentTable.EnumObjectPermissions()
            $foreignKeys += $currentTable.ForeignKeys
            $indexes += $currentTable.Indexes | Where-Object {$_.IndexKeyType -ne "DriPrimaryKey"}
        }
    }
    $foreignKeys += ($db.Tables | Where-Object {$_.ForeignKeys.ReferencedTableSchema -eq $dSchema}).ForeignKeys | Where-Object {$_.ReferencedTableSchema -eq $dSchema -and $foreignKeys -notcontains $_}

    ForEach ($p in $objectPermissions)
    {
        $permissionsString =  ($p.PermissionState).ToString() + " " + ($p.PermissionType).ToString() + " ON [" + ($p.ObjectSchema).ToString() + "].[" + ($p.ObjectName).ToString() + "] TO [" + ($p.Grantee).ToString() + "]";
        $permissionsString | Out-File $workFileName -Append
    }


    $totalIndexes = $indexes.Count
    ForEach ($in in $indexes)
    {
        $currentIndex = $in.name
        $inCode = $scriptingOptions.Script($in)
        $inCode | Out-File $workFileName -Append
    }

    $totalForeignKeys = $foreignKeys.Count
    if ($totalForeignKeys -gt 0) { Write-Verbose "Using script file: $workFileName (for foreign keys)" }

    foreach ($fk in $foreignKeys)
    {
        ($db.Tables | Where-Object {$_.Name -eq $fk.ReferencedTable -and $_.Schema -eq $fk.ReferencedTableSchema}).ForeignKeys.Refresh()
        $fkObject = ($db.Tables | Where-Object {$_.Name -eq $fk.Parent.Name -and $_.Schema -eq $fk.Parent.Schema}).ForeignKeys | Where-Object {$_.name -eq $fk.Name}
        $currentFKName = $fkObject.name
        Write-Verbose "Scripting foriegn key $currentFKName..."
        $fkCode = $fkscriptingOptions.Script($fkObject)
        $fkCode | Out-File $fkWorkFileName -Append
        $fkObject.Drop()
    }
    Write-Verbose "Applying script file $dropfilename to drop objects..."
    $dropCode = (Get-Content $dropFileName) -join [Environment]::NewLine
    $db.ExecuteWithResults($dropCode)
    Write-Verbose "Applying script file $dropfilename to recreate objects..."
    $workCode = (Get-Content $workfilename) -join [Environment]::NewLine
    $db.ExecuteWithResults($workCode)

    ForEach ($m in $mdm)
    {
        if ($db.Tables | Where-Object {$_.Name -eq $m.Name -and $_.Schema -eq $m.Schema})
        {
            $currentTable = $m.Schema + "." + $m.Name
            $targetConnection = New-Object System.Data.SqlClient.SqlConnection
            $targetConnectionString = "Server={0};Database={1};Trusted_Connection=True;Connection Timeout=15" -f $destinationSQLCmdServerInstance, $dDatabase
            $bcp = New-Object System.Data.SqlClient.SqlBulkCopy($targetConnectionString, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)
            $bcp.DestinationTableName = "{0}.{1}" -f  $m.Schema, $m.Name
            $bcp.BatchSize = 1000
            $bcp.SqlRowsCopied
            ForEach ($p in $m.properties)
            {
                $mapObj = New-Object System.Data.SqlClient.SqlBulkCopyColumnMapping($p.Name,$p.Name)
                [void]$bcp.ColumnMappings.Add($mapObj)
            }
            Write-Verbose "Writing data from source to $currentTable"
            $bcp.WriteToServer($m.data)
        } else {
            Write-Warning "The table $currentTable doesn't exist at the destination; use -REBUILD to copy the table"
        }
    }

    if ($totalForeignKeys -gt 0)
    {
        Write-Verbose "Applying FK workfile $fkworkfilename"
        $fkCode = (Get-Content $fkworkfilename) -join [Environment]::NewLine
        $db.ExecuteWithResults($fkCode)
    }

    $serverObject.ConnectionContext.Disconnect()

}


$SourceServerName = "SQLSERVERA"
$SourceInstanceName = "DEFAULT"
$SourceSchemaName = "Sales"
$SourceDatabaseName = "AdventureWorks2014"

$sourceSQLCmdServerInstance = $SourceServerName
$destinationSQLCmdServerInstance = $DestinationServerName
if ($SourceInstanceName -ne "DEFAULT") { $sourceSQLCmdServerInstance += "\" + $SourceInstanceName }
if ($DestinationInstanceName -ne "DEFAULT") { $destinationSQLCmdServerInstance += "\" + $DestinationInstanceName }
$StopWatch = [System.Diagnostics.Stopwatch]::StartNew()

Write-Host "Connecting to source instance..."
$sourceServer = New-Object Microsoft.SQLServer.Management.Smo.Server
$sourceServer.ConnectionContext.MultipleActiveResultSets = $true
$sourceServer.ConnectionContext.ServerInstance = $sourceSQLCmdServerInstance
$scriptingOptions = New-Object Microsoft.SqlServer.Management.Smo.Scripter($sourceServer)
$scriptingOptions.options.IncludeIfNotExists = $true
$scriptingOptions.options.DriPrimaryKey = $true

Write-Host "Getting source objects..."
$sourceDB = $sourceServer.Databases | Where-Object {$_.Name -eq $SourceDatabaseName}
$sourceTables = $SourceDB.Tables | Where-Object {$_.Schema -eq $sourceSchemaName}

$masterDataObjects = @()
Write-Host "Iterating over source tables..."
foreach ($st in $sourceTables)
{
    $tblCode = $scriptingOptions.Script($st)
    $currentData = $sourceDB.ExecuteWithResults("SELECT * FROM " + $st.Schema + "." + $st.Name)
    $properties = $st.columns | Where-Object {$_.Computed -eq $false}
    $dataTable = New-Object System.Data.DataTable

    ForEach ($p in $properties)
    {
        $dataTable.Columns.Add($p.Name) | Out-Null
		if ($p.DataType.SqlDataType -eq "uniqueidentifier") {$dataTable.Columns[$p.name].DataType = "guid"}
    }
    ForEach ($m in $currentData.tables.rows)
    {
        $newRow = $dataTable.NewRow()
        ForEach ($p in $properties)
        {
            if ($m.($p.Name) -eq $null) {
                $newRow[$p.Name] = [DBNull]::Value
            } else {
                $newRow[$p.Name] = $m.($p.Name)
            }
        }
        $dataTable.Rows.Add($newRow)
    }


    $masterData = [pscustomobject] @{
        Name = $st.Name
        Schema = $st.Schema
        Script = $tblCode
        Properties = $properties
        Data = $dataTable
    }

    $masterDataObjects += $masterData
}

$elapsed = $StopWatch.Elapsed
Write-Host "Source objects obtained, Elapsed time: $elapsed"

$computers = @("SQLSERVERB","SQLSERVERC","SQLSERVERB","SQLSERVERC")
$databases = @("TargetDBB1","TargetDBC1","TargetDBB2","TargetDBC2")
$instances = @("DEFAULT","DEFAULT","DEFAULT","DEFAULT")
$schemas = @("Sales","Sales","Sales","Sales")
$workingDirectory = "C:\temp"

Write-Host "Creating runspaces..."
For ($c = 0; $c -lt $computers.length; $c++)
{

    $runspaceObject = [PSCustomObject] @{
        Runspace = [PowerShell]::Create()
        Invoker = $null
    }
    $runspaceObject.Runspace.RunSpacePool = $runspacePool
    $runspaceObject.Runspace.AddScript($codeContainer) | Out-Null
    $runspaceObject.Runspace.AddParameter("dServer",$computers[$c]) | Out-Null
    $runspaceObject.Runspace.AddParameter("dInstance",$instances[$c]) | Out-Null
    $runspaceObject.Runspace.AddParameter("dDatabase",$databases[$c]) | Out-Null
    $runspaceObject.Runspace.AddParameter("dSchema",$schemas[$c]) | Out-Null
    $runspaceObject.Runspace.AddParameter("dNoCheckConstrains",$true) | Out-Null
    $runspaceObject.Runspace.AddParameter("mdm",$masterDataObjects) | Out-Null
    $runspaceObject.Runspace.AddParameter("dWorkingDirectory",$workingDirectory ) | Out-Null

    $runspaceObject.Invoker = $runspaceObject.Runspace.BeginInvoke()
    $threads += $runspaceObject

    $runSpaceName = $computers[$c] + "." + $databases[$c]
    $elapsed = $StopWatch.Elapsed
    Write-Host "Runspace created for $runSpaceName, Elapsed time: $elapsed"
}
$elapsed = $StopWatch.Elapsed
Write-Host "All runspaces created, elapsed time: $elapsed"

Write-Host "Waiting for threads to finish..."
while ($threads.Invoker.IsCompleted -contains $false) {}

$elapsed = $StopWatch.Elapsed
Write-Host "All threads completed, elapsed time: $elapsed"

Foreach ($t in $threads)
{
    $t.Runspace.Dispose()
}

$runspacePool.Close()
$runspacePool.Dispose()