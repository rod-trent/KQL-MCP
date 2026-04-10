"""KQL query template library — battle-tested patterns for common scenarios."""

from __future__ import annotations

QUERY_LIBRARY: dict[str, list[dict]] = {
    "security": [
        {
            "name": "Failed Login Attempts",
            "description": "Detect brute-force login failures by account and IP",
            "tags": ["sentinel", "security", "authentication", "brute-force"],
            "query": """SecurityEvent
| where TimeGenerated > ago(1h)
| where EventID == 4625
| summarize FailureCount = count(), DistinctComputers = dcount(Computer) by Account, IpAddress
| where FailureCount > 10
| sort by FailureCount desc
""",
        },
        {
            "name": "Successful Login After Multiple Failures",
            "description": "Detect successful login that follows a pattern of failures — possible credential stuffing",
            "tags": ["sentinel", "security", "authentication"],
            "query": """let FailedLogins = SecurityEvent
    | where TimeGenerated > ago(1h)
    | where EventID == 4625
    | summarize FailureCount = count() by Account, Computer;
SecurityEvent
| where TimeGenerated > ago(1h)
| where EventID == 4624
| join kind=inner (FailedLogins | where FailureCount > 5) on Account
| project TimeGenerated, Account, Computer, FailureCount
| sort by TimeGenerated desc
""",
        },
        {
            "name": "New Admin Account Created",
            "description": "Detect creation of new privileged accounts",
            "tags": ["sentinel", "security", "privilege-escalation"],
            "query": """SecurityEvent
| where TimeGenerated > ago(24h)
| where EventID in (4720, 4728, 4732, 4756)  // Account created or added to privileged group
| extend EventType = case(
    EventID == 4720, "Account Created",
    EventID == 4728, "Added to Global Group",
    EventID == 4732, "Added to Local Group",
    EventID == 4756, "Added to Universal Group",
    "Unknown"
)
| project TimeGenerated, EventType, Account, SubjectAccount, Computer
| sort by TimeGenerated desc
""",
        },
        {
            "name": "Azure Sign-In Risk",
            "description": "High-risk Azure AD sign-ins from Entra ID Protection",
            "tags": ["sentinel", "security", "identity", "entra"],
            "query": """SigninLogs
| where TimeGenerated > ago(24h)
| where RiskLevelDuringSignIn in ("high", "medium")
| project TimeGenerated, UserPrincipalName, AppDisplayName, RiskLevelDuringSignIn,
          RiskDetail, Location = tostring(LocationDetails), IPAddress
| sort by TimeGenerated desc
""",
        },
        {
            "name": "Impossible Travel Detection",
            "description": "Detect sign-ins from geographically distant locations within a short time window",
            "tags": ["sentinel", "security", "identity", "impossible-travel"],
            "query": """SigninLogs
| where TimeGenerated > ago(1d)
| where ResultType == 0  // Successful sign-ins only
| project TimeGenerated, UserPrincipalName, IPAddress,
          Country = tostring(LocationDetails.countryOrRegion),
          City = tostring(LocationDetails.city)
| sort by UserPrincipalName, TimeGenerated asc
| extend PrevTime = prev(TimeGenerated, 1),
         PrevCountry = prev(Country, 1),
         PrevUser = prev(UserPrincipalName, 1)
| where PrevUser == UserPrincipalName
| extend TimeDiffMinutes = datetime_diff('minute', TimeGenerated, PrevTime)
| where TimeDiffMinutes < 60 and Country != PrevCountry and isnotempty(Country)
| project TimeGenerated, UserPrincipalName, Country, PrevCountry, TimeDiffMinutes, IPAddress
""",
        },
        {
            "name": "Suspicious PowerShell Commands",
            "description": "Detect encoded or obfuscated PowerShell execution",
            "tags": ["sentinel", "security", "malware", "powershell"],
            "query": """SecurityEvent
| where TimeGenerated > ago(24h)
| where EventID == 4688  // Process creation
| where CommandLine has_any ("-EncodedCommand", "-enc ", "IEX", "Invoke-Expression",
                              "DownloadString", "DownloadFile", "WebClient",
                              "FromBase64String", "bypass", "-nop")
| project TimeGenerated, Computer, Account, ParentProcessName, CommandLine
| sort by TimeGenerated desc
""",
        },
        {
            "name": "Azure Resource Deletions",
            "description": "Track Azure resource deletion events by resource group",
            "tags": ["sentinel", "security", "azure", "resource-management"],
            "query": """AzureActivity
| where TimeGenerated > ago(24h)
| where OperationNameValue endswith "/delete"
| where ActivityStatusValue == "Success"
| project TimeGenerated, Caller, OperationNameValue, ResourceGroup, Resource
| summarize DeletedResources = make_list(Resource) by Caller, ResourceGroup
| sort by array_length(DeletedResources) desc
""",
        },
        {
            "name": "Network Connections to Suspicious IPs",
            "description": "Find outbound connections to non-RFC1918 IPs on unusual ports",
            "tags": ["sentinel", "security", "network", "threat-hunting"],
            "query": """AzureNetworkAnalytics_CL
| where TimeGenerated > ago(1h)
| where FlowDirection_s == "O"  // Outbound
| where not(ipv4_is_private(DestIP_s))
| where DestPort_d in (4444, 8080, 8443, 9999, 31337, 1337, 6667)
| project TimeGenerated, SrcIP_s, DestIP_s, DestPort_d, BytesSentInFlow_d
| summarize ConnectionCount = count(), TotalBytesSent = sum(BytesSentInFlow_d)
    by SrcIP_s, DestIP_s, DestPort_d
| sort by ConnectionCount desc
""",
        },
    ],
    "performance": [
        {
            "name": "CPU Utilization by Computer",
            "description": "Average and peak CPU utilization over the last hour",
            "tags": ["performance", "compute", "cpu"],
            "query": """Perf
| where TimeGenerated > ago(1h)
| where ObjectName == "Processor" and CounterName == "% Processor Time" and InstanceName == "_Total"
| summarize AvgCPU = avg(CounterValue), PeakCPU = max(CounterValue) by Computer, bin(TimeGenerated, 5m)
| sort by AvgCPU desc
""",
        },
        {
            "name": "Memory Usage by Computer",
            "description": "Available memory in MB over time",
            "tags": ["performance", "compute", "memory"],
            "query": """Perf
| where TimeGenerated > ago(1h)
| where ObjectName == "Memory" and CounterName == "Available MBytes"
| summarize AvailableMemoryMB = avg(CounterValue) by Computer, bin(TimeGenerated, 5m)
| sort by AvailableMemoryMB asc
""",
        },
        {
            "name": "Disk Latency",
            "description": "Disk read and write latency by computer and disk",
            "tags": ["performance", "storage", "disk"],
            "query": """Perf
| where TimeGenerated > ago(1h)
| where ObjectName == "LogicalDisk"
| where CounterName in ("Avg. Disk sec/Read", "Avg. Disk sec/Write")
| summarize AvgLatencyMs = avg(CounterValue * 1000) by Computer, InstanceName, CounterName, bin(TimeGenerated, 5m)
| sort by AvgLatencyMs desc
""",
        },
        {
            "name": "Top N Slow HTTP Requests",
            "description": "Find the slowest HTTP requests in Application Insights",
            "tags": ["performance", "appinsights", "http"],
            "query": """requests
| where timestamp > ago(1h)
| where success == false or duration > 2000
| project timestamp, name, url, resultCode, duration, cloud_RoleName
| top 50 by duration desc
""",
        },
        {
            "name": "Dependency Failures",
            "description": "Failed external dependency calls (SQL, HTTP, Service Bus) in Application Insights",
            "tags": ["performance", "appinsights", "dependencies"],
            "query": """dependencies
| where timestamp > ago(1h)
| where success == false
| summarize FailureCount = count(), AvgDuration = avg(duration)
    by name, type, target, resultCode
| sort by FailureCount desc
""",
        },
        {
            "name": "Exception Rate by Type",
            "description": "Top exceptions in Application Insights with trend",
            "tags": ["performance", "appinsights", "exceptions"],
            "query": """exceptions
| where timestamp > ago(24h)
| summarize Count = count() by type, outerMessage, cloud_RoleName
| sort by Count desc
| take 20
""",
        },
    ],
    "operations": [
        {
            "name": "Heartbeat Health Check",
            "description": "Identify computers that haven't sent a heartbeat in the last 15 minutes",
            "tags": ["operations", "monitoring", "availability"],
            "query": """Heartbeat
| where TimeGenerated > ago(24h)
| summarize LastSeen = max(TimeGenerated) by Computer
| where LastSeen < ago(15m)
| extend MinutesSinceLastSeen = datetime_diff('minute', now(), LastSeen)
| sort by MinutesSinceLastSeen desc
""",
        },
        {
            "name": "Service Health Events",
            "description": "Azure Service Health incidents and advisories",
            "tags": ["operations", "azure", "service-health"],
            "query": """ServiceHealthLogs_CL
| where TimeGenerated > ago(7d)
| where Status_s in ("Active", "Resolved")
| project TimeGenerated, Title_s, Status_s, ServiceName_s, Region_s, ImpactedServices_s
| sort by TimeGenerated desc
""",
        },
        {
            "name": "Azure Activity by Caller",
            "description": "Summarize Azure management operations by user/service principal",
            "tags": ["operations", "azure", "governance"],
            "query": """AzureActivity
| where TimeGenerated > ago(24h)
| where ActivityStatusValue == "Success"
| summarize OperationCount = count(),
    Operations = make_set(OperationNameValue, 20),
    ResourceGroups = dcount(ResourceGroup)
    by Caller
| sort by OperationCount desc
""",
        },
        {
            "name": "VM Start/Stop Events",
            "description": "Track virtual machine start and stop operations",
            "tags": ["operations", "azure", "compute"],
            "query": """AzureActivity
| where TimeGenerated > ago(7d)
| where OperationNameValue in ("Microsoft.Compute/virtualMachines/start/action",
                                "Microsoft.Compute/virtualMachines/deallocate/action",
                                "Microsoft.Compute/virtualMachines/restart/action")
| where ActivityStatusValue == "Success"
| project TimeGenerated, OperationNameValue, Resource, ResourceGroup, Caller
| sort by TimeGenerated desc
""",
        },
        {
            "name": "Log Analytics Ingestion Volume",
            "description": "Data ingestion volume by table over the last 7 days",
            "tags": ["operations", "log-analytics", "cost"],
            "query": """Usage
| where TimeGenerated > ago(7d)
| where IsBillable == true
| summarize IngestedGB = sum(Quantity) / 1024 by DataType, bin(TimeGenerated, 1d)
| sort by IngestedGB desc
""",
        },
        {
            "name": "Alert Rule Firings",
            "description": "Count of alert rule firings over the last week",
            "tags": ["operations", "sentinel", "alerts"],
            "query": """SecurityAlert
| where TimeGenerated > ago(7d)
| summarize AlertCount = count() by AlertName, AlertSeverity
| sort by AlertCount desc
""",
        },
    ],
    "kusto_adx": [
        {
            "name": "Query Execution Statistics",
            "description": "Analyze query performance using the .show queries management command result",
            "tags": ["adx", "admin", "performance"],
            "query": """.show queries
| where StartedOn > ago(1h)
| where State == "Completed"
| project StartedOn, Duration, Text, User, ResourceUtilization
| sort by Duration desc
| take 20
""",
        },
        {
            "name": "Table Ingestion Latency",
            "description": "Monitor how quickly data is ingested into ADX tables",
            "tags": ["adx", "ingestion", "monitoring"],
            "query": """.show ingestion failures
| where FailedOn > ago(24h)
| project FailedOn, OperationId, Database, Table, IngestionSourcePath, Details
| sort by FailedOn desc
""",
        },
        {
            "name": "Cluster Extents Statistics",
            "description": "Database and table-level storage usage",
            "tags": ["adx", "admin", "storage"],
            "query": """.show database extents
| summarize ExtentCount = count(), TotalSizeMB = sum(OriginalSize) / 1048576
    by TableName
| sort by TotalSizeMB desc
""",
        },
    ],
    "time_series": [
        {
            "name": "Anomaly Detection on Metric",
            "description": "Detect anomalies in a metric using series decomposition",
            "tags": ["time-series", "anomaly-detection", "ml"],
            "query": """// Replace 'Perf' and CounterValue with your metric table and column
Perf
| where TimeGenerated between (ago(7d)..now())
| where CounterName == "% Processor Time" and InstanceName == "_Total"
| make-series AvgCPU = avg(CounterValue) on TimeGenerated
    from ago(7d) to now() step 15m
    by Computer
| extend (Anomalies, Score, Baseline) = series_decompose_anomalies(AvgCPU, 1.5, -1, 'linefit')
| mv-expand TimeGenerated, AvgCPU, Anomalies, Score, Baseline
| where Anomalies == 1
| project TimeGenerated, Computer, AvgCPU, Score, Baseline
""",
        },
        {
            "name": "Forecast Next 24 Hours",
            "description": "Forecast a metric for the next 24 hours based on historical patterns",
            "tags": ["time-series", "forecasting", "ml"],
            "query": """Perf
| where TimeGenerated between (ago(14d)..now())
| where CounterName == "% Processor Time" and InstanceName == "_Total"
| make-series AvgCPU = avg(CounterValue) on TimeGenerated
    from ago(14d) to now() step 1h
    by Computer
| extend Forecast = series_decompose_forecast(AvgCPU, 24)
| project Computer, TimeGenerated, AvgCPU, Forecast
""",
        },
        {
            "name": "Event Rate Spike Detection",
            "description": "Detect sudden spikes in event volume",
            "tags": ["time-series", "anomaly-detection", "security"],
            "query": """SecurityEvent
| where TimeGenerated > ago(7d)
| make-series EventCount = count() on TimeGenerated
    from ago(7d) to now() step 1h
| extend (Anomalies, Score, Baseline) = series_decompose_anomalies(EventCount, 2.0)
| mv-expand TimeGenerated, EventCount, Anomalies, Score, Baseline
| where toint(Anomalies) == 1
| project TimeGenerated, EventCount, Score, Baseline = todouble(Baseline)
""",
        },
    ],
}


def get_templates_by_tag(tag: str) -> list[dict]:
    """Find all templates that have the specified tag."""
    tag_lower = tag.lower()
    results = []
    for category, templates in QUERY_LIBRARY.items():
        for template in templates:
            if any(tag_lower in t.lower() for t in template.get("tags", [])):
                results.append({**template, "category": category})
    return results


def get_templates_by_category(category: str) -> list[dict]:
    """Get all templates in a category."""
    return QUERY_LIBRARY.get(category, [])


def list_categories() -> list[str]:
    """List all available template categories."""
    return list(QUERY_LIBRARY.keys())


def search_templates(keyword: str) -> list[dict]:
    """Search templates by name, description, or query content."""
    kw = keyword.lower()
    results = []
    for category, templates in QUERY_LIBRARY.items():
        for template in templates:
            if (
                kw in template.get("name", "").lower()
                or kw in template.get("description", "").lower()
                or kw in template.get("query", "").lower()
                or any(kw in t.lower() for t in template.get("tags", []))
            ):
                results.append({**template, "category": category})
    return results
