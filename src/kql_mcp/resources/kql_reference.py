"""Comprehensive KQL language reference — operators, functions, and data types."""

from __future__ import annotations

KQL_REFERENCE: dict[str, dict] = {
    "tabular_operators": {
        "title": "KQL Tabular Operators",
        "description": "Operators that transform a table (applied with |)",
        "operators": {
            "where": {
                "syntax": "T | where Predicate",
                "description": "Filter rows that satisfy a predicate.",
                "examples": [
                    "SecurityEvent | where EventID == 4624",
                    "Heartbeat | where TimeGenerated > ago(1h)",
                    "AzureActivity | where Level == 'Error' and ResourceGroup contains 'prod'",
                ],
                "tips": [
                    "Place `where` clauses as early as possible to reduce data volume.",
                    "Use `has` instead of `contains` for better performance on large string columns.",
                    "Combine conditions with `and`/`or` — `and` is evaluated first.",
                ],
            },
            "summarize": {
                "syntax": "T | summarize [Aggregations] [by GroupColumns]",
                "description": "Collapse rows by computing aggregations, optionally grouped by columns.",
                "examples": [
                    "SecurityEvent | summarize count() by Computer",
                    "Perf | summarize avg(CounterValue) by CounterName, bin(TimeGenerated, 1h)",
                    "AzureActivity | summarize Events=count(), Errors=countif(Level=='Error') by ResourceGroup",
                ],
                "tips": [
                    "Use `bin()` to group timestamps into time buckets for time-series analysis.",
                    "Use `dcount()` for approximate distinct counts — much faster than `count(distinct ...)`.",
                    "Use `arg_max()` / `arg_min()` to get the row with the maximum/minimum value.",
                ],
            },
            "project": {
                "syntax": "T | project ColumnName [= Expression] [, ...]",
                "description": "Select, rename, or compute specific columns. Drops all others.",
                "examples": [
                    "SecurityEvent | project TimeGenerated, Computer, EventID, Account",
                    "Heartbeat | project Computer, OSType, ComputerIP=RemoteIPLongitude",
                    "AzureActivity | project TimeGenerated, Operation=OperationName, Status=ActivityStatus",
                ],
                "tips": [
                    "Always use `project` to select only needed columns — reduces data shuffled and speeds up queries.",
                    "Use `project-away` to drop specific columns and keep the rest.",
                    "Use `project-rename` to rename without changing column order.",
                ],
            },
            "extend": {
                "syntax": "T | extend ColumnName = Expression [, ...]",
                "description": "Add new computed columns without dropping existing ones.",
                "examples": [
                    "SecurityEvent | extend HourOfDay = hourofday(TimeGenerated)",
                    "Perf | extend MemoryGB = CounterValue / 1024",
                    "AzureActivity | extend IsFailure = (ActivityStatus == 'Failed')",
                ],
            },
            "join": {
                "syntax": "T1 | join kind=JoinType (T2) on Key",
                "description": "Join two tables on one or more keys. Join types: inner, leftouter, rightouter, fullouter, leftanti, rightanti, leftsemi.",
                "examples": [
                    "SecurityEvent | join kind=inner (Heartbeat | project Computer, OSType) on Computer",
                    "SigninLogs | join kind=leftouter (AADNonInteractiveUserSignInLogs | summarize count() by UserPrincipalName) on UserPrincipalName",
                ],
                "tips": [
                    "Put the smaller table on the right side of the join (inside the parentheses).",
                    "Use `leftsemi` or `leftanti` when you only need to filter — much faster than a full join.",
                    "Avoid `fullouter` join on large tables — use sparingly.",
                    "Use `hint.strategy=shuffle` for very large joins to distribute load.",
                ],
            },
            "union": {
                "syntax": "union [kind=inner|outer] T1, T2, ...",
                "description": "Combine rows from multiple tables.",
                "examples": [
                    "union SecurityEvent, WindowsEvent | where TimeGenerated > ago(1h)",
                    "union withsource=TableName SecurityEvent, Syslog | summarize count() by TableName",
                ],
                "tips": [
                    "Use `withsource=TableName` to track which table each row came from.",
                    "Avoid `union *` — it scans all tables and is very expensive.",
                ],
            },
            "sort": {
                "syntax": "T | sort by Column [asc|desc] [nulls first|last] [, ...]",
                "description": "Sort rows. Alias: `order by`.",
                "examples": [
                    "SecurityEvent | sort by TimeGenerated desc",
                    "Perf | sort by CounterValue desc nulls last",
                ],
            },
            "top": {
                "syntax": "T | top N by Column [asc|desc]",
                "description": "Return the top N rows sorted by a column. More efficient than sort+take.",
                "examples": [
                    "SecurityEvent | top 10 by TimeGenerated desc",
                    "Perf | top 5 by CounterValue desc",
                ],
            },
            "take": {
                "syntax": "T | take N",
                "description": "Return N rows (arbitrary order). Use for sampling. Alias: `limit`.",
                "examples": [
                    "SecurityEvent | take 100",
                    "Heartbeat | where Computer startswith 'web' | take 10",
                ],
            },
            "distinct": {
                "syntax": "T | distinct Column [, ...]",
                "description": "Return distinct combinations of specified columns.",
                "examples": [
                    "SecurityEvent | distinct Computer",
                    "SigninLogs | distinct UserPrincipalName, AppDisplayName",
                ],
            },
            "count": {
                "syntax": "T | count",
                "description": "Return the number of rows in the table.",
                "examples": ["SecurityEvent | where EventID == 4625 | count"],
            },
            "render": {
                "syntax": "T | render ChartType [with (property=value, ...)]",
                "description": "Visualize results as a chart. Types: timechart, barchart, piechart, table, scatterchart.",
                "examples": [
                    "SecurityEvent | summarize count() by bin(TimeGenerated, 1h) | render timechart",
                    "Perf | summarize avg(CounterValue) by CounterName | render barchart",
                ],
            },
            "parse": {
                "syntax": "T | parse [kind=regex|simple|relaxed] Expression with Pattern",
                "description": "Extract structured data from unstructured strings.",
                "examples": [
                    "Syslog | parse SyslogMessage with 'user=' User ' action=' Action ' src=' Source",
                    "AzureActivity | parse Properties with * 'resourceType\":\"' ResourceType '\"' *",
                ],
            },
            "mv-expand": {
                "syntax": "T | mv-expand [bagexpansion=bag|array] Column [to typeof(Type)] [limit N]",
                "description": "Expand a dynamic array or property bag into multiple rows.",
                "examples": [
                    "T | mv-expand Tags",
                    "AzureActivity | mv-expand Claims = todynamic(Claims)",
                ],
                "tips": [
                    "Use `mv-expand` to work with JSON arrays stored in dynamic columns.",
                    "Use `limit` to cap expansion and avoid row explosion.",
                ],
            },
            "mv-apply": {
                "syntax": "T | mv-apply Column [to typeof(Type)] on (SubQuery)",
                "description": "Apply a subquery to each element of a dynamic array. More powerful than mv-expand.",
                "examples": [
                    "T | mv-apply Item = arr to typeof(int) on (summarize s = sum(Item))",
                ],
            },
            "evaluate": {
                "syntax": "T | evaluate PluginName([Parameters])",
                "description": "Call table-valued extension plugins.",
                "examples": [
                    "SecurityEvent | evaluate bag_unpack(EventData)",
                    "T | evaluate autocluster()",
                    "T | evaluate diffpatterns(splitColumn, 'v1', 'v2')",
                    "T | evaluate basket()",
                ],
            },
            "make-series": {
                "syntax": "T | make-series [DefaultValue=N] Column=Aggregation on Axis [from Start] [to End] step Step [by GroupColumn]",
                "description": "Create a time series for use with series analysis functions.",
                "examples": [
                    "Perf | make-series avg(CounterValue) on TimeGenerated from ago(7d) to now() step 1h by Computer",
                ],
                "tips": [
                    "Feed results into `series_decompose_anomalies()` for anomaly detection.",
                    "Use with `series_stats()` to get statistics across the time series.",
                ],
            },
            "lookup": {
                "syntax": "T | lookup [kind=leftouter|inner] (LookupTable) on Key",
                "description": "Optimized join for enriching rows with a static lookup table. The lookup table is broadcast to all nodes.",
                "examples": [
                    "SecurityEvent | lookup (ComputerMetadata | project Computer, Owner) on Computer",
                ],
                "tips": ["Faster than `join` when the right-hand table is small and static."],
            },
            "partition": {
                "syntax": "T | partition by Column (SubQuery)",
                "description": "Run a subquery independently for each partition of a column's values.",
                "examples": [
                    "T | partition by Computer (top 3 by TimeGenerated desc)",
                ],
            },
            "as": {
                "syntax": "T | as [hint.materialized=true] Name",
                "description": "Assign a name to a tabular expression for later use in the same query.",
                "examples": [
                    "SecurityEvent | where EventID == 4625 | as FailedLogins",
                ],
            },
            "let": {
                "syntax": "let Name = Expression;\nlet Name = (Parameters) { Body };",
                "description": "Declare a variable or function for reuse within the query.",
                "examples": [
                    "let timeRange = ago(7d);\nSecurityEvent | where TimeGenerated > timeRange",
                    "let FailedLogins = SecurityEvent | where EventID == 4625;\nFailedLogins | summarize count() by Computer",
                    "let GetEvents = (computer:string) { SecurityEvent | where Computer == computer };\nGetEvents('DC01')",
                ],
                "tips": [
                    "Use `let` to avoid repeating complex subqueries.",
                    "Functions defined with `let` can take typed parameters.",
                    "Use `materialize()` to cache an expensive expression: `let cached = materialize(T | ...);`",
                ],
            },
        },
    },
    "scalar_functions": {
        "title": "KQL Scalar Functions",
        "description": "Functions that operate on individual values",
        "categories": {
            "string": {
                "title": "String Functions",
                "functions": {
                    "tostring": "tostring(value) — Convert any value to string",
                    "strlen": "strlen(text) — String length",
                    "substring": "substring(text, start[, length]) — Extract substring",
                    "toupper": "toupper(text) — Convert to uppercase",
                    "tolower": "tolower(text) — Convert to lowercase",
                    "trim": "trim(regex, text) — Trim matching characters from both ends",
                    "trim_start": "trim_start(regex, text) — Trim from start",
                    "trim_end": "trim_end(regex, text) — Trim from end",
                    "split": "split(text, delimiter[, index]) — Split string; returns dynamic array or element at index",
                    "strcat": "strcat(str1, str2, ...) — Concatenate strings",
                    "strcat_delim": "strcat_delim(delimiter, str1, str2, ...) — Concatenate with delimiter",
                    "replace_string": "replace_string(text, lookup, replacement) — Replace all occurrences",
                    "replace_regex": "replace_regex(text, regex, replacement) — Regex replace",
                    "contains": "x contains y — True if x contains y (case-insensitive)",
                    "has": "x has y — True if x has y as a complete term (faster than contains)",
                    "has_any": "x has_any (list) — True if x contains any term in the list",
                    "has_all": "x has_all (list) — True if x contains all terms in the list",
                    "startswith": "x startswith y — True if x starts with y",
                    "endswith": "x endswith y — True if x ends with y",
                    "matches regex": "x matches regex pattern — Regex match",
                    "extract": "extract(regex, captureGroup, text[, typeof]) — Extract regex match",
                    "extract_all": "extract_all(regex, text) — Extract all regex matches",
                    "parse_json": "parse_json(text) — Parse JSON string to dynamic",
                    "parse_url": "parse_url(url) — Parse URL into components",
                    "parse_ipv4": "parse_ipv4(ip) — Parse IPv4 address to long",
                    "parse_ipv6": "parse_ipv6(ip) — Parse IPv6 address",
                    "url_decode": "url_decode(encoded) — URL-decode a string",
                    "url_encode": "url_encode(text) — URL-encode a string",
                    "base64_encode_tostring": "base64_encode_tostring(text) — Base64 encode",
                    "base64_decode_tostring": "base64_decode_tostring(encoded) — Base64 decode",
                    "hash": "hash(value[, mod]) — Compute hash of value",
                    "hash_sha256": "hash_sha256(value) — SHA-256 hash",
                    "hash_md5": "hash_md5(value) — MD5 hash",
                    "countof": "countof(text, search[, kind]) — Count occurrences of search in text",
                    "indexof": "indexof(text, lookup[, start[, length[, occurrence]]]) — Find index of lookup in text",
                },
            },
            "datetime": {
                "title": "DateTime Functions",
                "functions": {
                    "now": "now([offset]) — Current UTC time, optionally offset by a timespan",
                    "ago": "ago(timespan) — Time in the past relative to now. E.g., ago(1h), ago(7d)",
                    "datetime": "datetime(value) — Create a datetime literal. E.g., datetime(2024-01-01)",
                    "todatetime": "todatetime(value) — Convert string or number to datetime",
                    "bin": "bin(value, roundTo) — Round down to a multiple. E.g., bin(TimeGenerated, 1h)",
                    "bin_at": "bin_at(value, step, fixed_point) — Bin at a fixed alignment",
                    "startofday": "startofday(datetime[, offset]) — Start of the day",
                    "startofweek": "startofweek(datetime[, offset]) — Start of the week",
                    "startofmonth": "startofmonth(datetime[, offset]) — Start of the month",
                    "startofyear": "startofyear(datetime[, offset]) — Start of the year",
                    "endofday": "endofday(datetime[, offset]) — End of the day",
                    "endofweek": "endofweek(datetime[, offset]) — End of the week",
                    "endofmonth": "endofmonth(datetime[, offset]) — End of the month",
                    "hourofday": "hourofday(datetime) — Hour of the day (0-23)",
                    "dayofweek": "dayofweek(datetime) — Day of the week as timespan (Sunday=0d)",
                    "dayofmonth": "dayofmonth(datetime) — Day of the month (1-31)",
                    "dayofyear": "dayofyear(datetime) — Day of the year (1-366)",
                    "weekofyear": "weekofyear(datetime) — ISO week number",
                    "monthofyear": "monthofyear(datetime) — Month (1-12)",
                    "getyear": "getyear(datetime) — Year",
                    "datetime_diff": "datetime_diff(period, datetime1, datetime2) — Difference in the specified period",
                    "datetime_add": "datetime_add(period, number, datetime) — Add time to a datetime",
                    "datetime_part": "datetime_part(part, datetime) — Extract a date/time part",
                    "format_datetime": "format_datetime(datetime, format) — Format a datetime as string",
                    "format_timespan": "format_timespan(timespan, format) — Format a timespan as string",
                    "totimespan": "totimespan(value) — Convert string to timespan",
                    "tolong": "tolong(value) — Convert to long integer",
                    "unixtime_seconds_todatetime": "unixtime_seconds_todatetime(seconds) — Convert Unix timestamp to datetime",
                    "unixtime_milliseconds_todatetime": "unixtime_milliseconds_todatetime(ms) — Convert Unix ms timestamp",
                    "datetime_utc_to_local": "datetime_utc_to_local(datetime, timezone) — Convert UTC to local time",
                },
            },
            "math": {
                "title": "Math & Statistical Functions",
                "functions": {
                    "abs": "abs(x) — Absolute value",
                    "ceiling": "ceiling(x) — Round up to nearest integer",
                    "floor": "floor(x, roundTo) — Round down",
                    "round": "round(x[, precision]) — Round to nearest integer or specified decimals",
                    "pow": "pow(base, exponent) — Power",
                    "sqrt": "sqrt(x) — Square root",
                    "log": "log(x[, base]) — Logarithm (default: natural log)",
                    "log2": "log2(x) — Base-2 logarithm",
                    "log10": "log10(x) — Base-10 logarithm",
                    "exp": "exp(x) — e^x",
                    "exp2": "exp2(x) — 2^x",
                    "sign": "sign(x) — Sign of x (-1, 0, or 1)",
                    "rand": "rand([N]) — Random number [0,1) or random int [0,N)",
                    "beta_cdf": "beta_cdf(x, alpha, beta) — Beta distribution CDF",
                    "gamma": "gamma(x) — Gamma function",
                    "iff": "iff(condition, ifTrue, ifFalse) — Inline conditional (alias: iif)",
                    "coalesce": "coalesce(val1, val2, ...) — First non-null value",
                    "isnan": "isnan(x) — True if x is NaN",
                    "isinf": "isinf(x) — True if x is infinite",
                    "isfinite": "isfinite(x) — True if x is finite",
                    "isnull": "isnull(x) — True if x is null",
                    "isnotnull": "isnotnull(x) — True if x is not null",
                    "isempty": "isempty(x) — True if x is null or empty string",
                    "isnotempty": "isnotempty(x) — True if x is not null and not empty",
                    "todouble": "todouble(x) — Convert to double",
                    "toint": "toint(x) — Convert to int",
                    "tolong": "tolong(x) — Convert to long",
                    "tobool": "tobool(x) — Convert to bool",
                },
            },
            "dynamic_json": {
                "title": "Dynamic / JSON Functions",
                "functions": {
                    "todynamic": "todynamic(text) — Parse JSON text into a dynamic value",
                    "tostring": "tostring(dynamic) — Serialize dynamic to JSON string",
                    "array_length": "array_length(arr) — Length of a dynamic array",
                    "array_slice": "array_slice(arr, start, end) — Slice an array",
                    "array_concat": "array_concat(arr1, arr2, ...) — Concatenate arrays",
                    "array_split": "array_split(arr, indices) — Split array at indices",
                    "array_reverse": "array_reverse(arr) — Reverse an array",
                    "array_sort_asc": "array_sort_asc(arr) — Sort array ascending",
                    "array_sort_desc": "array_sort_desc(arr) — Sort array descending",
                    "array_sum": "array_sum(arr) — Sum numeric array",
                    "array_index_of": "array_index_of(arr, value) — Index of value in array (-1 if not found)",
                    "array_iif": "array_iif(condArr, ifTrueArr, ifFalseArr) — Element-wise conditional",
                    "bag_keys": "bag_keys(bag) — Keys of a property bag as array",
                    "bag_values": "bag_values(bag) — Values of a property bag as array",
                    "bag_has_key": "bag_has_key(bag, key) — True if bag contains key",
                    "bag_merge": "bag_merge(bag1, bag2, ...) — Merge property bags",
                    "bag_remove_keys": "bag_remove_keys(bag, keys) — Remove keys from bag",
                    "bag_pack": "bag_pack(key1, val1, ...) — Create a property bag",
                    "pack_array": "pack_array(val1, val2, ...) — Create a dynamic array",
                    "zip": "zip(arr1, arr2, ...) — Zip arrays into array of arrays",
                    "set_union": "set_union(arr1, arr2, ...) — Union of sets",
                    "set_intersect": "set_intersect(arr1, arr2, ...) — Intersection of sets",
                    "set_difference": "set_difference(arr1, arr2) — Set difference",
                    "set_has_element": "set_has_element(set, value) — True if set contains value",
                },
            },
            "ip": {
                "title": "IP Address Functions",
                "functions": {
                    "ipv4_is_match": "ipv4_is_match(ip1, ip2[, prefix]) — Check if IPs match with optional prefix length",
                    "ipv4_is_private": "ipv4_is_private(ip) — True if IP is in a private range (RFC 1918)",
                    "ipv4_is_in_range": "ipv4_is_in_range(ip, ipRange) — True if IP is in the specified CIDR range",
                    "ipv4_is_in_any_range": "ipv4_is_in_any_range(ip, ranges) — True if IP is in any of the specified ranges",
                    "ipv4_compare": "ipv4_compare(ip1, ip2) — Compare two IPv4 addresses",
                    "ipv4_range_to_cidr_list": "ipv4_range_to_cidr_list(start, end) — Convert IP range to CIDR list",
                    "ipv6_is_match": "ipv6_is_match(ip1, ip2[, prefix]) — IPv6 match",
                    "ipv6_compare": "ipv6_compare(ip1, ip2) — Compare two IPv6 addresses",
                    "ipv4_netmask_suffix": "ipv4_netmask_suffix(ip) — Get CIDR prefix length from netmask",
                    "geo_point_to_geohash": "geo_point_to_geohash(lon, lat[, accuracy]) — Convert coordinates to geohash",
                    "geo_geohash_to_central_point": "geo_geohash_to_central_point(geohash) — Get center of geohash",
                },
            },
        },
    },
    "aggregation_functions": {
        "title": "KQL Aggregation Functions",
        "description": "Functions used inside `summarize` to aggregate rows into groups",
        "functions": {
            "count": "count() — Count rows in the group",
            "countif": "countif(predicate) — Count rows where predicate is true",
            "dcount": "dcount(column[, accuracy]) — Approximate distinct count (fast). Accuracy 0-4 (default 1)",
            "dcountif": "dcountif(column, predicate[, accuracy]) — Approximate distinct count where predicate is true",
            "sum": "sum(column) — Sum of values",
            "sumif": "sumif(column, predicate) — Sum where predicate is true",
            "avg": "avg(column) — Average of values",
            "avgif": "avgif(column, predicate) — Average where predicate is true",
            "min": "min(column) — Minimum value",
            "minif": "minif(column, predicate) — Minimum where predicate is true",
            "max": "max(column) — Maximum value",
            "maxif": "maxif(column, predicate) — Maximum where predicate is true",
            "stdev": "stdev(column) — Standard deviation",
            "variance": "variance(column) — Variance",
            "percentile": "percentile(column, percentile) — Nth percentile. E.g., percentile(Duration, 95)",
            "percentiles": "percentiles(column, p1, p2, ...) — Multiple percentiles in one pass",
            "percentile_tdigest": "percentile_tdigest(tdigest, percentile) — Percentile from a tdigest",
            "tdigest": "tdigest(column) — Build a t-digest for later percentile computation",
            "tdigest_merge": "tdigest_merge(tdigest) — Merge t-digests",
            "arg_max": "arg_max(maximizeExpr, * | column, ...) — Row with the max value of maximizeExpr",
            "arg_min": "arg_min(minimizeExpr, * | column, ...) — Row with the min value of minimizeExpr",
            "any": "any(column) — Return an arbitrary value from the group",
            "anyif": "anyif(column, predicate) — Return arbitrary value where predicate is true",
            "take_any": "take_any(column) — Return an arbitrary (non-null-preferred) value",
            "make_list": "make_list(column[, maxListSize]) — Build a dynamic array of column values",
            "make_list_if": "make_list_if(column, predicate[, maxListSize]) — Build list where predicate is true",
            "make_set": "make_set(column[, maxSetSize]) — Build a dynamic array of distinct values",
            "make_set_if": "make_set_if(column, predicate[, maxSetSize]) — Build distinct set where predicate is true",
            "make_bag": "make_bag(column[, maxBagSize]) — Merge property bags in the group",
            "make_bag_if": "make_bag_if(column, predicate[, maxBagSize]) — Merge bags where predicate is true",
            "hll": "hll(column[, accuracy]) — HyperLogLog sketch for dcount",
            "hll_merge": "hll_merge(hll) — Merge HLL sketches",
            "dcount_hll": "dcount_hll(hll) — Compute dcount from HLL sketch",
            "buildschema": "buildschema(column) — Infer the schema of a dynamic column",
        },
    },
    "window_functions": {
        "title": "KQL Window Functions",
        "description": "Functions that compute values across a sliding window of rows",
        "functions": {
            "row_number": "row_number([startingIndex[, restart]]) — Sequential row number within partition",
            "row_rank_dense": "row_rank_dense(column) — Dense rank based on column value",
            "row_rank_min": "row_rank_min(column) — Min rank based on column value",
            "prev": "prev(column[, offset[, default]]) — Value of column N rows before current row",
            "next": "next(column[, offset[, default]]) — Value of column N rows after current row",
            "row_cumsum": "row_cumsum(column[, restart]) — Cumulative sum",
            "row_window_session": "row_window_session(column, maxDistFromStart, maxDistBetweenNeighbors[, restart]) — Session windowing",
        },
        "notes": "Window functions require data to be sorted. Use `serialize` before window functions to guarantee row order.",
    },
    "series_functions": {
        "title": "KQL Time Series Functions",
        "description": "Functions for analyzing time series data produced by make-series",
        "functions": {
            "series_stats": "series_stats(series[, ignore_nonfinite]) — Statistics (min, max, avg, stdev, variance) of a series",
            "series_stats_dynamic": "series_stats_dynamic(series) — Returns stats as a property bag",
            "series_decompose": "series_decompose(series[, ...]) — Decompose into baseline + seasonal + trend + residual",
            "series_decompose_anomalies": "series_decompose_anomalies(series[, threshold, seasonality, ...]) — Detect anomalies",
            "series_decompose_forecast": "series_decompose_forecast(series, points[, ...]) — Forecast future values",
            "series_outliers": "series_outliers(series[, threshold, ignore_val, min_percentile, max_percentile]) — Detect outlier points",
            "series_fit_line": "series_fit_line(series) — Linear regression fit",
            "series_fit_2lines": "series_fit_2lines(series) — Two-segment linear regression",
            "series_fit_poly": "series_fit_poly(series, degree[, ...]) — Polynomial regression fit",
            "series_periods_detect": "series_periods_detect(series, min_period, max_period, num_periods) — Detect periodic patterns",
            "series_periods_validate": "series_periods_validate(series, period, ...) — Validate periods",
            "series_fill_backward": "series_fill_backward(series[, fill_value]) — Fill missing values backward",
            "series_fill_forward": "series_fill_forward(series[, fill_value]) — Fill missing values forward",
            "series_fill_linear": "series_fill_linear(series[, ...]) — Linear interpolation for missing values",
            "series_fill_const": "series_fill_const(series[, fill_value, missing_value]) — Fill with constant",
            "series_add": "series_add(series1, series2) — Element-wise addition",
            "series_subtract": "series_subtract(series1, series2) — Element-wise subtraction",
            "series_multiply": "series_multiply(series1, series2) — Element-wise multiplication",
            "series_divide": "series_divide(series1, series2) — Element-wise division",
            "series_greater": "series_greater(series1, series2) — Element-wise greater-than",
            "series_equals": "series_equals(series1, series2) — Element-wise equality",
            "series_sum": "series_sum(series) — Sum of all elements",
            "series_product": "series_product(series) — Product of all elements",
            "series_abs": "series_abs(series) — Element-wise absolute value",
            "series_not": "series_not(series) — Element-wise logical NOT",
        },
    },
    "data_types": {
        "title": "KQL Data Types",
        "types": {
            "bool": "Boolean: true or false",
            "int": "32-bit signed integer: int(42)",
            "long": "64-bit signed integer: long(42) or 42L",
            "real": "64-bit floating-point: real(3.14) or 3.14",
            "decimal": "128-bit decimal: decimal(3.14159265358979323846)",
            "string": "Unicode text: 'hello' or \"hello\"",
            "datetime": "Date and time: datetime(2024-01-01 12:00:00)",
            "timespan": "Duration: timespan(1h) or 1h or 00:01:00",
            "guid": "Globally unique identifier: guid(null) or a GUID literal",
            "dynamic": "JSON-like value (scalar, array, or property bag): dynamic({'a': 1})",
        },
        "timespan_literals": {
            "description": "Timespan literals use suffixes",
            "examples": {
                "1d": "1 day",
                "2h": "2 hours",
                "30m": "30 minutes",
                "1h30m": "1 hour 30 minutes",
                "1.5h": "1.5 hours",
                "00:30:00": "30 minutes (HH:MM:SS format)",
            },
        },
    },
    "best_practices": {
        "title": "KQL Best Practices",
        "performance": [
            "Filter early: place `| where` clauses as early as possible to reduce data volume.",
            "Use `has` instead of `contains` for better full-text search performance (word boundary matching).",
            "Use `has_any()` and `has_all()` for multi-term full-text filtering.",
            "Avoid leading wildcards in `startswith` or `contains` — they prevent index use.",
            "Use `project` to select only needed columns — reduces I/O.",
            "Prefer `dcount()` over `count(distinct ...)` for approximate distinct counts (much faster).",
            "Use `bin()` for time bucketing — it's index-friendly.",
            "Materialize expensive subqueries used multiple times: `let t = materialize(T | ...);`",
            "In joins, put the smaller table on the right side (inside parentheses).",
            "Use `lookup` instead of `join` when the right-hand table is small and static.",
            "Avoid `search *` and `union *` — they scan all tables.",
            "Use `arg_max()` / `arg_min()` instead of `sort | take 1` for top-N by group.",
        ],
        "readability": [
            "Use `let` to name complex subqueries and reuse them.",
            "Format multi-step queries vertically with one operator per line.",
            "Use `//` for comments in KQL.",
            "Name aggregations clearly: `| summarize EventCount=count(), DistinctComputers=dcount(Computer) by ...`",
            "Use `project-rename` to give clearer names to columns.",
            "Use `project-away` to drop unneeded columns rather than listing all kept columns.",
        ],
        "security": [
            "Always scope queries with time filters (TimeGenerated, timestamp).",
            "Use workspace/database-level permissions instead of row-level filtering where possible.",
            "Avoid storing secrets or PII in query text or results.",
            "Use `search` in Log Analytics Sentinel only when you don't know the table — prefer specific tables.",
        ],
    },
}


def get_operator_help(operator: str) -> str:
    """Get detailed help for a specific KQL operator."""
    ops = KQL_REFERENCE.get("tabular_operators", {}).get("operators", {})
    op = ops.get(operator.lower())
    if not op:
        return f"Operator `{operator}` not found. Available: {', '.join(ops.keys())}"

    lines = [
        f"## `{operator}`",
        f"\n{op['description']}",
        f"\n**Syntax:** `{op['syntax']}`",
    ]

    if op.get("examples"):
        lines.append("\n**Examples:**")
        for ex in op["examples"]:
            lines.append(f"```kql\n{ex}\n```")

    if op.get("tips"):
        lines.append("\n**Tips:**")
        for tip in op["tips"]:
            lines.append(f"- {tip}")

    return "\n".join(lines)


def get_function_help(function_name: str) -> str:
    """Get help for a specific KQL scalar function."""
    fn_lower = function_name.lower()
    categories = KQL_REFERENCE.get("scalar_functions", {}).get("categories", {})
    for cat_name, cat in categories.items():
        for fname, fdesc in cat.get("functions", {}).items():
            if fn_lower in fname.lower():
                return f"**`{fname}`** ({cat['title']})\n\n{fdesc}"
    return f"Function `{function_name}` not found in reference."


def search_reference(keyword: str) -> list[str]:
    """Search the entire KQL reference for a keyword."""
    kw = keyword.lower()
    results = []

    # Search operators
    for op_name, op in KQL_REFERENCE.get("tabular_operators", {}).get("operators", {}).items():
        if kw in op_name or kw in op.get("description", "").lower():
            results.append(f"**Operator** `{op_name}`: {op['description'][:100]}")

    # Search functions
    for cat in KQL_REFERENCE.get("scalar_functions", {}).get("categories", {}).values():
        for fname, fdesc in cat.get("functions", {}).items():
            if kw in fname.lower() or kw in fdesc.lower():
                results.append(f"**Function** `{fname}`: {fdesc[:100]}")

    # Search aggregations
    for fname, fdesc in KQL_REFERENCE.get("aggregation_functions", {}).get("functions", {}).items():
        if kw in fname.lower() or kw in fdesc.lower():
            results.append(f"**Aggregation** `{fname}`: {fdesc[:100]}")

    return results
