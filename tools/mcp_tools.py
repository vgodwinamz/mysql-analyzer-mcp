"""
MCP tool definitions for MySQL Performance Analyzer.
This file contains all the tool functions that are registered with the MCP server.
"""
import json
import time
from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import Context, FastMCP

from db.connector import MySQLConnector
from analysis.structure import (
    get_database_structure, 
    organize_db_structure_by_table,
    analyze_database_structure_for_response
)
from analysis.query import (
    extract_tables_from_query, 
    get_table_statistics, 
    get_schema_information, 
    get_index_information,
    format_query_analysis_response
)
from analysis.patterns import (
    detect_query_patterns, 
    detect_query_anti_patterns, 
    validate_read_only_query
)
from analysis.indexes import (
    extract_potential_indexes,
    get_table_structure_for_index,
    check_existing_indexes,
    format_index_recommendations_response
)

def register_all_tools(mcp: FastMCP):
    """Register all tools with the MCP server"""
    
    @mcp.tool()
    async def analyze_database_structure(secret_name: str = None, region_name: str = "us-west-2", ctx: Context = None) -> str:
        """
        Analyze the database structure and provide insights on schema design, indexes, and potential optimizations.
        
        Args:
            secret_name: AWS Secrets Manager secret name containing database credentials (required)
            region_name: AWS region where the secret is stored (default: us-west-2)
        
        Returns:
            A comprehensive analysis of the database structure with optimization recommendations
        """
        # Check if secret_name is provided
        if not secret_name:
            return "Error: Please provide a valid AWS Secrets Manager secret name containing database credentials."
        
        # Initialize connector with the provided secret name
        connector = MySQLConnector(
            secret_name=secret_name,
            region_name=region_name
        )
        
        try:
            if not connector.connect():
                return f"Failed to connect to database using secret '{secret_name}'. Please check your credentials."
            
            # Get comprehensive database structure
            db_structure = get_database_structure(connector)
            
            # Generate the formatted response
            response = analyze_database_structure_for_response(db_structure)
            
            return response
        except Exception as e:
            return f"Error analyzing database structure: {str(e)}"
        finally:
            # Always disconnect when done
            connector.disconnect()
    
    @mcp.tool()
    async def get_slow_queries(secret_name: str = None, region_name: str = "us-west-2", 
                              min_execution_time: int = 100, limit: int = 10, ctx: Context = None) -> str:
        """
        Identify slow-running queries in the database.
        
        Args:
            secret_name: AWS Secrets Manager secret name containing database credentials (required)
            region_name: AWS region where the secret is stored (default: us-west-2)
            min_execution_time: Minimum execution time in milliseconds (default: 100ms)
            limit: Maximum number of queries to return (default: 10)
        
        Returns:
            A list of slow queries with their execution statistics and analysis
        """
        # Check if secret_name is provided
        if not secret_name:
            return "Error: Please provide a valid AWS Secrets Manager secret name containing database credentials."
        
        # Initialize connector with the provided secret name
        connector = MySQLConnector(
            secret_name=secret_name,
            region_name=region_name
        )
        
        try:
            if not connector.connect():
                return f"Failed to connect to database using secret '{secret_name}'. Please check your credentials."
            
            # Check if slow query log is enabled
            log_status_query = "SHOW VARIABLES LIKE 'slow_query_log'"
            log_status = connector.execute_query(log_status_query)
            
            if not log_status or log_status[0]['Value'].lower() != 'on':
                return """
                    The slow query log is not enabled. To enable it, run:
                    
                    ```sql
                    SET GLOBAL slow_query_log = 'ON';
                    SET GLOBAL long_query_time = 1;  -- Set threshold in seconds
                    SET GLOBAL slow_query_log_file = '/var/lib/mysql/slow-queries.log';
                    ```
                    
                    Note: These settings require SUPER privileges and will reset after MySQL restart.
                    For permanent configuration, add to my.cnf:
                    
                    ```
                    slow_query_log = 1
                    long_query_time = 1
                    slow_query_log_file = /var/lib/mysql/slow-queries.log
                    ```
                    
                    Alternatively, we can analyze the performance_schema tables.
                """
            
            # Check if performance_schema is enabled
            perf_schema_query = "SHOW VARIABLES LIKE 'performance_schema'"
            perf_schema = connector.execute_query(perf_schema_query)
            
            if not perf_schema or perf_schema[0]['Value'].lower() != 'on':
                return """
                    Both slow query log and performance_schema are not enabled.
                    
                    To enable performance_schema, add to my.cnf and restart MySQL:
                    
                    ```
                    performance_schema = ON
                    ```
                    
                    For immediate analysis, enable the slow query log:
                    
                    ```sql
                    SET GLOBAL slow_query_log = 'ON';
                    SET GLOBAL long_query_time = 1;
                    ```
                """
            
            # Query to find slow queries from performance_schema
            query = """
                SELECT 
                    DIGEST_TEXT as query,
                    COUNT_STAR as calls,
                    AVG_TIMER_WAIT/1000000000 as avg_exec_time_ms,
                    SUM_TIMER_WAIT/1000000000 as total_time_ms,
                    SUM_ROWS_SENT/COUNT_STAR as avg_rows,
                    MAX_TIMER_WAIT/1000000000 as max_time_ms,
                    MIN_TIMER_WAIT/1000000000 as min_time_ms,
                    SUM_ROWS_EXAMINED/COUNT_STAR as avg_rows_examined,
                    SUM_CREATED_TMP_TABLES as tmp_tables,
                    SUM_NO_INDEX_USED as no_index_used
                FROM 
                    performance_schema.events_statements_summary_by_digest
                WHERE 
                    AVG_TIMER_WAIT/1000000000 >= %s
                ORDER BY 
                    avg_exec_time_ms DESC
                LIMIT %s
            """
            
            results = connector.execute_query(query, [min_execution_time, limit])
            
            if not results:
                return f"No queries found with execution time >= {min_execution_time}ms."
            
            # Format results as markdown
            response = f"## Slow Queries (Execution Time >= {min_execution_time}ms)\n\n"
            
            # Extract patterns and prepare data for model analysis
            query_patterns = {}
            total_execution_time = 0
            max_single_time = 0
            total_calls = 0
            
            for i, query_stat in enumerate(results, 1):
                response += f"### Query {i}\n"
                response += f"- **Average Execution Time**: {query_stat['avg_exec_time_ms']:.2f}ms\n"
                response += f"- **Total Execution Time**: {query_stat['total_time_ms']:.2f}ms\n"
                response += f"- **Calls**: {query_stat['calls']}\n"
                response += f"- **Average Rows Returned**: {query_stat['avg_rows']}\n"
                response += f"- **Average Rows Examined**: {query_stat['avg_rows_examined']}\n"
                response += f"- **Max Execution Time**: {query_stat['max_time_ms']:.2f}ms\n"
                response += f"- **Min Execution Time**: {query_stat['min_time_ms']:.2f}ms\n"
                response += f"- **Temporary Tables Created**: {query_stat['tmp_tables']}\n"
                response += f"- **No Index Used Count**: {query_stat['no_index_used']}\n"
                response += f"- **SQL**: ```sql\n{query_stat['query']}\n```\n\n"
                
                # Analyze query complexity
                complexity = connector.analyze_query_complexity(query_stat['query'])
                response += "#### Complexity Analysis\n"
                response += f"- **Complexity Score**: {complexity['complexity_score']}\n"
                response += f"- **Join Count**: {complexity['join_count']}\n"
                response += f"- **Subquery Count**: {complexity['subquery_count']}\n"
                response += f"- **Aggregation Count**: {complexity['aggregation_count']}\n"
                
                if complexity['warnings']:
                    response += "- **Warnings**:\n"
                    for warning in complexity['warnings']:
                        response += f"  - {warning}\n"
                response += "\n"
                
                # Collect data for pattern analysis
                total_execution_time += query_stat['total_time_ms']
                total_calls += query_stat['calls']
                max_single_time = max(max_single_time, query_stat['max_time_ms'])
                
                # Categorize query by type (SELECT, INSERT, UPDATE, etc.)
                query_type = query_stat['query'].strip().upper().split(' ')[0]
                if query_type not in query_patterns:
                    query_patterns[query_type] = 0
                query_patterns[query_type] += 1
            
            # Add summary section for model to provide insights
            response += "## Summary Analysis\n\n"
            response += f"- **Total Queries Analyzed**: {len(results)}\n"
            response += f"- **Total Execution Time**: {total_execution_time:.2f}ms\n"
            response += f"- **Total Query Calls**: {total_calls}\n"
            response += f"- **Query Type Distribution**: {', '.join([f'{k}: {v}' for k, v in query_patterns.items()])}\n\n"
            
            # The model will use this data to provide insights in the response
            response += "### Key Observations\n\n"
            # This section will be filled by the model based on the data provided
            
            return response
        except Exception as e:
            return f"Error retrieving slow queries: {str(e)}"
        finally:
            # Always disconnect when done
            connector.disconnect()
    
    @mcp.tool()
    async def analyze_query(query: str, secret_name: str = None, region_name: str = "us-west-2", ctx: Context = None) -> str:
        """
        Analyze a SQL query and provide optimization recommendations.
        
        Args:
            query: The SQL query to analyze
            secret_name: AWS Secrets Manager secret name containing database credentials (required)
            region_name: AWS region where the secret is stored (default: us-west-2)
        
        Returns:
            Analysis of the query execution plan and optimization suggestions
        """
        # Check if secret_name is provided
        if not secret_name:
            return "Error: Please provide a valid AWS Secrets Manager secret name containing database credentials."
        
        # Initialize connector with the provided secret name
        connector = MySQLConnector(
            secret_name=secret_name,
            region_name=region_name
        )
        
        try:
            if not connector.connect():
                return f"Failed to connect to database using secret '{secret_name}'. Please check your credentials."
            
            # Clean the query before analysis
            query = query.strip()
            
            # Get the execution plan
            explain_query = f"EXPLAIN FORMAT=JSON {query}"
            explain_results = connector.execute_query(explain_query)
            
            if not explain_results or not explain_results[0]:
                return "Failed to generate execution plan for the query. The EXPLAIN command returned no results."
            
            # Extract the plan JSON
            plan_json = None
            if 'EXPLAIN' in explain_results[0]:
                plan_json = json.loads(explain_results[0]['EXPLAIN'])
            else:
                return f"Error: Could not find query plan in EXPLAIN results: {explain_results[0]}"
            
            # Get database structure information for tables involved in the query
            tables_involved = extract_tables_from_query(query)
            if not tables_involved:
                return "Could not identify any tables in the query. Please check the query syntax."
                
            table_stats = get_table_statistics(connector, tables_involved)
            schema_info = get_schema_information(connector, tables_involved)
            index_info = get_index_information(connector, tables_involved)
            
            # Detect query patterns and anti-patterns
            patterns = detect_query_patterns(plan_json)
            anti_patterns = detect_query_anti_patterns(query)
            
            # Analyze query complexity
            complexity = connector.analyze_query_complexity(query)
            
            # Format the response
            response = format_query_analysis_response(
                query=query,
                plan_json=plan_json,
                tables_involved=tables_involved,
                table_stats=table_stats,
                schema_info=schema_info,
                index_info=index_info,
                patterns=patterns,
                anti_patterns=anti_patterns,
                complexity=complexity
            )
            
            return response
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            return f"Error analyzing query: {str(e)}\n\nDetails:\n{error_details}"
        finally:
            # Always disconnect when done
            connector.disconnect()
    
    @mcp.tool()
    async def recommend_indexes(query: str, secret_name: str = None, region_name: str = "us-west-2", ctx: Context = None) -> str:
        """
        Recommend indexes for a given SQL query.
        
        Args:
            query: The SQL query to analyze for index recommendations
            secret_name: AWS Secrets Manager secret name containing database credentials (required)
            region_name: AWS region where the secret is stored (default: us-west-2)
        
        Returns:
            Recommended indexes to improve query performance
        """
        # Check if secret_name is provided
        if not secret_name:
            return "Error: Please provide a valid AWS Secrets Manager secret name containing database credentials."
        
        # Initialize connector with the provided secret name
        connector = MySQLConnector(
            secret_name=secret_name,
            region_name=region_name
        )
        
        try:
            if not connector.connect():
                return f"Failed to connect to database using secret '{secret_name}'. Please check your credentials."
            
            # First, analyze the database structure to understand the context
            tables_involved = extract_tables_from_query(query)
            if not tables_involved:
                return "Could not identify any tables in the query. Please check the query syntax."
                
            # Get database structure for the tables involved
            db_structure = get_table_structure_for_index(connector, tables_involved)
            
            # Use MySQL's EXPLAIN to analyze the query
            explain_query = f"EXPLAIN FORMAT=JSON {query}"
            explain_results = connector.execute_query(explain_query)
            
            if not explain_results or not explain_results[0]:
                return "Failed to generate execution plan for the query."
            
            plan_json = None
            if 'EXPLAIN' in explain_results[0]:
                plan_json = json.loads(explain_results[0]['EXPLAIN'])
            else:
                return "Failed to extract query plan from EXPLAIN results."
            
            # Extract potential index candidates using basic parsing
            potential_indexes = extract_potential_indexes(query)
            
            # Check which potential indexes already exist
            existing_indexes, missing_indexes = check_existing_indexes(potential_indexes, db_structure)
            
            # Format the response
            response = format_index_recommendations_response(
                query=query,
                plan_json=plan_json,
                db_structure=db_structure,
                existing_indexes=existing_indexes,
                missing_indexes=missing_indexes
            )
            
            return response
        except Exception as e:
            return f"Error generating index recommendations: {str(e)}"
        finally:
            # Always disconnect when done
            connector.disconnect()
    
    @mcp.tool()
    async def suggest_query_rewrite(query: str, secret_name: str = None, region_name: str = "us-west-2", ctx: Context = None) -> str:
        """
        Suggest optimized rewrites for a SQL query.
        
        Args:
            query: The SQL query to optimize
            secret_name: AWS Secrets Manager secret name containing database credentials (required)
            region_name: AWS region where the secret is stored (default: us-west-2)
        
        Returns:
            Suggestions for query rewrites to improve performance
        """
        # Check if secret_name is provided
        if not secret_name:
            return "Error: Please provide a valid AWS Secrets Manager secret name containing database credentials."
        
        # Initialize connector with the provided secret name
        connector = MySQLConnector(
            secret_name=secret_name,
            region_name=region_name
        )
        
        try:
            if not connector.connect():
                return f"Failed to connect to database using secret '{secret_name}'. Please check your credentials."
            
            # Get the execution plan
            explain_query = f"EXPLAIN FORMAT=JSON {query}"
            explain_results = connector.execute_query(explain_query)
            
            if not explain_results or not explain_results[0]:
                return "Failed to generate execution plan for the query."
            
            plan_json = None
            if 'EXPLAIN' in explain_results[0]:
                plan_json = json.loads(explain_results[0]['EXPLAIN'])
            else:
                return "Failed to extract query plan from EXPLAIN results."
            
            # Get schema information for tables in the query
            tables_involved = extract_tables_from_query(query)
            schema_info = get_schema_information(connector, tables_involved)
            
            # Get table statistics
            table_stats = get_table_statistics(connector, tables_involved)
            
            # Get index information
            index_info = get_index_information(connector, tables_involved)
            
            # Analyze the query for common anti-patterns
            anti_patterns = detect_query_anti_patterns(query)
            
            # Analyze query complexity
            complexity = connector.analyze_query_complexity(query)
            
            # Format the response
            response = "## Query Rewrite Suggestions\n\n"
            
            # Add query complexity analysis
            response += "### Query Complexity Analysis\n"
            response += f"- **Complexity Score**: {complexity['complexity_score']}\n"
            response += f"- **Join Count**: {complexity['join_count']}\n"
            response += f"- **Subquery Count**: {complexity['subquery_count']}\n"
            response += f"- **Aggregation Count**: {complexity['aggregation_count']}\n"
            
            if complexity['warnings']:
                response += "- **Warnings**:\n"
                for warning in complexity['warnings']:
                    response += f"  - {warning}\n"
            response += "\n"
            
            # Add database context
            response += "### Database Context\n\n"
            for table in tables_involved:
                table_info = next((t for t in table_stats if t.get('table_name') == table), None)
                if table_info:
                    response += f"**Table**: `{table}`\n"
                    response += f"- **Rows**: {table_info.get('table_rows', 'Unknown')}\n"
                    response += f"- **Data Size**: {format_bytes(table_info.get('data_length', 0))}\n"
                    response += f"- **Index Size**: {format_bytes(table_info.get('index_length', 0))}\n\n"
            
            # Add schema information
            response += "### Schema Information\n\n"
            for table in tables_involved:
                table_columns = [col for col in schema_info if col.get('table_name') == table]
                if table_columns:
                    response += f"**Table**: `{table}`\n"
                    for col in table_columns:
                        nullable = "NULL" if col.get('is_nullable') == 'YES' else "NOT NULL"
                        response += f"- `{col.get('column_name')}` ({col.get('column_type')}, {nullable})\n"
                    response += "\n"
            
            # Add index information
            response += "### Index Information\n\n"
            for table in tables_involved:
                table_indexes = [idx for idx in index_info if idx.get('table_name') == table]
                if table_indexes:
                    response += f"**Table**: `{table}`\n"
                    for idx in table_indexes:
                        unique = "Unique" if idx.get('non_unique') == 0 else "Non-Unique"
                        response += f"- `{idx.get('index_name')}`: {idx.get('columns')} ({idx.get('index_type')}, {unique})\n"
                    response += "\n"
                else:
                    response += f"**Table**: `{table}` - No indexes found\n\n"
            
            # Add anti-pattern analysis
            if anti_patterns:
                response += "### Detected Anti-Patterns\n\n"
                for i, issue in enumerate(anti_patterns, 1):
                    response += f"#### Issue {i}: {issue['issue']}\n"
                    response += f"{issue['description']}\n\n"
                    response += f"**Suggestion**: {issue['suggestion']}\n"
                    if "example" in issue and issue["example"]:
                        response += f"**Example**: ```sql\n{issue['example']}\n```\n\n"
            else:
                response += "### Detected Anti-Patterns\n\n"
                response += "No obvious anti-patterns detected in the query.\n\n"
            
            # Add execution plan summary
            response += "### Execution Plan Summary\n\n"
            try:
                if "query_block" in plan_json:
                    query_block = plan_json["query_block"]
                    
                    # Check for table scans
                    table_scans = []
                    if "table" in query_block:
                        tables = query_block["table"]
                        if isinstance(tables, dict):
                            tables = [tables]
                        
                        for table in tables:
                            access_type = table.get("access_type", "")
                            if access_type == "ALL":
                                table_scans.append(table.get("table_name", "Unknown"))
                    
                    if table_scans:
                        response += "- **Full Table Scans**: " + ", ".join([f"`{t}`" for t in table_scans]) + "\n"
                    
                    # Check for temporary tables
                    if "temporary_table" in query_block:
                        response += "- **Uses Temporary Table**: Yes\n"
                    
                    # Check for filesorts
                    if "ordering_operation" in query_block:
                        response += "- **Uses Filesort**: Yes\n"
            except Exception as e:
                response += f"Error parsing execution plan: {str(e)}\n"
            
            response += "\n"
            
            # The model will use the provided data to generate query rewrite suggestions
            response += "## Recommended Query Rewrites\n\n"
            # This section will be filled by the model based on the data provided
            
            return response
        except Exception as e:
            return f"Error generating query rewrite suggestions: {str(e)}"
        finally:
            # Always disconnect when done
            connector.disconnect()
    
    @mcp.tool()
    async def analyze_innodb_buffer_pool(secret_name: str = None, region_name: str = "us-west-2", ctx: Context = None) -> str:
        """
        Analyze InnoDB buffer pool usage and provide optimization recommendations.
        
        Args:
            secret_name: AWS Secrets Manager secret name containing database credentials (required)
            region_name: AWS region where the secret is stored (default: us-west-2)
        
        Returns:
            Analysis of InnoDB buffer pool usage with recommendations
        """
        # Check if secret_name is provided
        if not secret_name:
            return "Error: Please provide a valid AWS Secrets Manager secret name containing database credentials."
        
        # Initialize connector with the provided secret name
        connector = MySQLConnector(
            secret_name=secret_name,
            region_name=region_name
        )
        
        try:
            if not connector.connect():
                return f"Failed to connect to database using secret '{secret_name}'. Please check your credentials."
            
            # Get buffer pool size and configuration
            buffer_config_query = """
                SHOW VARIABLES WHERE Variable_name IN (
                    'innodb_buffer_pool_size',
                    'innodb_buffer_pool_instances',
                    'innodb_buffer_pool_chunk_size',
                    'innodb_page_size'
                )
            """
            buffer_config = connector.execute_query(buffer_config_query)
            
            # Get buffer pool status
            buffer_status_query = """
                SHOW STATUS WHERE Variable_name LIKE 'Innodb_buffer_pool%'
            """
            buffer_status = connector.execute_query(buffer_status_query)
            
            # Get server memory information
            memory_query = """
                SHOW VARIABLES WHERE Variable_name IN (
                    'key_buffer_size',
                    'query_cache_size',
                    'max_connections',
                    'max_heap_table_size',
                    'tmp_table_size'
                )
            """
            memory_info = connector.execute_query(memory_query)
            
            # Get buffer pool content by table
            buffer_content_query = """
                SELECT 
                    table_name,
                    index_name,
                    count(*) as page_count,
                    sum(data_size)/1024/1024 as data_size_mb
                FROM 
                    information_schema.innodb_buffer_page
                JOIN 
                    information_schema.innodb_buffer_page_lru USING (pool_id, block_id)
                WHERE 
                    table_name IS NOT NULL AND table_name != ''
                GROUP BY 
                    table_name, index_name
                ORDER BY 
                    page_count DESC
                LIMIT 20
            """
            
            try:
                buffer_content = connector.execute_query(buffer_content_query)
            except Exception:
                buffer_content = []
                # This query might fail on some MySQL versions or configurations
            
            # Format the response
            response = "# InnoDB Buffer Pool Analysis\n\n"
            
            # Buffer pool configuration
            response += "## Buffer Pool Configuration\n\n"
            response += "| Parameter | Value | Size |\n"
            response += "| --------- | ----- | ---- |\n"
            
            buffer_pool_size = 0
            page_size = 16384  # Default
            
            for param in buffer_config:
                name = param["Variable_name"]
                value = param["Value"]
                
                if name == "innodb_buffer_pool_size":
                    buffer_pool_size = int(value)
                    size_str = format_bytes(buffer_pool_size)
                elif name == "innodb_page_size":
                    page_size = int(value)
                    size_str = format_bytes(int(value))
                else:
                    size_str = value
                
                response += f"| {name} | {value} | {size_str} |\n"
            
            response += "\n"
            
            # Buffer pool status
            response += "## Buffer Pool Status\n\n"
            
            # Extract key metrics
            pages_total = 0
            pages_free = 0
            pages_data = 0
            read_requests = 0
            reads = 0
            
            for status in buffer_status:
                if status["Variable_name"] == "Innodb_buffer_pool_pages_total":
                    pages_total = int(status["Value"])
                elif status["Variable_name"] == "Innodb_buffer_pool_pages_free":
                    pages_free = int(status["Value"])
                elif status["Variable_name"] == "Innodb_buffer_pool_pages_data":
                    pages_data = int(status["Value"])
                elif status["Variable_name"] == "Innodb_buffer_pool_read_requests":
                    read_requests = int(status["Value"])
                elif status["Variable_name"] == "Innodb_buffer_pool_reads":
                    reads = int(status["Value"])
            
            # Calculate derived metrics
            buffer_pool_used_pct = ((pages_total - pages_free) / pages_total) * 100 if pages_total > 0 else 0
            hit_ratio = ((read_requests - reads) / read_requests) * 100 if read_requests > 0 else 0
            
            response += f"- **Buffer Pool Size**: {format_bytes(buffer_pool_size)}\n"
            response += f"- **Total Pages**: {pages_total:,}\n"
            response += f"- **Free Pages**: {pages_free:,}\n"
            response += f"- **Data Pages**: {pages_data:,}\n"
            response += f"- **Buffer Pool Used**: {buffer_pool_used_pct:.2f}%\n"
            response += f"- **Read Requests**: {read_requests:,}\n"
            response += f"- **Physical Reads**: {reads:,}\n"
            response += f"- **Hit Ratio**: {hit_ratio:.2f}%\n\n"
            
            # Buffer pool content
            if buffer_content:
                response += "## Top Tables in Buffer Pool\n\n"
                response += "| Table | Index | Pages | Data Size |\n"
                response += "| ----- | ----- | ----- | --------- |\n"
                
                for item in buffer_content:
                    table = item["table_name"]
                    index = item["index_name"] or "PRIMARY"
                    pages = item["page_count"]
                    size = f"{item['data_size_mb']:.2f} MB"
                    
                    response += f"| {table} | {index} | {pages:,} | {size} |\n"
                
                response += "\n"
            
            # Recommendations
            response += "## Recommendations\n\n"
            
            # Buffer pool size recommendations
            if buffer_pool_used_pct > 95:
                response += "### Buffer Pool Size\n\n"
                response += "The buffer pool is nearly full (>95% used). Consider increasing the buffer pool size if server has available memory.\n\n"
                response += "```sql\n"
                response += f"SET GLOBAL innodb_buffer_pool_size = {buffer_pool_size * 2};\n"
                response += "```\n\n"
                response += "For permanent changes, update your my.cnf file:\n\n"
                response += "```\n"
                response += f"innodb_buffer_pool_size = {format_bytes(buffer_pool_size * 2)}\n"
                response += "```\n\n"
            elif buffer_pool_used_pct < 50:
                response += "### Buffer Pool Size\n\n"
                response += "The buffer pool is less than 50% used. You might be able to reduce the buffer pool size to free memory for other purposes.\n\n"
                response += "```sql\n"
                response += f"SET GLOBAL innodb_buffer_pool_size = {buffer_pool_size // 2};\n"
                response += "```\n\n"
                response += "For permanent changes, update your my.cnf file:\n\n"
                response += "```\n"
                response += f"innodb_buffer_pool_size = {format_bytes(buffer_pool_size // 2)}\n"
                response += "```\n\n"
            
            # Hit ratio recommendations
            if hit_ratio < 95:
                response += "### Hit Ratio\n\n"
                response += f"The buffer pool hit ratio is {hit_ratio:.2f}%, which is below the recommended 95%. This indicates that MySQL is reading from disk more often than optimal.\n\n"
                response += "Consider:\n"
                response += "1. Increasing the buffer pool size if memory is available\n"
                response += "2. Optimizing queries to reduce the working set size\n"
                response += "3. Adding appropriate indexes to reduce full table scans\n\n"
            
            # The model will use the provided data to generate additional recommendations
            response += "### Additional Recommendations\n\n"
            # This section will be filled by the model based on the data provided
            
            return response
        except Exception as e:
            return f"Error analyzing InnoDB buffer pool: {str(e)}"
        finally:
            # Always disconnect when done
            connector.disconnect()
    
    @mcp.tool()
    async def analyze_table_fragmentation(secret_name: str = None, region_name: str = "us-west-2", ctx: Context = None) -> str:
        """
        Analyze table fragmentation and provide optimization recommendations.
        
        Args:
            secret_name: AWS Secrets Manager secret name containing database credentials (required)
            region_name: AWS region where the secret is stored (default: us-west-2)
        
        Returns:
            Analysis of table fragmentation with optimization recommendations
        """
        # Check if secret_name is provided
        if not secret_name:
            return "Error: Please provide a valid AWS Secrets Manager secret name containing database credentials."
        
        # Initialize connector with the provided secret name
        connector = MySQLConnector(
            secret_name=secret_name,
            region_name=region_name
        )
        
        try:
            if not connector.connect():
                return f"Failed to connect to database using secret '{secret_name}'. Please check your credentials."
            
            # Get table information
            tables_query = """
                SELECT 
                    table_name,
                    engine,
                    table_rows,
                    data_length,
                    index_length,
                    data_free,
                    create_time,
                    update_time
                FROM 
                    information_schema.tables
                WHERE 
                    table_schema = DATABASE()
                    AND engine = 'InnoDB'
                ORDER BY 
                    data_length DESC
            """
            tables = connector.execute_query(tables_query)
            
            # Format the response
            response = "# Table Fragmentation Analysis\n\n"
            
            if not tables:
                return response + "No InnoDB tables found in the current database."
            
            # Table fragmentation overview
            response += "## Table Fragmentation Overview\n\n"
            response += "| Table | Rows | Data Size | Index Size | Free Space | Fragmentation % |\n"
            response += "| ----- | ---- | --------- | ---------- | ---------- | -------------- |\n"
            
            fragmented_tables = []
            
            for table in tables:
                table_name = table["table_name"]
                rows = table["table_rows"] or 0
                data_length = table["data_length"] or 0
                index_length = table["index_length"] or 0
                data_free = table["data_free"] or 0
                
                # Calculate fragmentation percentage
                total_size = data_length + index_length
                fragmentation_pct = (data_free / total_size) * 100 if total_size > 0 else 0
                
                # Format sizes
                data_size = format_bytes(data_length)
                index_size = format_bytes(index_length)
                free_space = format_bytes(data_free)
                
                response += f"| {table_name} | {rows:,} | {data_size} | {index_size} | {free_space} | {fragmentation_pct:.2f}% |\n"
                
                # Track tables with significant fragmentation
                if fragmentation_pct > 10 and data_length > 10 * 1024 * 1024:  # >10% fragmentation and >10MB
                    fragmented_tables.append({
                        "name": table_name,
                        "fragmentation": fragmentation_pct,
                        "size": data_length
                    })
            
            response += "\n"
            
            # Recommendations for fragmented tables
            if fragmented_tables:
                response += "## Optimization Recommendations\n\n"
                response += "The following tables have significant fragmentation and could benefit from optimization:\n\n"
                
                for table in fragmented_tables:
                    response += f"### {table['name']}\n\n"
                    response += f"- **Fragmentation**: {table['fragmentation']:.2f}%\n"
                    response += f"- **Size**: {format_bytes(table['size'])}\n"
                    response += "- **Recommendation**: Run OPTIMIZE TABLE to defragment and reclaim space\n\n"
                    response += "```sql\n"
                    response += f"OPTIMIZE TABLE {table['name']};\n"
                    response += "```\n\n"
                    response += "Note: OPTIMIZE TABLE locks the table during operation. Consider running during off-peak hours.\n\n"
            else:
                response += "## Optimization Recommendations\n\n"
                response += "No tables with significant fragmentation were detected. Your database appears to be well-optimized in terms of storage.\n\n"
            
            # General recommendations
            response += "## General Recommendations\n\n"
            response += "1. **Regular Maintenance**: Schedule regular OPTIMIZE TABLE operations for large tables during off-peak hours.\n\n"
            response += "2. **Monitor Growth**: Keep an eye on tables that grow rapidly, as they may fragment more quickly.\n\n"
            response += "3. **Consider Partitioning**: For very large tables, consider partitioning to make maintenance operations more manageable.\n\n"
            response += "4. **Adjust innodb_file_per_table**: Ensure this is set to ON (default in modern MySQL) for better space management.\n\n"
            
            # The model will use the provided data to generate additional recommendations
            response += "## Additional Insights\n\n"
            # This section will be filled by the model based on the data provided
            
            return response
        except Exception as e:
            return f"Error analyzing table fragmentation: {str(e)}"
        finally:
            # Always disconnect when done
            connector.disconnect()
    
    @mcp.tool()
    async def show_mysql_settings(pattern: str = None, secret_name: str = None, region_name: str = "us-west-2", ctx: Context = None) -> str:
        """
        Show MySQL configuration settings with optional filtering.
        
        Args:
            pattern: Optional pattern to filter settings (e.g., "innodb" for all InnoDB-related settings)
            secret_name: AWS Secrets Manager secret name containing database credentials (required)
            region_name: AWS region where the secret is stored (default: us-west-2)
        
        Returns:
            Current MySQL configuration settings in a formatted table
        
        Examples:
            show_mysql_settings(secret_name="my-db-secret")
            show_mysql_settings(pattern="innodb", secret_name="my-db-secret")
            show_mysql_settings(pattern="buffer", secret_name="my-db-secret")
        """
        # Check if secret_name is provided
        if not secret_name:
            return "Error: Please provide a valid AWS Secrets Manager secret name containing database credentials."
        
        # Initialize connector with the provided secret name
        connector = MySQLConnector(
            secret_name=secret_name,
            region_name=region_name
        )
        
        try:
            if not connector.connect():
                return f"Failed to connect to database using secret '{secret_name}'. Please check your credentials."
            
            # Build the query based on whether a pattern is provided
            if pattern:
                query = f"SHOW VARIABLES WHERE Variable_name LIKE '%{pattern}%'"
            else:
                query = "SHOW VARIABLES"
            
            results = connector.execute_query(query)
            
            if not results:
                if pattern:
                    return f"No settings found matching pattern '{pattern}'."
                else:
                    return "No settings found."
            
            # Group settings by prefix for better organization
            settings_by_prefix = {}
            for setting in results:
                name = setting['Variable_name']
                prefix = name.split('_')[0] if '_' in name else 'other'
                
                if prefix not in settings_by_prefix:
                    settings_by_prefix[prefix] = []
                settings_by_prefix[prefix].append(setting)
            
            # Format the response
            response = "# MySQL Configuration Settings\n\n"
            
            if pattern:
                response += f"Showing settings matching pattern: '{pattern}'\n\n"
            
            for prefix, settings in sorted(settings_by_prefix.items()):
                response += f"## {prefix.upper()}\n\n"
                response += "| Name | Value |\n"
                response += "| ---- | ----- |\n"
                
                for setting in sorted(settings, key=lambda x: x['Variable_name']):
                    name = setting['Variable_name']
                    value = setting['Value']
                    
                    # Format byte values for better readability
                    if any(size_param in name.lower() for size_param in ['size', 'buffer', 'cache', 'length']):
                        try:
                            if value.isdigit() and int(value) > 1024:
                                value = f"{value} ({format_bytes(int(value))})"
                        except:
                            pass
                    
                    response += f"| {name} | {value} |\n"
                
                response += "\n"
            
            response += f"\n{len(results)} setting(s) displayed."
            
            return response
        except Exception as e:
            return f"Error retrieving MySQL settings: {str(e)}"
        finally:
            # Always disconnect when done
            connector.disconnect()
    
    @mcp.tool()
    async def execute_read_only_query(query: str, secret_name: str = None, region_name: str = "us-west-2", 
                                     max_rows: int = 100, ctx: Context = None) -> str:
        """
        Execute a read-only SQL query and return the results.
        
        Args:
            query: The SQL query to execute (must be SELECT, EXPLAIN, or SHOW only)
            secret_name: AWS Secrets Manager secret name containing database credentials (required)
            region_name: AWS region where the secret is stored (default: us-west-2)
            max_rows: Maximum number of rows to return (default: 100)
        
        Returns:
            Query results in a formatted table
        
        Examples:
            execute_read_only_query("SELECT * FROM information_schema.processlist LIMIT 10", secret_name="my-db-secret")
            execute_read_only_query("EXPLAIN SELECT * FROM users WHERE user_id = 123", secret_name="my-db-secret")
            execute_read_only_query("SHOW VARIABLES LIKE 'innodb%'", secret_name="my-db-secret")
            execute_read_only_query("SHOW STATUS", secret_name="my-db-secret")
        """
        # Check if secret_name is provided
        if not secret_name:
            return "Error: Please provide a valid AWS Secrets Manager secret name containing database credentials."
        
        # Validate that this is a read-only query
        is_valid, error_message = validate_read_only_query(query)
        if not is_valid:
            return f"Error: {error_message}"
        
        # Initialize connector with the provided secret name
        connector = MySQLConnector(
            secret_name=secret_name,
            region_name=region_name
        )
        
        try:
            if not connector.connect():
                return f"Failed to connect to database using secret '{secret_name}'. Please check your credentials."
            
            # Set session to read-only mode
            connector.execute_query("SET SESSION TRANSACTION READ ONLY")
            connector.execute_query("SET SESSION MAX_EXECUTION_TIME=30000")  # 30-second timeout for safety
            
            # Execute the query
            start_time = time.time()
            results = connector.execute_query(query)
            execution_time = time.time() - start_time
            
            if not results:
                return f"Query executed successfully in {execution_time:.2f} seconds, but returned no results."
            
            # Limit the number of rows returned
            if len(results) > max_rows:
                truncated = True
                results = results[:max_rows]
            else:
                truncated = False
            
            # Format the results as a markdown table
            response = f"## Query Results\n\n"
            response += f"Executed in {execution_time:.2f} seconds\n\n"
            
            if truncated:
                response += f"*Results truncated to {max_rows} rows*\n\n"
            
            # Get column names from the first row
            columns = list(results[0].keys())
            
            # Create the header row
            response += "| " + " | ".join(columns) + " |\n"
            response += "| " + " | ".join(["---" for _ in columns]) + " |\n"
            
            # Add data rows
            for row in results:
                # Convert each value to string and handle None values
                row_values = []
                for col in columns:
                    val = row.get(col)
                    if val is None:
                        row_values.append("NULL")
                    else:
                        # Escape pipe characters in the data to prevent breaking the markdown table
                        row_values.append(str(val).replace("|", "\\|"))
                
                response += "| " + " | ".join(row_values) + " |\n"
            
            response += f"\n{len(results)} rows returned" + (" (truncated)" if truncated else "")
            
            return response
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            return f"Error executing query: {str(e)}\n\nDetails:\n{error_details}"
        finally:
            # Always disconnect when done
            connector.disconnect()
    
    @mcp.tool()
    async def health_check(ctx: Context = None) -> str:
        """
        Check if the server is running and responsive.
        
        Returns:
            A message indicating the server is healthy
        """
        return "MySQL Performance Analyzer MCP server is running and healthy!"

def format_bytes(bytes_value):
    """Format bytes to human-readable format"""
    if bytes_value is None:
        return "Unknown"
    
    bytes_value = float(bytes_value)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024
    
    return f"{bytes_value:.2f} PB"