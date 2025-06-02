"""
Functions for analyzing MySQL queries
"""
import re
import json
from typing import Dict, List, Any, Optional
from db.connector import MySQLConnector

def extract_tables_from_query(query: str) -> List[str]:
    """
    Extract table names from a SQL query
    
    Args:
        query: SQL query
        
    Returns:
        List of table names
    """
    # Normalize query
    query = re.sub(r'/\*.*?\*/', ' ', query, flags=re.DOTALL)  # Remove comments
    query = re.sub(r'--.*?(\n|\$)', ' ', query)  # Remove single line comments
    query = re.sub(r'\s+', ' ', query)  # Normalize whitespace
    query = query.lower()  # Convert to lowercase
    
    # Find tables in FROM and JOIN clauses
    tables = []
    
    # Match FROM clause
    from_matches = re.finditer(r'from\s+([a-z0-9_\.]+)(?:\s+as\s+[a-z0-9_]+)?', query)
    for match in from_matches:
        table = match.group(1)
        if '.' in table:
            table = table.split('.')[-1]  # Remove schema prefix
        tables.append(table)
    
    # Match JOIN clauses
    join_matches = re.finditer(r'join\s+([a-z0-9_\.]+)(?:\s+as\s+[a-z0-9_]+)?', query)
    for match in join_matches:
        table = match.group(1)
        if '.' in table:
            table = table.split('.')[-1]  # Remove schema prefix
        tables.append(table)
    
    # Remove duplicates and return
    return list(set(tables))

def get_table_statistics(connector: MySQLConnector, tables: List[str]) -> List[Dict[str, Any]]:
    """
    Get statistics for the specified tables
    
    Args:
        connector: MySQLConnector instance
        tables: List of table names
        
    Returns:
        List of table statistics
    """
    if not tables:
        return []
    
    # Format table names for IN clause
    table_names = ', '.join([f"'{table}'" for table in tables])
    
    # Query for table statistics
    query = f"""
        SELECT 
            table_name,
            table_rows,
            avg_row_length,
            data_length,
            index_length,
            auto_increment
        FROM 
            information_schema.tables
        WHERE 
            table_schema = DATABASE()
            AND table_name IN ({table_names})
    """
    
    table_stats = connector.execute_query(query)
    
    # Get additional statistics from SHOW TABLE STATUS
    for table in tables:
        status_query = f"SHOW TABLE STATUS LIKE '{table}'"
        status_result = connector.execute_query(status_query)
        
        if status_result:
            # Find matching table in our results
            for i, stat in enumerate(table_stats):
                if stat["table_name"] == table:
                    # Add additional info
                    table_stats[i]["engine"] = status_result[0]["Engine"]
                    table_stats[i]["create_time"] = status_result[0]["Create_time"]
                    table_stats[i]["update_time"] = status_result[0]["Update_time"]
                    table_stats[i]["collation"] = status_result[0]["Collation"]
                    break
    
    return table_stats

def get_schema_information(connector: MySQLConnector, tables: List[str]) -> List[Dict[str, Any]]:
    """
    Get schema information for the specified tables
    
    Args:
        connector: MySQLConnector instance
        tables: List of table names
        
    Returns:
        List of column information
    """
    if not tables:
        return []
    
    # Format table names for IN clause
    table_names = ', '.join([f"'{table}'" for table in tables])
    
    # Query for column information
    query = f"""
        SELECT 
            table_name,
            column_name,
            column_type,
            is_nullable,
            column_key,
            column_default,
            extra
        FROM 
            information_schema.columns
        WHERE 
            table_schema = DATABASE()
            AND table_name IN ({table_names})
        ORDER BY 
            table_name, ordinal_position
    """
    
    return connector.execute_query(query)

def get_index_information(connector: MySQLConnector, tables: List[str]) -> List[Dict[str, Any]]:
    """
    Get index information for the specified tables
    
    Args:
        connector: MySQLConnector instance
        tables: List of table names
        
    Returns:
        List of index information
    """
    if not tables:
        return []
    
    # Format table names for IN clause
    table_names = ', '.join([f"'{table}'" for table in tables])
    
    # Query for index information
    query = f"""
        SELECT 
            table_name,
            index_name,
            GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
            index_type,
            non_unique
        FROM 
            information_schema.statistics
        WHERE 
            table_schema = DATABASE()
            AND table_name IN ({table_names})
        GROUP BY 
            table_name, index_name, index_type, non_unique
        ORDER BY 
            table_name, index_name
    """
    
    return connector.execute_query(query)

def format_query_analysis_response(query: str, plan_json: Dict[str, Any], tables_involved: List[str],
                                  table_stats: List[Dict[str, Any]], schema_info: List[Dict[str, Any]],
                                  index_info: List[Dict[str, Any]], patterns: List[Dict[str, Any]],
                                  anti_patterns: List[Dict[str, Any]], complexity: Dict[str, Any]) -> str:
    """
    Format query analysis as a response
    
    Args:
        query: Original SQL query
        plan_json: Execution plan JSON
        tables_involved: List of tables in the query
        table_stats: Table statistics
        schema_info: Schema information
        index_info: Index information
        patterns: Detected query patterns
        anti_patterns: Detected query anti-patterns
        complexity: Query complexity metrics
        
    Returns:
        Formatted string with analysis
    """
    response = "# MySQL Query Analysis\n\n"
    
    # Original query
    response += "## Original Query\n\n"
    response += f"```sql\n{query}\n```\n\n"
    
    # Query complexity
    response += "## Query Complexity Analysis\n\n"
    response += f"- **Complexity Score**: {complexity['complexity_score']}\n"
    response += f"- **Join Count**: {complexity['join_count']}\n"
    response += f"- **Subquery Count**: {complexity['subquery_count']}\n"
    response += f"- **Aggregation Count**: {complexity['aggregation_count']}\n"
    
    if complexity['warnings']:
        response += "- **Warnings**:\n"
        for warning in complexity['warnings']:
            response += f"  - {warning}\n"
    response += "\n"
    
    # Execution plan
    response += "## Execution Plan\n\n"
    response += "```json\n"
    response += json.dumps(plan_json, indent=2)
    response += "\n```\n\n"
    
    # Execution plan analysis
    response += "### Execution Plan Analysis\n\n"
    
    # Extract key information from the plan
    try:
        plan_type = plan_json.get("query_block", {}).get("select_id", "Unknown")
        response += f"- **Plan Type**: {plan_type}\n"
        
        # Check for table scans
        table_scans = []
        if "query_block" in plan_json and "table" in plan_json["query_block"]:
            tables = plan_json["query_block"]["table"]
            if isinstance(tables, dict):
                tables = [tables]
            
            for table in tables:
                access_type = table.get("access_type", "")
                if access_type == "ALL":
                    table_scans.append(table.get("table_name", "Unknown"))
        
        if table_scans:
            response += "- **Full Table Scans**:\n"
            for table in table_scans:
                response += f"  - `{table}`\n"
        
        # Check for temporary tables
        if "query_block" in plan_json and "temporary_table" in plan_json["query_block"]:
            response += "- **Uses Temporary Table**: Yes\n"
        
        # Check for filesorts
        if "query_block" in plan_json and "ordering_operation" in plan_json["query_block"]:
            response += "- **Uses Filesort**: Yes\n"
    
    except Exception as e:
        response += f"Error parsing execution plan: {str(e)}\n"
    
    response += "\n"
    
    # Tables involved
    response += "## Tables Involved\n\n"
    for table in tables_involved:
        # Find table stats
        table_stat = next((stat for stat in table_stats if stat["table_name"] == table), None)
        
        if table_stat:
            response += f"### {table}\n\n"
            response += f"- **Rows (approx)**: {table_stat.get('table_rows', 'Unknown')}\n"
            response += f"- **Engine**: {table_stat.get('engine', 'Unknown')}\n"
            
            # Add data size if available
            if "data_length" in table_stat and table_stat["data_length"]:
                data_size = format_bytes(table_stat["data_length"])
                response += f"- **Data Size**: {data_size}\n"
            
            # Add index size if available
            if "index_length" in table_stat and table_stat["index_length"]:
                index_size = format_bytes(table_stat["index_length"])
                response += f"- **Index Size**: {index_size}\n"
            
            response += "\n"
            
            # Add columns
            table_columns = [col for col in schema_info if col["table_name"] == table]
            if table_columns:
                response += "#### Columns\n\n"
                response += "| Column | Type | Nullable | Key | Default | Extra |\n"
                response += "| ------ | ---- | -------- | --- | ------- | ----- |\n"
                
                for column in table_columns:
                    nullable = "YES" if column["is_nullable"] == "YES" else "NO"
                    key = column["column_key"] or ""
                    default = column["column_default"] or ""
                    extra = column["extra"] or ""
                    
                    response += f"| {column['column_name']} | {column['column_type']} | {nullable} | {key} | {default} | {extra} |\n"
                
                response += "\n"
            
            # Add indexes
            table_indexes = [idx for idx in index_info if idx["table_name"] == table]
            if table_indexes:
                response += "#### Indexes\n\n"
                response += "| Name | Columns | Type | Unique |\n"
                response += "| ---- | ------- | ---- | ------ |\n"
                
                for index in table_indexes:
                    unique = "No" if index["non_unique"] == 1 else "Yes"
                    response += f"| {index['index_name']} | {index['columns']} | {index['index_type']} | {unique} |\n"
                
                response += "\n"
    
    # Query patterns
    if patterns:
        response += "## Detected Query Patterns\n\n"
        for pattern in patterns:
            response += f"### {pattern['pattern']}\n\n"
            response += f"{pattern['description']}\n\n"
            if "recommendation" in pattern:
                response += f"**Recommendation**: {pattern['recommendation']}\n\n"
    
    # Query anti-patterns
    if anti_patterns:
        response += "## Detected Query Anti-Patterns\n\n"
        for anti_pattern in anti_patterns:
            response += f"### {anti_pattern['issue']}\n\n"
            response += f"{anti_pattern['description']}\n\n"
            if "suggestion" in anti_pattern:
                response += f"**Suggestion**: {anti_pattern['suggestion']}\n\n"
            if "example" in anti_pattern:
                response += f"**Example**:\n```sql\n{anti_pattern['example']}\n```\n\n"
    
    # Optimization recommendations
    response += "## Optimization Recommendations\n\n"
    # This section will be filled by the model based on the data provided
    
    return response

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