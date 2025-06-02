"""
Functions for analyzing and recommending MySQL indexes
"""
import re
from typing import Dict, List, Any, Tuple
from db.connector import MySQLConnector

def extract_potential_indexes(query: str) -> List[Dict[str, Any]]:
    """
    Extract potential index candidates from a query
    
    Args:
        query: SQL query
        
    Returns:
        List of potential indexes
    """
    query_lower = query.lower()
    potential_indexes = []
    
    # Extract tables
    tables = []
    from_clause_match = re.search(r'from\s+([^()]+?)(?:where|group|having|order|limit|\$)', query_lower, re.DOTALL)
    
    if from_clause_match:
        from_clause = from_clause_match.group(1).strip()
        # Handle multiple tables (comma-separated or joined)
        table_parts = re.split(r',|\sjoin\s', from_clause)
        
        for part in table_parts:
            # Extract table name, handling aliases
            table_match = re.search(r'([a-z0-9_\.]+)(?:\s+(?:as\s+)?([a-z0-9_]+))?', part.strip())
            if table_match:
                table_name = table_match.group(1)
                alias = table_match.group(2) if table_match.group(2) else table_name
                
                # Remove schema prefix if present
                if '.' in table_name:
                    table_name = table_name.split('.')[-1]
                
                tables.append({"name": table_name, "alias": alias})
    
    # Extract WHERE conditions
    where_clause_match = re.search(r'where\s+(.+?)(?:group|having|order|limit|\$)', query_lower, re.DOTALL)
    
    if where_clause_match:
        where_clause = where_clause_match.group(1).strip()
        # Split by AND
        conditions = re.split(r'\sand\s', where_clause)
        
        for condition in conditions:
            # Look for equality conditions (column = value)
            eq_match = re.search(r'([a-z0-9_\.]+)\s*=\s*', condition)
            if eq_match:
                column_ref = eq_match.group(1)
                
                # Determine table and column
                if '.' in column_ref:
                    parts = column_ref.split('.')
                    table_alias = parts[0]
                    column_name = parts[1]
                    
                    # Find the actual table name from alias
                    table_name = next((t["name"] for t in tables if t["alias"] == table_alias), None)
                    
                    if table_name:
                        potential_indexes.append({
                            "table": table_name,
                            "columns": [column_name],
                            "reason": "Equality condition in WHERE clause"
                        })
                else:
                    # Column without table reference - try to match to all tables
                    for table in tables:
                        potential_indexes.append({
                            "table": table["name"],
                            "columns": [column_ref],
                            "reason": "Possible equality condition in WHERE clause"
                        })
    
    # Extract JOIN conditions
    join_conditions = re.finditer(r'join\s+([a-z0-9_\.]+)(?:\s+(?:as\s+)?([a-z0-9_]+))?\s+on\s+(.+?)(?:(?:inner|left|right|outer)\s+join|where|group|having|order|limit|\$)', query_lower, re.DOTALL)
    
    for match in join_conditions:
        table_name = match.group(1)
        if '.' in table_name:
            table_name = table_name.split('.')[-1]
        
        join_condition = match.group(3).strip()
        
        # Look for equality join conditions
        eq_match = re.search(r'([a-z0-9_\.]+)\s*=\s*([a-z0-9_\.]+)', join_condition)
        if eq_match:
            left_col = eq_match.group(1)
            right_col = eq_match.group(2)
            
            # Check if either column belongs to this table
            for col in [left_col, right_col]:
                if '.' in col:
                    parts = col.split('.')
                    col_table_alias = parts[0]
                    col_name = parts[1]
                    
                    # Find the actual table name from alias
                    col_table_name = next((t["name"] for t in tables if t["alias"] == col_table_alias), None)
                    
                    if col_table_name == table_name:
                        potential_indexes.append({
                            "table": table_name,
                            "columns": [col_name],
                            "reason": "Join condition"
                        })
    
    # Extract ORDER BY columns
    order_by_match = re.search(r'order\s+by\s+(.+?)(?:limit|\$)', query_lower, re.DOTALL)
    
    if order_by_match:
        order_clause = order_by_match.group(1).strip()
        order_cols = re.split(r',', order_clause)
        
        for col in order_cols:
            col = col.strip().split()[0]  # Remove ASC/DESC if present
            
            if '.' in col:
                parts = col.split('.')
                table_alias = parts[0]
                column_name = parts[1]
                
                # Find the actual table name from alias
                table_name = next((t["name"] for t in tables if t["alias"] == table_alias), None)
                
                if table_name:
                    potential_indexes.append({
                        "table": table_name,
                        "columns": [column_name],
                        "reason": "ORDER BY clause"
                    })
            else:
                # Column without table reference - try to match to all tables
                for table in tables:
                    potential_indexes.append({
                        "table": table["name"],
                        "columns": [col],
                        "reason": "Possible ORDER BY column"
                    })
    
    # Extract GROUP BY columns
    group_by_match = re.search(r'group\s+by\s+(.+?)(?:having|order|limit|\$)', query_lower, re.DOTALL)
    
    if group_by_match:
        group_clause = group_by_match.group(1).strip()
        group_cols = re.split(r',', group_clause)
        
        for col in group_cols:
            col = col.strip()
            
            if '.' in col:
                parts = col.split('.')
                table_alias = parts[0]
                column_name = parts[1]
                
                # Find the actual table name from alias
                table_name = next((t["name"] for t in tables if t["alias"] == table_alias), None)
                
                if table_name:
                    potential_indexes.append({
                        "table": table_name,
                        "columns": [column_name],
                        "reason": "GROUP BY clause"
                    })
            else:
                # Column without table reference - try to match to all tables
                for table in tables:
                    potential_indexes.append({
                        "table": table["name"],
                        "columns": [col],
                        "reason": "Possible GROUP BY column"
                    })
    
    return potential_indexes

def get_table_structure_for_index(connector: MySQLConnector, tables: List[str]) -> Dict[str, Any]:
    """
    Get table structure information for index analysis
    
    Args:
        connector: MySQLConnector instance
        tables: List of table names
        
    Returns:
        Dict with table structure information
    """
    result = {}
    
    for table in tables:
        # Get columns
        columns_query = f"""
            SELECT 
                column_name,
                column_type,
                is_nullable,
                column_key
            FROM 
                information_schema.columns
            WHERE 
                table_schema = DATABASE()
                AND table_name = '{table}'
            ORDER BY 
                ordinal_position
        """
        columns = connector.execute_query(columns_query)
        
        # Get indexes
        indexes_query = f"""
            SELECT 
                index_name,
                GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
                index_type,
                non_unique
            FROM 
                information_schema.statistics
            WHERE 
                table_schema = DATABASE()
                AND table_name = '{table}'
            GROUP BY 
                index_name, index_type, non_unique
        """
        indexes = connector.execute_query(indexes_query)
        
        # Get table stats
        stats_query = f"SHOW TABLE STATUS LIKE '{table}'"
        stats = connector.execute_query(stats_query)
        
        result[table] = {
            "columns": columns,
            "indexes": indexes,
            "stats": stats[0] if stats else {}
        }
    
    return result

def check_existing_indexes(potential_indexes: List[Dict[str, Any]], db_structure: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Check which potential indexes already exist
    
    Args:
        potential_indexes: List of potential indexes
        db_structure: Database structure information
        
    Returns:
        Tuple of (existing_indexes, missing_indexes)
    """
    existing = []
    missing = []
    
    for index in potential_indexes:
        table = index["table"]
        columns = index["columns"]
        
        # Skip if table doesn't exist in our structure
        if table not in db_structure:
            continue
        
        # Check if this index already exists
        found = False
        for existing_index in db_structure[table]["indexes"]:
            existing_columns = existing_index["columns"].split(",")
            
            # Check if all columns in our potential index are covered by this existing index
            # Note: Order matters for MySQL indexes, so we check if our columns are a prefix of the existing index
            if len(columns) <= len(existing_columns):
                if all(columns[i] == existing_columns[i] for i in range(len(columns))):
                    found = True
                    existing.append({
                        "table": table,
                        "columns": columns,
                        "existing_index": existing_index["index_name"],
                        "reason": index["reason"]
                    })
                    break
        
        if not found:
            missing.append(index)
    
    return existing, missing

def format_index_recommendations_response(query: str, plan_json: Dict[str, Any], db_structure: Dict[str, Any],
                                         existing_indexes: List[Dict[str, Any]], missing_indexes: List[Dict[str, Any]]) -> str:
    """
    Format index recommendations as a response
    
    Args:
        query: Original SQL query
        plan_json: Execution plan JSON
        db_structure: Database structure information
        existing_indexes: List of existing indexes that match potential candidates
        missing_indexes: List of missing indexes that could be beneficial
        
    Returns:
        Formatted string with recommendations
    """
    response = "# MySQL Index Recommendations\n\n"
    
    # Original query
    response += "## Original Query\n\n"
    response += f"```sql\n{query}\n```\n\n"
    
    # Execution plan summary
    response += "## Execution Plan Summary\n\n"
    
    try:
        # Extract key information from the plan
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
                response += "### Full Table Scans Detected\n\n"
                response += "The following tables are being scanned without using indexes:\n\n"
                for table in table_scans:
                    response += f"- `{table}`\n"
                response += "\n"
            
            # Check for temporary tables
            if "temporary_table" in query_block:
                response += "### Temporary Table Used\n\n"
                response += "The query creates a temporary table, which might benefit from better indexing.\n\n"
            
            # Check for filesorts
            if "ordering_operation" in query_block:
                response += "### Filesort Used\n\n"
                response += "The query uses a filesort operation, which could be optimized with proper indexes on ORDER BY columns.\n\n"
    except Exception as e:
        response += f"Error analyzing execution plan: {str(e)}\n\n"
    
    # Existing indexes
    if existing_indexes:
        response += "## Existing Indexes That Match Query Needs\n\n"
        response += "| Table | Columns | Existing Index | Reason |\n"
        response += "| ----- | ------- | -------------- | ------ |\n"
        
        for index in existing_indexes:
            columns_str = ", ".join(index["columns"])
            response += f"| {index['table']} | {columns_str} | {index['existing_index']} | {index['reason']} |\n"
        
        response += "\n"
    
    # Missing indexes
    if missing_indexes:
        response += "## Recommended New Indexes\n\n"
        response += "| Table | Columns | Reason | SQL |\n"
        response += "| ----- | ------- | ------ | --- |\n"
        
        for index in missing_indexes:
            columns_str = ", ".join(index["columns"])
            index_name = f"idx_{index['table']}_{'_'.join(index['columns'])}"
            create_sql = f"CREATE INDEX {index_name} ON {index['table']} ({columns_str});"
            
            response += f"| {index['table']} | {columns_str} | {index['reason']} | `{create_sql}` |\n"
        
        response += "\n"
    else:
        response += "## No New Indexes Recommended\n\n"
        response += "The query appears to be using existing indexes effectively, or no clear index candidates were identified.\n\n"
    
    # Table structure information
    response += "## Table Structure Information\n\n"
    
    for table_name, structure in db_structure.items():
        response += f"### {table_name}\n\n"
        
        # Table stats
        stats = structure["stats"]
        if stats:
            response += "#### Statistics\n\n"
            response += f"- **Engine**: {stats.get('Engine', 'Unknown')}\n"
            response += f"- **Rows (approx)**: {stats.get('Rows', 'Unknown')}\n"
            response += f"- **Data Size**: {format_bytes(stats.get('Data_length', 0))}\n"
            response += f"- **Index Size**: {format_bytes(stats.get('Index_length', 0))}\n\n"
        
        # Columns
        columns = structure["columns"]
        if columns:
            response += "#### Columns\n\n"
            response += "| Column | Type | Nullable | Key |\n"
            response += "| ------ | ---- | -------- | --- |\n"
            
            for column in columns:
                nullable = "YES" if column["is_nullable"] == "YES" else "NO"
                key = column["column_key"] or ""
                
                response += f"| {column['column_name']} | {column['column_type']} | {nullable} | {key} |\n"
            
            response += "\n"
        
        # Existing indexes
        indexes = structure["indexes"]
        if indexes:
            response += "#### Existing Indexes\n\n"
            response += "| Name | Columns | Type | Unique |\n"
            response += "| ---- | ------- | ---- | ------ |\n"
            
            for index in indexes:
                unique = "No" if index["non_unique"] == 1 else "Yes"
                
                response += f"| {index['index_name']} | {index['columns']} | {index['index_type']} | {unique} |\n"
            
            response += "\n"
    
    # Additional recommendations
    response += "## Additional Recommendations\n\n"
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
