"""
Functions for analyzing MySQL database structure
"""
from typing import Dict, List, Any
from db.connector import MySQLConnector

def get_database_structure(connector: MySQLConnector) -> Dict[str, Any]:
    """
    Get comprehensive database structure information
    
    Args:
        connector: MySQLConnector instance
        
    Returns:
        Dict containing database structure information
    """
    # Get tables
    tables_query = """
        SELECT 
            table_name, 
            engine, 
            table_rows,
            avg_row_length,
            data_length,
            index_length,
            create_time,
            update_time
        FROM 
            information_schema.tables
        WHERE 
            table_schema = DATABASE()
        ORDER BY 
            table_name
    """
    tables = connector.execute_query(tables_query)
    
    # Get columns for each table
    columns_query = """
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
        ORDER BY 
            table_name, ordinal_position
    """
    columns = connector.execute_query(columns_query)
    
    # Get indexes
    indexes_query = """
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
        GROUP BY 
            table_name, index_name, index_type, non_unique
        ORDER BY 
            table_name, index_name
    """
    indexes = connector.execute_query(indexes_query)
    
    # Get foreign keys
    foreign_keys_query = """
        SELECT 
            tc.table_name, 
            kcu.column_name, 
            kcu.referenced_table_name,
            kcu.referenced_column_name,
            rc.update_rule,
            rc.delete_rule
        FROM 
            information_schema.table_constraints tc
        JOIN 
            information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN 
            information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE 
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = DATABASE()
        ORDER BY 
            tc.table_name, kcu.column_name
    """
    foreign_keys = connector.execute_query(foreign_keys_query)
    
    # Get table statistics
    table_stats_query = """
        SHOW TABLE STATUS
    """
    table_stats = connector.execute_query(table_stats_query)
    
    # Return all collected information
    return {
        "tables": tables,
        "columns": columns,
        "indexes": indexes,
        "foreign_keys": foreign_keys,
        "table_stats": table_stats
    }

def organize_db_structure_by_table(db_structure: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Organize database structure by table for easier analysis
    
    Args:
        db_structure: Database structure from get_database_structure
        
    Returns:
        Dict with tables as keys and their details as values
    """
    organized = {}
    
    # Process tables
    for table in db_structure["tables"]:
        table_name = table["table_name"]
        organized[table_name] = {
            "info": table,
            "columns": [],
            "indexes": [],
            "foreign_keys": []
        }
    
    # Add columns to their respective tables
    for column in db_structure["columns"]:
        table_name = column["table_name"]
        if table_name in organized:
            organized[table_name]["columns"].append(column)
    
    # Add indexes to their respective tables
    for index in db_structure["indexes"]:
        table_name = index["table_name"]
        if table_name in organized:
            organized[table_name]["indexes"].append(index)
    
    # Add foreign keys to their respective tables
    for fk in db_structure["foreign_keys"]:
        table_name = fk["table_name"]
        if table_name in organized:
            organized[table_name]["foreign_keys"].append(fk)
    
    # Add table stats
    for stat in db_structure["table_stats"]:
        table_name = stat["Name"]
        if table_name in organized:
            organized[table_name]["stats"] = stat
    
    return organized

def analyze_database_structure_for_response(db_structure: Dict[str, Any]) -> str:
    """
    Analyze database structure and format as a response
    
    Args:
        db_structure: Database structure from get_database_structure
        
    Returns:
        Formatted string with analysis
    """
    # Organize by table
    organized = organize_db_structure_by_table(db_structure)
    
    # Start building the response
    response = "# MySQL Database Structure Analysis\n\n"
    
    # Database overview
    response += "## Database Overview\n\n"
    response += f"- **Total Tables**: {len(organized)}\n"
    
    # Count total indexes
    total_indexes = sum(len(table_data["indexes"]) for table_data in organized.values())
    response += f"- **Total Indexes**: {total_indexes}\n"
    
    # Count total foreign keys
    total_fks = sum(len(table_data["foreign_keys"]) for table_data in organized.values())
    response += f"- **Total Foreign Keys**: {total_fks}\n\n"
    
    # Storage engines used
    engines = {}
    for table_name, table_data in organized.items():
        engine = table_data["info"]["engine"]
        if engine not in engines:
            engines[engine] = 0
        engines[engine] += 1
    
    response += "### Storage Engines\n\n"
    for engine, count in engines.items():
        response += f"- **{engine}**: {count} tables\n"
    response += "\n"
    
    # Table details
    response += "## Table Details\n\n"
    
    for table_name, table_data in organized.items():
        info = table_data["info"]
        columns = table_data["columns"]
        indexes = table_data["indexes"]
        foreign_keys = table_data["foreign_keys"]
        stats = table_data.get("stats", {})
        
        response += f"### {table_name}\n\n"
        
        # Table info
        response += "#### General Information\n\n"
        response += f"- **Engine**: {info['engine']}\n"
        response += f"- **Rows (approx)**: {info['table_rows'] or 'Unknown'}\n"
        response += f"- **Data Size**: {format_bytes(info['data_length'])}\n"
        response += f"- **Index Size**: {format_bytes(info['index_length'])}\n"
        
        if stats:
            if stats.get("Create_time"):
                response += f"- **Created**: {stats['Create_time']}\n"
            if stats.get("Update_time"):
                response += f"- **Last Updated**: {stats['Update_time']}\n"
            if stats.get("Auto_increment"):
                response += f"- **Auto Increment**: {stats['Auto_increment']}\n"
        
        # Columns
        response += "\n#### Columns\n\n"
        response += "| Column | Type | Nullable | Key | Default | Extra |\n"
        response += "| ------ | ---- | -------- | --- | ------- | ----- |\n"
        
        for column in columns:
            nullable = "YES" if column["is_nullable"] == "YES" else "NO"
            key = column["column_key"] or ""
            default = column["column_default"] or ""
            extra = column["extra"] or ""
            
            response += f"| {column['column_name']} | {column['column_type']} | {nullable} | {key} | {default} | {extra} |\n"
        
        # Indexes
        if indexes:
            response += "\n#### Indexes\n\n"
            response += "| Name | Columns | Type | Unique |\n"
            response += "| ---- | ------- | ---- | ------ |\n"
            
            for index in indexes:
                unique = "No" if index["non_unique"] == 1 else "Yes"
                response += f"| {index['index_name']} | {index['columns']} | {index['index_type']} | {unique} |\n"
        
        # Foreign Keys
        if foreign_keys:
            response += "\n#### Foreign Keys\n\n"
            response += "| Column | References | On Update | On Delete |\n"
            response += "| ------ | ---------- | --------- | --------- |\n"
            
            for fk in foreign_keys:
                ref = f"{fk['referenced_table_name']}({fk['referenced_column_name']})"
                response += f"| {fk['column_name']} | {ref} | {fk['update_rule']} | {fk['delete_rule']} |\n"
        
        response += "\n"
    
    # Add optimization recommendations
    response += "## Optimization Recommendations\n\n"
    
    # Analyze tables without primary keys
    tables_without_pk = []
    for table_name, table_data in organized.items():
        has_pk = any(column["column_key"] == "PRI" for column in table_data["columns"])
        if not has_pk:
            tables_without_pk.append(table_name)
    
    if tables_without_pk:
        response += "### Tables Without Primary Keys\n\n"
        response += "The following tables do not have primary keys, which can cause performance issues:\n\n"
        for table in tables_without_pk:
            response += f"- `{table}`\n"
        response += "\nConsider adding primary keys to these tables.\n\n"
    
    # Analyze potential index issues
    tables_with_many_indexes = []
    for table_name, table_data in organized.items():
        if len(table_data["indexes"]) > 5:
            tables_with_many_indexes.append((table_name, len(table_data["indexes"])))
    
    if tables_with_many_indexes:
        response += "### Tables With Many Indexes\n\n"
        response += "The following tables have a high number of indexes, which might impact INSERT/UPDATE performance:\n\n"
        for table, count in tables_with_many_indexes:
            response += f"- `{table}`: {count} indexes\n"
        response += "\nConsider reviewing these indexes to ensure they are all necessary.\n\n"
    
    # Analyze large tables
    large_tables = []
    for table_name, table_data in organized.items():
        data_size = table_data["info"]["data_length"] or 0
        if data_size > 100 * 1024 * 1024:  # 100 MB
            large_tables.append((table_name, data_size))
    
    if large_tables:
        response += "### Large Tables\n\n"
        response += "The following tables are large and may benefit from partitioning or archiving strategies:\n\n"
        for table, size in large_tables:
            response += f"- `{table}`: {format_bytes(size)}\n"
        response += "\n"
    
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