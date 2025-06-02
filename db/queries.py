"""
SQL queries used by the MySQL Performance Analyzer
"""

# Database structure queries
GET_TABLES = """
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

GET_COLUMNS = """
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

GET_INDEXES = """
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

GET_FOREIGN_KEYS = """
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

GET_TABLE_STATUS = """
    SHOW TABLE STATUS
"""

# Table statistics queries
GET_TABLE_STATISTICS = """
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

GET_TABLE_STATUS_BY_NAME = """
    SHOW TABLE STATUS LIKE '{table_name}'
"""

# Index information queries
GET_INDEX_INFORMATION = """
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

# Slow query analysis
CHECK_SLOW_QUERY_LOG = """
    SHOW VARIABLES LIKE 'slow_query_log'
"""

CHECK_PERFORMANCE_SCHEMA = """
    SHOW VARIABLES LIKE 'performance_schema'
"""

GET_SLOW_QUERIES = """
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

# InnoDB buffer pool analysis
GET_BUFFER_POOL_CONFIG = """
    SHOW VARIABLES WHERE Variable_name IN (
        'innodb_buffer_pool_size',
        'innodb_buffer_pool_instances',
        'innodb_buffer_pool_chunk_size',
        'innodb_page_size'
    )
"""

GET_BUFFER_POOL_STATUS = """
    SHOW STATUS WHERE Variable_name LIKE 'Innodb_buffer_pool%'
"""

GET_SERVER_MEMORY_INFO = """
    SHOW VARIABLES WHERE Variable_name IN (
        'key_buffer_size',
        'query_cache_size',
        'max_connections',
        'max_heap_table_size',
        'tmp_table_size'
    )
"""

GET_BUFFER_POOL_CONTENT = """
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

# Table fragmentation analysis
GET_FRAGMENTED_TABLES = """
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

# MySQL settings
GET_MYSQL_SETTINGS = """
    SHOW VARIABLES
"""

GET_MYSQL_SETTINGS_FILTERED = """
    SHOW VARIABLES WHERE Variable_name LIKE '%{pattern}%'
"""

# Read-only session settings
SET_READ_ONLY_SESSION = """
    SET SESSION TRANSACTION READ ONLY
"""

SET_QUERY_TIMEOUT = """
    SET SESSION MAX_EXECUTION_TIME=30000
"""