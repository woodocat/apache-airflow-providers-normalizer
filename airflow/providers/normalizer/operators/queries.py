DROP_TABLE_QUERY = '''
    DROP TABLE IF EXISTS {table}
'''


CREATE_TABLE_QUERY = '''
    CREATE TABLE IF NOT EXISTS {table} (
        {definition}
    )
'''


INSERT_INTO_QUERY = '''
    INSERT INTO {table} (
        {fields}
    )
    VALUES {values}
'''


SELECT_ALL_QUERY = '''
    SELECT {fields}
    FROM {table}
'''


SELECT_COUNT_QUERY = '''
    SELECT count(*)
    FROM {table}
'''


SELECT_MAX_QUERY = '''
   SELECT max({pk})
   FROM {table}
'''
