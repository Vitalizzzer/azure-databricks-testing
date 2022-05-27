CREATE_TABLE = "CREATE TABLE {} USING {} OPTIONS (path '{}', header '{}')"
SELECT_ALL_QUERY = "SELECT * FROM {}"
DROP_TABLE = "DROP TABLE IF EXISTS {}"
SELECT_DUPLICATES = "SELECT Count(*) as DuplicateRecords FROM (SELECT Count(*),product,price,discount,expDate " \
                    "FROM {} GROUP BY product,price,discount,expDate HAVING Count(*)>1)"
SELECT_NULL_VALUES = "SELECT Count(*) as NullValues FROM {} " \
                     "WHERE (product IS null OR price IS null OR discount IS null OR expDate IS null)"
