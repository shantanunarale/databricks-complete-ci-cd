from pyspark.sql.functions import create_map, current_timestamp, lit

def add_map(df, **kwargs):
    print(kwargs)
    map_list = []
    for val in kwargs.items():
        map_list.append(lit(val[0]))
        map_list.append(lit(val[1]))    
    map_list.append(lit("insert_timestamp"))
    map_list.append(current_timestamp().cast("string"))
    return df.withColumn("metadata", create_map(map_list))