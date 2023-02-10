def MapFunction(row):
    return row.item_type, row.quantity


def ReduceFunction(a, b):
    return a + b
