def MapFunction(row):
    return row.item_type, row.quantity


def ReduceFunction(values):
    return sum(values)
