from functools import reduce

import pandas as pd


class Structure:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class FunctionGenerators:
    @staticmethod
    def convert_dict_to_object(dictionary: dict) -> Structure:
        return Structure(**dictionary)

    @staticmethod
    def convert_string_to_function(function_as_string: str, function_name: str):
        functions = {}
        exec(function_as_string, globals(), functions)
        return functions[function_name]


class MapAndShuffle(FunctionGenerators):
    def __init__(self, filename: str):
        self.csv = pd.read_csv(f'{filename}.csv').to_dict('records')
        self.map_ = {}

    def map_function_on_data(self, map_function_as_string: str) -> None:
        map_function = self.convert_string_to_function(map_function_as_string, function_name="MapFunction")
        self.map_ = {}
        for data in self.csv:
            key, value = map_function(self.convert_dict_to_object(data))
            if key not in self.map_:
                self.map_[key] = []
            self.map_[key].append(value)

    def get_map_value(self, key: str) -> list:
        return self.map_[key]

    @property
    def keys(self) -> list:
        return list(self.map_.keys())


class Reducer(FunctionGenerators):
    def __init__(self):
        self.reduced_data = {}

    def reduced_function_on_data(self, reduce_function_as_a_string: str, key: str, values: list) -> None:
        self.reduced_data[key] = reduce(
            self.convert_string_to_function(reduce_function_as_a_string, function_name="ReduceFunction"),
            values
        )

    def get_reduced_data(self, key):
        return self.reduced_data[key]


# if __name__ == '__main__':
#     map_function = """def MapFunction(row): return row.item_type, row.quantity
#     """
#     reduce_function = """def ReduceFunction(a, b): return a+b"""
#     mas = MapAndShuffle('random_data')
#     red = Reducer()
#     mas.map_function_on_data(map_function_as_string=map_function)
#     print(mas.get_map_value('orange'))
#     print(mas.keys)
#     print(sum(mas.get_map_value('orange')))
#     red.reduced_function_on_data(reduce_function, "orange", mas.get_map_value('orange'))
#     print(red.get_reduced_data("orange"))
