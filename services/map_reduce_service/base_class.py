import dill


class Structure:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class FunctionGenerators:
    @staticmethod
    def convert_dict_to_object(dictionary: dict) -> Structure:
        return Structure(**dictionary)

    @staticmethod
    def de_serialize_function(function_binary_for_dill: bytes):
        return dill.loads(function_binary_for_dill)
