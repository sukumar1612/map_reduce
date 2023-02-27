class SharedMapValue:
    MAP_VALUE: dict = None

    @classmethod
    def clear_map(cls):
        cls.MAP_VALUE.clear()

    @classmethod
    def update_map_list(cls, new_map_values: dict):
        cls.clear_map()
        cls.MAP_VALUE.update(new_map_values)
