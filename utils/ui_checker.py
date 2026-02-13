from typing import Callable, Dict, List


class SelectChecker:
    def __init__(self, list_req: Dict, input_fn: Callable[[str], str] = input):
        self.list_req = list_req
        self.input_fn = input_fn

    def util_character(self):
        output = "Please Type and Enter: \n"
        output2 = "Invalid input!  "
        items = list(self.list_req.items())
        for idx, (key, value) in enumerate(items):
            sep = ", " if idx < len(items) - 1 else ""
            output += f"({key}) for {value}{sep}"
            output2 += f"({key}) for {value}{sep}"
        return [f"{output}:---> ", f"{output2}!!"]

    def start_checker(self):
        prompt, invalid_msg = self.util_character()
        while True:
            try:
                selected = int(self.input_fn(prompt))
                if selected in self.list_req:
                    return selected
                print(invalid_msg)
            except ValueError:
                print(invalid_msg)
            except KeyboardInterrupt:
                print("\nProgram interrupted.")
                return None

    def input_update_shp(self, list_geodata: List[str], add: str = ""):
        if not list_geodata:
            raise ValueError("No input files/options available.")

        while True:
            try:
                for i, item in enumerate(list_geodata, start=1):
                    print(f"({i}) : {item} ------------------")
                print(
                    "\n ------------------------------- PLEASE CHOOSE THIS CAREFULLY SINCE IT WILL UPDATE THE PLOT DATA \n----------------------------"
                )
                num_select = int(self.input_fn(f"please select the file/option {add} from number indicated above!: "))
                if 1 <= num_select <= len(list_geodata):
                    print(f"you select the file for upload/updating ({num_select}): {list_geodata[num_select-1]}")
                    return num_select
                print("you selected out of range number")
            except ValueError:
                print("please select number only!")
            except KeyboardInterrupt:
                print("\nProgram interrupted.")
                return None