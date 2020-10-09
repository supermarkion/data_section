import json

from utils import build_context

def generate_json_file(release_excel_path):
    """ Generate the machine-readable release file.

    Args:
        release_excel_path: str
    Returns:
        None
    """
    release_json_file = "DataSchema_spec.json"

    context = build_context(release_excel_path, "DataSchema Table")

    with open(release_json_file, "w") as json_file:
        DataSchema_keys = [
            value.get_content()
            for key, value in context.items()
            if value.datatype != "group"
            and key != "gt"
            and value.datatype != "group-thirdparty"
        ]

        output_DataSchema_keys = []

        # post process the & reserved key
        for DataSchema_key in DataSchema_keys:
            if '&' in DataSchema_key['title']:
                for slot_index in range(len(DataSchema_key['title'].split('|'))):
                    if '&' in DataSchema_key['title'].split('|')[slot_index]:
                        sub_group_keys = DataSchema_key['title'].split('|')[slot_index].split('&')

                        for sub_group_key_index in range(len(sub_group_keys)):
                            sub_group_key_name = '|'.join(DataSchema_key['title'].split('|')[:slot_index]) + '|' + sub_group_keys[sub_group_key_index].strip() + '|' + '|'.join(DataSchema_key['title'].split('|')[slot_index + 1:])

                            if sub_group_key_name.endswith('|'): # handle the slot_index is the end index number
                                sub_group_key_name = sub_group_key_name[:-1]
                            sub_DataSchema_key = copy.deepcopy(DataSchema_key)
                            sub_DataSchema_key['title'] = sub_group_key_name
                            output_DataSchema_keys.append(sub_DataSchema_key)        
            else:
                output_DataSchema_keys.append(DataSchema_key)


        json.dump(output_DataSchema_keys, json_file, indent=2)
