from typing import Dict
import pandas as pd
import copy

from data_schema_node import DataSchemaNode

def build_context(excel_path, sheet_name):
    """ Read excel sheet in and build list of objects from rows.

    Args:
        excel_path (str):
        sheet_name (str):
    Returns:
        Dict[str, DataSchemaNode]:
    """
    spec = pd.read_excel(io=excel_path, sheet_name=sheet_name)

    spec = spec.dropna(subset=["DataSchema Level"]).fillna("None").fillna("None")

    level_prefix_list = [] # this list includes all the prefix key names
    context: Dict[str, DataSchemaNode] = {} # the key is the schema title, value is its row content
    for index in range(len(spec)):
        node, level_prefix_list = generate_key_value_pair(
            spec.iloc[index], level_prefix_list
        )
        context[node.title] = node
    return context


def generate_key_value_pair(row_dict, level_prefix_list):
    """
    This method used to update the level prefix list, and also
    generate the DataSchema Node class

    Args:
        row_dict: Dict[str, pd.array]
        level_prefix_list: List[str]

    Returns:
        row_dict: Dict[str, pd.array]
        level_prefix_list: List[str]
    """
    DataSchema_level = int(row_dict["DataSchema Level"])

    # base on level number, get its full name, the gt is a reserved key as Ground Truth
    if not level_prefix_list[0 : DataSchema_level]:
        parent_DataSchema_key = "gt"
    else:
        if level_prefix_list.count("gt") > 0:
            parent_DataSchema_key = "|".join(level_prefix_list[0 : DataSchema_level])
        else:
            parent_DataSchema_key = "gt|" + "|".join(
                level_prefix_list[0 : DataSchema_level]
            )

    # the data schema key is: parent_DataSchema_key + '|' + title
    
    DataSchema_key = "{0}|{1}".format(parent_DataSchema_key, row_dict["DataSchema Key"])
    data_type = row_dict["Data Type"]
    defined_values = row_dict["Defined Values"]
    notes = row_dict["Notes"]
    previous_DataSchema_key = row_dict["Previous DataSchema Key (to be deleted)"]
    group = row_dict["DataSchema Group"]
    example = row_dict["Example"]

    # Only the first line has the DataSchema_key is empty issue
    if pd.isna(row_dict["DataSchema Key"]):
        level_prefix_list.append("gt")
        DataSchema_key = "gt"

    DataSchema_node = DataSchemaNode(
        DataSchema_key,
        data_type,
        defined_values,
        notes,
        DataSchema_level,
        parent_DataSchema_key,
        group,
        example,
        previous_DataSchema_key,
    )

    if not pd.isna(row_dict["DataSchema Key"]):
        if len(level_prefix_list) == DataSchema_level + 1:
            level_prefix_list.pop()
            level_prefix_list.append(row_dict["DataSchema Key"])
        elif len(level_prefix_list) < DataSchema_level + 1:
            level_prefix_list.append(row_dict["DataSchema Key"])
        else:
            level_prefix_list = level_prefix_list[0 : DataSchema_level]
            level_prefix_list.append(row_dict["DataSchema Key"])

    return DataSchema_node, level_prefix_list