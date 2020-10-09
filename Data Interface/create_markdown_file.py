import pandas as pd

from utils import build_context

def generate_markdown_file(release_excel_path: str):
    """ Generate the human-readable release file.

    Args:
        release_excel_path: str
    Returns:
        None
    """

    context = build_context(release_excel_path, "Ground Truth DataSchema Table")

    release_markdown_file = "DataSchema_spec.md"
    available_keys = [e for e in context.values()]

    # this list used to generate the numbering format
    level_index_list = [0] * (max([e.level_number for e in available_keys]) + 1)

    with open(f"{release_markdown_file}", "w", newline="\n") as md_file:

        foreword = extract_foreword(release_excel_path, ["Version", "Specification", "Reserved Keywords"])

        # add the version control information to markdown file
        construct_version_section(md_file, foreword)

        # add specification statement information to markdown file
        construct_specification_section(md_file, foreword)
        
        # add reserved keyword information to markdown file
        construct_reserved_keyword_section(md_file, foreword)

        # add table of content to makedown file
        construct_table_of_content_section(md_file, foreword, available_keys)
        
        # add main body to markdown file
        construct_main_body_section(md_file, available_keys, level_index_list)

def extract_foreword(excel_path, sheet_name_list):
    """
    This method aims to extract the none-data schema information.
    """
    DataSchema_key_df_list = [
        pd.read_excel(io=excel_path, sheet_name=sheet_name)
        for sheet_name in sheet_name_list
    ]
    return DataSchema_key_df_list

def construct_version_section(md_file, foreword):
    version_df = foreword[0]

    md_file.write("## Release Guidelines\n")

    end_version_flag = False

    for i in range(len(version_df)):
        content = version_df.iloc[i]["Release Guidelines"]

        if pd.notna(content):
            content = str(content).replace("(;)", "( ; )")
            if not end_version_flag:
                if content != "Version":
                    md_file.write(f"{content}\n")
                else:
                    md_file.write("\n")
                    md_file.write("|Version|Date|Who|Notes|\n")
                    md_file.write("| ------ | ------ | ------ | ------ |\n")
                    end_version_flag = True
            else:
                md_file.write(
                        "|{0}|{1}|{2}|{3}|\n".format(
                            content,
                            str(version_df.iloc[i]["Unnamed: 1"]).replace("\n", "; "),
                            str(version_df.iloc[i]["Unnamed: 2"]).replace("\n", "; "),
                            str(version_df.iloc[i]["Unnamed: 3"])
                            .replace("\n", ", ")
                            .replace("|", r"\|"),
                        )
                )

    md_file.write("\n")

def construct_specification_section(md_file, foreword):
    specification_sheet_df = foreword[1]

    title_words = [
        "Scope",
        "GT DataSchema Levels",
        "Deviation to Accurate Application of GT DataSchema",
        "Namespace",
        "Hierarchy Separator",
        "Reserved Keywords",
        "Uppercase / Lowercase",
        "Multi-value Field",
        "Missing Data",
    ]

    # the specification part
    for i in range(len(specification_sheet_df)):
        content = specification_sheet_df.iloc[i]["Scope"].replace("(;)", "( ; )")
        if i == 0:
            md_file.write("## Scope\n")
            md_file.write("{0}\n".format(content))
        else:
            if content in title_words:
                md_file.write("\n")
                md_file.write("## {0}\n".format(content))
            else:
                md_file.write("{0}\n".format(content))

def construct_reserved_keyword_section(md_file, foreword):
    reserved_keyword_df = foreword[2]
    # the reserved key part
    md_file.write("\n")
    md_file.write("|Keyword|Hierarchy Level|Notes|\n")
    md_file.write("| ------ | ------ | ------ |\n")
    for i in range(len(reserved_keyword_df)):
        md_file.write(
            "|{0}|{1}|{2}|\n".format(
                str(reserved_keyword_df.iloc[i]["Keyword"])
                .replace("<", r"\<")
                .replace(">", r"\>"),
                str(reserved_keyword_df.iloc[i]["Hierarchy Level"]).replace(
                    "nan", "None"
                ),
                str(reserved_keyword_df.iloc[i]["Notes"])
                .replace("|", r"\|")
                .replace("(;)", "( ; )")
                .replace("\n", " "),
            )
        )
    md_file.write("\n")

def as_anchor(name: str, content: str = ""):
    """ Return a link for a PDF section header
    """
    return f'<a name="{name}">{content}</a>'

def construct_main_body_section(md_file, available_keys, level_index_list):
    for node_content in available_keys:
        if node_content.datatype == "group":
            if node_content.level_number == 0:
                level_index_list[0] += 1

                hash_format = "#" * (node_content.level_number + 3)
                link = as_anchor(name=node_content.get_anchor_key())
                md_file.write(
                    f'{hash_format} {link} {level_index_list[0]}.'
                    f"{node_content.group}\n"
                )

                group_level = level_index_list[0]
                level_index_list = [0] * (
                    max([e.level_number for e in available_keys]) + 1
                )
                level_index_list[0] = group_level
            else:
                level_index_list[node_content.level_number] += 1

                numbering_format = ".".join(
                    str(e) for e in level_index_list[: node_content.level_number + 1]
                )
                data_field_name = node_content.get_markdown_display_key().split("|")[-1]
                hash_format = "#" * (node_content.level_number + 3)
                link = as_anchor(node_content.get_anchor_key())
                md_file.write(
                    f'{hash_format} {link} {numbering_format}.'
                    f"{data_field_name}\n"
                )

                for i in range(node_content.level_number + 1, len(level_index_list)):
                    level_index_list[i] = 0
        else:
            link = as_anchor(node_content.get_anchor_key())
            md_file.write(f'{link}\n')

            if node_content.datatype != "group":
                text_content = "".join(
                    [
                        "|DataSchema Key|",
                        node_content.get_markdown_display_key(),
                        "|\n",
                    ]
                )
                text_content = "".join([text_content, "| ------ | ------ |\n"])
                text_content = "".join(
                    [text_content, "|DataSchema Group|", node_content.group, "|\n"]
                )
                text_content = "".join(
                    [text_content, "|Date Type|", node_content.datatype, "|\n"]
                )
                text_content = "".join(
                    [
                        text_content,
                        "|Defined Values|",
                        node_content.defined_values.replace("<", r"\<")
                        .replace(">", r"\>")
                        .replace("(;)", "( ; )"),
                        "|\n",
                    ]
                )
                text_content = "".join(
                    [
                        text_content,
                        "|Example|",
                        node_content.example.replace("<", r"\<")
                        .replace(">", r"\>")
                        .replace("(;)", "( ; )"),
                        "|\n",
                    ]
                )
                text_content = "".join(
                    [text_content, "|Notes|", node_content.notes, "|\n"]
                )

                md_file.write(text_content)
                md_file.write("\n")

def construct_table_of_content_section(md_file, available_keys, level_index_list):
    for node in available_keys:
        if node.level_number == 0:
            level_index_list[0] += 1
            md_file.write(
                f"* {level_index_list[0]}. [{node.group}]"
                f"(#{node.get_anchor_key()})\n"
            )

            group_level = level_index_list[0]
            level_index_list = [0] * (
                max([e.level_number for e in available_keys]) + 1 # if meet level 0, empty numbering format list
            )
            level_index_list[0] = group_level
        else:
            level_index_list[node.level_number] += 1

            prefix_space = "    " * (node.level_number)
            number = ".".join(
                str(index) for index in level_index_list[:node.level_number + 1] # if not level 0, get the numbering format, e.g. 3.1
            )
            basename = node.get_markdown_display_key().split("|")[-1]
            md_file.write(
                f"{prefix_space}* {number}. "
                f"[{basename}]"
                f"(#{node.get_anchor_key()})\n"
            )

            for i in range(node.level_number + 1, len(level_index_list)):
                level_index_list[i] = 0

    md_file.write("\n\n")
