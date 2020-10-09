import argparse

from create_markdown_file import generate_markdown_file
from create_json_file import generate_json_file

def main(extra_args: argparse.ArgumentParser = None):
    """
    Args:
        extra_args (argparse.ArgumentParser): Extra arguments for the parser.
    Returns:
        None
    """
    arg = argparse.ArgumentParser(
        "Convert DataSchema Specification into markdown format"
    )
    arg.add_argument(
        "-e",
        "--excel_file_path",
        help="Path to the DataSchema specification excel file.",
        required=True,
    )
    opts = arg.parse_args(extra_args)

    release_excel_path = opts.excel_file_path

    generate_markdown_file(release_excel_path)
    generate_json_file(release_excel_path)


if __name__ == "__main__":
    main()
