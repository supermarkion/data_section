class DataSchemaNode():
    """ Each data schema node is a single row in the DataSchema spec.

    The elements of this class are essentially the column.

    Each Data Schema Node has following critical information:
        - title (its name)
        - datatype (int, float, enum, string)
        - its full key name: prefix_keys + title
    """

    def __init__(
        self,
        title,
        datatype,
        defined_values,
        notes,
        level_number,
        parent_node,
        group,
        example,
        pre_key,
    ):
        self.title = str(title) # data field name
        self.datatype = str(datatype)
        self.defined_values = str(defined_values)
        self.notes = str(notes)
        self.level_number = level_number # define structure level
        self.parent_node = parent_node  # save its parent DataSchema title
        self.group = str(group)  # save the DataSchema key classification information
        self.example = str(example)
        self.previous_DataSchema_key = pre_key # define the previous row of data key

    def get_content(self):
        """ Return dictionary of content
        """
        return {
            "title": self.title,
            "datatype": self.datatype,
            "defined_values": self.defined_values,
            "notes": self.notes,
            "group": self.group,
            "example": self.example,
            "previous_DataSchema_key": self.previous_DataSchema_key,
        }

    def get_markdown_display_key(self):
        """ Escape title string for markdown
        """
        return self.title.replace("|", r"\|").replace("<", r"\<").replace(">", r"\>")

    def get_anchor_key(self):
        """ Make title into useful URL-like key for PDF.
        """
        return self.title.replace("|", "").replace("<", "").replace(">", "")

