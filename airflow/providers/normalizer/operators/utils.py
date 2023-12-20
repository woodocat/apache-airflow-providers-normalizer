import json
import datetime


class MappingElements:

    def __init__(self, translation):

        destination = list(translation.keys())[0]
        # 'schema.table'

        table = destination.split(".")[-1]
        # 'table'

        mapping = translation[destination]
        # {'UserId*': {'user_id':'int'}, 'FullName': {'fullname':'varchar'}, ...}

        original = list(mapping.keys())
        # ['UserId*', 'FullName', ...]

        fieldset = [mapping[field] for field in mapping]
        # [{'user_id':'int'}, {'fullname':'varchar'}, ...]

        fields = [''.join(x.keys()) for x in fieldset]
        # ['user_id', 'fullname', ...]

        types = [''.join(x.values()).lower() for x in fieldset]
        # ['int', 'varchar', ...]

        definition = [''.join(x.keys())+" "+''.join(x.values()) for x in fieldset]
        # ['user_id int', 'fullname varchar', ...]

        has_uniques = '*' in ''.join(mapping.keys())
        uniques = [mapping[field] for field in mapping if '*' in field]
        # [{'user_id':'int'}, {'fullname':'int'}]

        self.destination = destination
        self.table = table
        self.mapping = mapping
        self.original = original
        self.fieldset = fieldset
        self.fields = fields
        self.types = types
        self.definition = definition
        self.uniques = uniques
        self.has_uniques = has_uniques

    def __str__(self):
        return self.definition


def get_table(key):
    return key.split("[")[0].strip("[]") if "[" in key else key


def get_fields(key):
    return key.split("[")[1].strip("[]") if "[" in key else None


def flatten(dictionary, parent_key='', delim='__'):
    items = []
    for key, value in dictionary.items():
        child_key = f"{parent_key}{delim}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten(value, child_key, delim=delim).items())
        else:
            items.append((child_key, value))

    return dict(items)


def prepare_value(value, cast):
    if (cast == 'timestamp' or cast == 'datetime64') and value:
        value = value.strftime("%Y-%m-%d %H:%M:%S.%f")

    if cast == 'datetime' and value and isinstance(value, datetime):
        value = value.strftime("%Y-%m-%d %H:%M:%S")

    if cast == 'date' and value:
        value = str(value)

    if isinstance(value, list) or isinstance(value, dict):
        value = json.dumps(value, ensure_ascii=False)

    if isinstance(value, bool):
        value = str(value).lower()

    if isinstance(value, str):
        value = value.replace("\\", "\\\\").replace("'", "''")
        value = f"'{value}'"

    if isinstance(value, int) or isinstance(value, float):
        value = str(value)

    if value is None:
        value = 'NULL'

    return value


def normalize(documents, buffers, ids, mappings, parent_key, fk=0, id=0, delim="__", stringify_values=True):
    if parent_key not in buffers:
        buffers[parent_key] = []

    # Filter the keys for current level to work with
    nested_keys = [key.replace(parent_key+".", "") for key in mappings if parent_key in key and key != parent_key]
    mapping = mappings[parent_key]

    id = ids[parent_key] if not id else id

    for document in documents:
        flat_document = flatten(document, delim=delim)
        id += 1

        values = []
        for i, key in enumerate(mapping.fields):
            origin = mapping.original[i]
            value = flat_document.get(origin)
            cast = mapping.types[i]
            prepared = prepare_value(value, cast)
            values.append(prepared)

            # Check the nested fields to normalize
            if value and isinstance(value, list) and key in nested_keys:
                child_key = parent_key + "." + key
                child_id = ids[child_key] if child_key in ids else 0
                normalize(value, buffers, ids, mappings, child_key, fk=id, id=child_id)

        # TODO: if stringify_values:
        foreign_key = [str(fk)] if fk else []
        values = foreign_key + [str(id)] + values
        buffers[parent_key].append("(" + ", ".join(values) + ")")

    ids[parent_key] = id
