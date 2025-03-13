import os
import json
from src.utils.etl_utils import get_config, set_up_logging


def init():
    config = get_config()
    set_up_logging(config)
    config['schema_json_path'] = os.path.join(config['static_dir'], 'SchemaSpecification22056.json')
    config['creation_script'] = os.path.join(config['script_dir'], 'create_hesa_3nf.sql')
    return config


def generate_create(table_name, table_structure: dict[dict]):
    """Generates CREATE TABLE statement from table name and definition dictionary."""

    # Initialise new CREATE statement
    create = f"CREATE TABLE hesa_3nf_{table_name.lower()} (\n"

    # Build column definitions
    column_definitions: list = []
    fields: dict = table_structure['Fields']
    for field in fields:
        column_definitions.append(f"        {field['Name'].lower()} {field['DataType']}")

    create += ',\n'.join(column_definitions)

    # Build PK clause 
    if len(table_structure['PK']) > 0:
        pk_elements: list = []
        elements: dict = table_structure['PK']
        for element in elements:
            pk_elements.append(element)
        
        create += ',\n        PRIMARY KEY ('
        create += ',\n'.join(pk_elements)
        create += ')\n'
    
    # Wrap-up the statement
    create += '        );\n'
    
    return create



def main():
    config = init()

    # Parse schema definition file into nested dictionary
    with open(config['schema_json_path']) as json_file:
        schema = json.load(json_file)

    # Generate CREATE TABLE statement for each top-level schema entity (table definition)
    creates = []

    for table_name, table_definition in schema['22056'].items():
        create = generate_create(table_name, table_definition)
        creates.append(create)

    # Write CREATE TABLE statements to file
    with open(config['creation_script'], 'w') as sql_file:
        for create in creates:
            sql_file.write(create + '\n')

if __name__ == '__main__':
    main()
