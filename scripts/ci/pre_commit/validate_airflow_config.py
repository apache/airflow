#!/usr/bin/env python
import sys
import os
import configparser
import json

def validate_config_file(config_path, schema):
    # Verify file exists
    if not os.path.exists(config_path):
        print(f"Error: Configuration file {config_path} does not exist.")
        return False

    # Parse config file
    config = configparser.ConfigParser()
    try:
        config.read(config_path, encoding="utf-8")
    except Exception as e:
        print(f"Error parsing {config_path} as INI/CFG: {e}")
        return False

    # Convert config parser sections/options to a typed dictionary matching the schema
    config_dict = {}
    for section in config.sections():
        config_dict[section] = {}
        section_schema = schema.get("properties", {}).get(section, {}).get("properties", {})
        
        for option in config.options(section):
            val = config.get(section, option)
            opt_schema = section_schema.get(option, {})
            opt_type = opt_schema.get("type", "string")

            # Cast type accordingly so jsonschema can validate it
            if opt_type == "integer":
                try:
                    config_dict[section][option] = int(val)
                except ValueError:
                    config_dict[section][option] = val  # Let validator catch type mismatch
            elif opt_type == "number":
                try:
                    config_dict[section][option] = float(val)
                except ValueError:
                    config_dict[section][option] = val
            elif opt_type == "boolean":
                val_lower = val.lower()
                if val_lower in ("true", "yes", "on", "1"):
                    config_dict[section][option] = True
                elif val_lower in ("false", "no", "off", "0", ""):
                    config_dict[section][option] = False
                else:
                    config_dict[section][option] = val
            else:
                config_dict[section][option] = val

    # Validate against schema using jsonschema
    try:
        import jsonschema
    except ImportError:
        print("Error: jsonschema library is required for config validation.")
        return False

    try:
        jsonschema.validate(instance=config_dict, schema=schema)
        return True
    except jsonschema.ValidationError as err:
        # Generate a friendly error message
        path = list(err.absolute_path)
        if len(path) == 2:
            section, option = path
            print(f"Validation Error in {config_path} under [{section}] {option}: {err.message}")
        elif len(path) == 1:
            section = path[0]
            print(f"Validation Error in {config_path} under [{section}]: {err.message}")
        else:
            print(f"Validation Error in {config_path}: {err.message}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: validate_airflow_config.py <path_to_airflow.cfg> ...")
        sys.exit(1)

    # Resolve schema path relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.abspath(os.path.join(script_dir, "../../../airflow-core/src/airflow/config_templates/schema.json"))
    
    if not os.path.exists(schema_path):
        print(f"Error: Schema file not found at {schema_path}. Run generate_schema.py first.")
        sys.exit(1)

    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    success = True
    for config_path in sys.argv[1:]:
        if not validate_config_file(config_path, schema):
            success = False

    if not success:
        sys.exit(1)
    else:
        print("SUCCESS: All config files passed validation.")

if __name__ == "__main__":
    main()
