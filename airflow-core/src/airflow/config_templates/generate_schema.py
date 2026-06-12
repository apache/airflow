import json
import os
import sys

def generate_schema():
    try:
        import yaml
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyyaml"])
        import yaml
        
    config_yml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "config.yml"))
    if not os.path.exists(config_yml_path):
        print(f"Error: {config_yml_path} does not exist.")
        sys.exit(1)
        
    with open(config_yml_path, "r", encoding="utf-8") as f:
        config_desc = yaml.safe_load(f)
        
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Airflow Configuration Schema",
        "description": "JSON Schema for airflow.cfg configuration file",
        "type": "object",
        "properties": {},
        "additionalProperties": False
    }
    
    for section, section_data in sorted(config_desc.items()):
        # section_data might be None if section has no contents, though usually it's a dict
        if not section_data:
            continue
        options = section_data.get("options", {})
        if not options:
            continue
            
        section_properties = {}
        
        for option, option_data in sorted(options.items()):
            if not option_data:
                option_data = {}
            opt_type = option_data.get("type", "string")
            sensitive = option_data.get("sensitive", False)
            description = option_data.get("description", "")
            if description is None:
                description = ""
            else:
                description = str(description).strip()
            
            # Map YAML config types to JSON Schema types
            if opt_type == "string":
                js_type = {"type": "string"}
            elif opt_type == "integer":
                js_type = {"type": "integer"}
            elif opt_type == "float":
                js_type = {"type": "number"}
            elif opt_type == "boolean":
                js_type = {"type": "boolean"}
            else:
                js_type = {"type": "string"}
                
            if description:
                js_type["description"] = description
                
            section_properties[option] = js_type
            
            # If sensitive or name ends with password/secret, allow _cmd and _secret suffixes
            if sensitive or option.endswith(("password", "secret")):
                section_properties[f"{option}_cmd"] = {
                    "type": "string",
                    "description": f"Command to retrieve the value for [{section}] {option}"
                }
                section_properties[f"{option}_secret"] = {
                    "type": "string",
                    "description": f"Secrets Backend path to retrieve the value for [{section}] {option}"
                }
                
        schema["properties"][section] = {
            "type": "object",
            "properties": section_properties,
            "additionalProperties": False
        }
        
    schema_path = os.path.join(os.path.dirname(__file__), "schema.json")
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
        f.write("\n")
    print(f"SUCCESS: Generated schema at {schema_path}")

if __name__ == "__main__":
    generate_schema()
