import pandas as pd
import json
import os

def generate_glue_schema(csv_path: str) -> list:
    """
    Generate AWS Glue compatible schema from CSV file
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        List of column definitions in Glue format
    """
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Map pandas dtypes to Glue data types
    dtype_mapping = {
        'object': 'string',
        'int64': 'int',
        'float64': 'double',
        'bool': 'boolean',
        'datetime64[ns]': 'timestamp'
    }
    
    # Generate schema
    schema = []
    for column in df.columns:
        dtype = str(df[column].dtype)
        glue_type = dtype_mapping.get(dtype, 'string')  # default to string if type unknown
        
        column_def = {
            "Name": column,
            "Type": glue_type
        }
        schema.append(column_def)
    
    return schema

def save_schema(schema: list, output_path: str):
    """
    Save schema to JSON file
    
    Args:
        schema: Schema list
        output_path: Path where to save the JSON file
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    print(f"Schema saved to: {output_path}")

def main():
    # Get all CSV files in the data directory
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    schema_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schemas')
    
    # Create schemas directory if it doesn't exist
    os.makedirs(schema_dir, exist_ok=True)
    
    # Process each CSV file
    for filename in os.listdir(data_dir):
        if filename.endswith('.csv'):
            csv_path = os.path.join(data_dir, filename)
            print(f"\nProcessing: {filename}")
            
            try:
                # Generate schema
                schema = generate_glue_schema(csv_path)
                
                # Save schema to JSON file
                schema_filename = f"{os.path.splitext(filename)[0]}_schema.json"
                schema_path = os.path.join(schema_dir, schema_filename)
                save_schema(schema, schema_path)
                
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

if __name__ == "__main__":
    main()
