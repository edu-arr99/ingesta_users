import boto3
import pandas as pd
import json
from datetime import datetime
import os
from typing import List, Dict, Any

class DynamoDBExporter:
    def __init__(self, table_names: List[str]):
        """        
        Inicializa el exportador de datos de DynamoDB

        Args:
            table_names: Lista de tablas a importar en DynamoDB
        """
        self.dynamodb = boto3.resource('dynamodb', 'us-east-1')
        self.table_names = table_names

        
        os.makedirs('/data', exist_ok=True)

    def scan_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Scan tabla DynamoDB 
        
        Args:
            table_name: Tabla a scanear
            
        Returns:
            Lista de registros de la tabla
        """
        table = self.dynamodb.Table(table_name)
        items = []
        
        try:
            response = table.scan()
            items.extend(response['Items'])
            
            while 'LastEvaluatedKey' in response:
                response = table.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])
                
            print(f"Escaneo completo de {len(items)} registros de {table_name}")
            return items
            
        except Exception as e:
            print(f"Error scaneando {table_name}: {str(e)}")
            return []

    def normalize_data(self, items: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalizar registros DynamoDB a pandas DataFrame
        
        Args:
            items: Lista de registros de DynamoDB
            
        Returns:
            Pandas DataFrame normalizado
        """
        df = pd.json_normalize(items)
        return df

    def export_data(self, df: pd.DataFrame, table_name: str):
        """
        Exporta data normalizada a archivo '.csv'
        
        Args:
            df: Pandas DataFrame a exportar
            table_name: Nombre de la tabla
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{table_name}_{timestamp}"
        
        try:
            output_path = os.path.join(self.output_dir, f"{filename}.csv")
            df.to_csv(output_path, index=False)
                
            print(f"Se exportó {table_name} a {output_path}")
            
        except Exception as e:
            print(f"Error exportando {table_name}: {str(e)}")

    def run(self):
        """
        Correr el proceso completo de exportación
        """
        for table_name in self.table_names:
            print(f"\Procesando tabla: {table_name}")
            
            # Scan table
            items = self.scan_table(table_name)
            if not items:
                continue
                
            # Normalize data
            df = self.normalize_data(items)
            
            # Export data
            self.export_data(df, table_name)

if __name__ == "__main__":
    TABLE_NAMES = ["dev-fp-t_users", "dev-fp-t_user_replays"]
    
    # Initialize and run exporter
    exporter = DynamoDBExporter(table_names=TABLE_NAMES)
    exporter.run()