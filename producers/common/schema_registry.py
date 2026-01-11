"""
Schema Registry utilities for Avro serialization
"""
import json
import logging
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


logger = logging.getLogger(__name__)


class SchemaRegistry:
    """Schema Registry client for registering and retrieving Avro schemas"""
    
    def __init__(self, schema_registry_url):
        self.client = SchemaRegistryClient({'url': schema_registry_url})
        self.serializers = {}
    
    def register_schema(self, subject, schema_path):
        """Register a schema from file"""
        try:
            with open(schema_path, 'r') as f:
                schema_str = f.read()
            
            schema = Schema(schema_str, schema_type='AVRO')
            schema_id = self.client.register_schema(subject, schema)
            
            logger.info(f"Schema registered: {subject} (ID: {schema_id})")
            return schema_id
        except Exception as e:
            logger.error(f"Failed to register schema {subject}: {e}")
            raise
    
    def get_serializer(self, schema_path):
        """Get or create an Avro serializer for a schema"""
        if schema_path not in self.serializers:
            with open(schema_path, 'r') as f:
                schema_str = f.read()
            
            self.serializers[schema_path] = AvroSerializer(
                self.client,
                schema_str
            )
        
        return self.serializers[schema_path]
    
    def serialize(self, schema_path, data, topic):
        """Serialize data using Avro schema"""
        serializer = self.get_serializer(schema_path)
        ctx = SerializationContext(topic, MessageField.VALUE)
        return serializer(data, ctx)
    
    def register_all_schemas(self, schemas_dir='../schemas'):
        """Register all schemas in the schemas directory"""
        import os
        
        schemas = {
            'km.clicks.raw.v1-value': os.path.join(schemas_dir, 'click-event.avsc'),
            'km.purchases.raw.v1-value': os.path.join(schemas_dir, 'purchase-event.avsc'),
            'km.anomalies.v1-value': os.path.join(schemas_dir, 'anomaly-event.avsc')
        }
        
        for subject, schema_path in schemas.items():
            if os.path.exists(schema_path):
                self.register_schema(subject, schema_path)
            else:
                logger.warning(f"Schema file not found: {schema_path}")
        
        logger.info("All schemas registered successfully")


def register_schemas_cli():
    """CLI utility to register schemas"""
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    schema_registry_url = sys.argv[1] if len(sys.argv) > 1 else 'http://localhost:8081'
    schemas_dir = sys.argv[2] if len(sys.argv) > 2 else '../schemas'
    
    sr = SchemaRegistry(schema_registry_url)
    sr.register_all_schemas(schemas_dir)
    
    print("✅ All schemas registered successfully!")


if __name__ == '__main__':
    register_schemas_cli()
