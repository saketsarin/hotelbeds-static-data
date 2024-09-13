from flask import Flask, request, jsonify
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import os
from dotenv import load_dotenv

app = Flask(__name__)

# Load environment variables
load_dotenv()

# Database connection with optimized pool settings
DB_URL = os.getenv('DB_URL')
engine = create_engine(DB_URL, pool_size=20, max_overflow=10, pool_timeout=30, pool_recycle=1800)

@app.route('/fetch', methods=['GET'])
def fetch_data():
    table_name = request.args.get('table')
    code_param = request.args.get('code')
    
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400
    
    try:
        with engine.connect() as connection:
            # Check if the table exists
            inspector = inspect(connection)
            if not inspector.has_table(table_name):
                return jsonify({"error": f"Table {table_name} does not exist"}), 404
            
            # Build the query safely using SQLAlchemy text to prevent SQL injection
            if code_param:
                codes_list = code_param.split(',')
                query = text(f"SELECT * FROM {table_name} WHERE code IN :codes")
                result = pd.read_sql(query, connection, params={"codes": tuple(codes_list)})
            else:
                query = text(f"SELECT * FROM {table_name}")
                result = pd.read_sql(query, connection)
            
            data = result.to_dict(orient='records')
            return jsonify(data)
    
    except Exception as e:
        error_message = str(e)
        if "QueuePool limit" in error_message:
            error_message = "Database connection limit reached. Please try again later."
        return jsonify({"error": error_message}), 500

if __name__ == '__main__':
    app.run(debug=True)
