from flask import Flask, jsonify, request
from main import run_cashflow_model
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv('.env.local')

app = Flask(__name__)

# MongoDB setup
client = MongoClient(os.environ.get("MONGODB_URI"))
db = client[os.environ.get("MONGODB_DB")]

@app.route('/')
def home():
    return "Flask server is running!"

@app.route('/run_model', methods=['POST'])
def run_model():
    """
    API endpoint to run the cash flow model.
    """
    try:
        result = run_cashflow_model()
        return jsonify({"status": "success", "message": result}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/contracts', methods=['GET'])
def get_contracts():
    """
    API endpoint to retrieve all contracts from the database.
    """
    contracts = list(db.contracts.find({}, {'_id': 0}))
    return jsonify(contracts)

@app.route('/contracts', methods=['POST'])
def add_contract():
    """
    API endpoint to add a new contract to the database.
    """
    contract = request.get_json()
    db.contracts.insert_one(contract)
    return jsonify({"status": "success", "message": "Contract added successfully."}), 201

if __name__ == '__main__':
    app.run(debug=True)