# Local Backend Setup

Quick guide to run the Python backend locally for development.

## Prerequisites

- Python 3.8+
- MongoDB connection (local or Atlas)

## Setup Steps

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Create `.env` file:**
   ```env
   MONGODB_URI=mongodb://localhost:27017/your_db
   # OR for MongoDB Atlas:
   # MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
   MONGODB_DB=your_database_name
   PORT=10000
   ```

3. **Run the backend:**
   ```bash
   python app.py
   ```

4. **Verify it's running:**
   ```bash
   curl http://localhost:10000/
   ```
   
   Should return:
   ```json
   {
     "status": "healthy",
     "message": "Renewable Finance Backend API",
     ...
   }
   ```

## Testing the API

### Health Check
```bash
curl http://localhost:10000/
```

### Run Model
```bash
curl -X POST http://localhost:10000/api/run-model \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Run Sensitivity Analysis
```bash
curl -X POST http://localhost:10000/api/sensitivity \
  -H "Content-Type: application/json" \
  -d '{"config_file": "config/sensitivity_config.json"}'
```

## Troubleshooting

### Port already in use
Change the port in `.env`:
```env
PORT=10001
```

### Import errors
Make sure you're in the `backend-renew` directory and all dependencies are installed:
```bash
pip install -r requirements.txt
```

### MongoDB connection issues
- Check your `MONGODB_URI` in `.env`
- Ensure MongoDB is running (if local)
- Check network access (if using Atlas)






