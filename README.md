# Kundali API Backend

A FastAPI-based Vedic astrology birth chart generator using Swiss Ephemeris.

## Features

- Generate Vedic birth charts (Kundalis)
- Chart matching (Ashtakoota compatibility)
- Chart storage and management
- RESTful API with OpenAPI documentation
- Health monitoring

## Deployment on Render.com

### Prerequisites

1. Push this code to a GitHub repository
2. Create a Render.com account
3. Connect your GitHub account to Render

### Deployment Steps

1. **Create Web Service**
   - Go to Render Dashboard → New → Web Service
   - Connect your GitHub repository
   - Select the `backend` folder as root directory
   - Render will auto-detect Python environment

2. **Configure Environment Variables**
   - Set `PORT` to `10000`
   - Set `CORS_ORIGINS` to your frontend URL(s)
   - Set `EPHE_PATH` to `./ephe`
   - Set `DB_PATH` to `./kundali.db`

3. **Health Check**
   - Health check path: `/health`
   - Auto-deploy from main branch

### Manual Deployment with render.yaml

1. Copy `render.yaml` to your repository root
2. Update `CORS_ORIGINS` with your frontend domain
3. Push changes to GitHub
4. Create new Web Service from Render dashboard
5. Render will automatically use the configuration

## Environment Variables

```bash
PORT=10000
EPHE_PATH=./ephe
DB_PATH=./kundali.db
CORS_ORIGINS=https://your-frontend-domain.onrender.com
```

## API Endpoints

- `GET /` - API information
- `GET /health` - Health check
- `POST /api/kundali` - Generate birth chart
- `GET /api/charts` - List saved charts
- `POST /api/charts` - Save chart
- `PUT /api/charts/{id}` - Update chart
- `DELETE /api/charts/{id}` - Delete chart
- `POST /api/match` - Chart compatibility matching

## Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run development server
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Access API docs at http://localhost:8000/docs
```

## Database

The application uses SQLite for simplicity. The database file (`kundali.db`) is created automatically on first run.

For production, consider migrating to PostgreSQL for better scalability and persistence.

## Dependencies

- FastAPI - Web framework
- Uvicorn - ASGI server
- PySwissEph - Swiss Ephemeris bindings
- Pydantic - Data validation
- TimezoneFinder - Timezone lookup
- SQLite - Database (built-in)

## License

[Your License Here]