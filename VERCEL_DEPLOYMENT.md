# Vercel Deployment Guide for Backend

This guide covers deploying the Flask backend to Vercel.

## Prerequisites

1. Install Vercel CLI:
   ```bash
   npm i -g vercel
   ```

2. Login to Vercel:
   ```bash
   vercel login
   ```

## Deployment Steps

### 1. Configure Environment Variables

Before deploying, set up environment variables in Vercel:

- `MONGODB_URI` - Your MongoDB connection string
- `MONGODB_DB` - Your database name
- `PORT` - (Optional, defaults to 10000)

You can set these via:
- Vercel Dashboard: Project Settings → Environment Variables
- Or via CLI: `vercel env add MONGODB_URI`

### 2. Deploy to Vercel

From the `backend-renew` directory:

```bash
vercel
```

For production deployment:

```bash
vercel --prod
```

### 3. Update Frontend Backend URL

After deployment, update the frontend's `NEXT_PUBLIC_BACKEND_URL` environment variable in Vercel to point to your backend URL:

```
NEXT_PUBLIC_BACKEND_URL=https://your-backend-project.vercel.app
```

## Project Structure

- `api/index.py` - Vercel serverless function entry point
- `vercel.json` - Vercel configuration
- `app.py` - Main Flask application
- `requirements.txt` - Python dependencies

## Important Notes

1. **Function Timeout**: The backend is configured with a 300-second (5 minute) timeout. This requires a Vercel Pro plan or higher.

2. **Memory**: Configured with 1024MB memory for complex calculations.

3. **File System**: Vercel serverless functions have read-only file systems except for `/tmp`. Make sure all data files are accessible or use MongoDB for data storage.

4. **Environment Variables**: Never commit `.env.local` files. They are already in `.gitignore`.

## Troubleshooting

### Import Errors
If you see import errors, check that:
- All dependencies are in `requirements.txt`
- The `PYTHONPATH` is set correctly in `vercel.json`

### Timeout Issues
If requests timeout:
- Check Vercel plan limits (Hobby: 10s, Pro: 60s, Enterprise: 300s)
- Consider optimizing long-running calculations
- Use background jobs for heavy processing

### Database Connection Issues
- Verify `MONGODB_URI` is set correctly in Vercel
- Check MongoDB Atlas network access allows Vercel IPs
- Ensure connection string includes authentication

## Local Testing

Test the Vercel setup locally:

```bash
vercel dev
```

This will start a local server that mimics Vercel's environment.





