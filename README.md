# Lead Pipeline Setup

## Quick Start

### Option 1: Using requirements.txt (Recommended)

```bash
# Clone the repository
git clone <your-repo-url>
cd lead-pipeline

# Create virtual environment
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
# source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

### Option 2: Using pyproject.toml (Modern Python)

```bash
# Clone the repository
git clone <your-repo-url>
cd lead-pipeline

# Install with pip (will use pyproject.toml)
pip install -e .
```

## Environment Setup

1. **Configure API Keys:**
   - Copy `.env.example` to `.env`
   - Edit the `.env` file with your actual API keys:
     - `GEMINI_API_KEY`: Get from https://makersuite.google.com/app/apikey
     - `PLACES_API_KEY`: Get from https://console.cloud.google.com/apis/credentials

2. **Run the Pipeline:**
   ```bash
   python lead_pipeline.py
   ```

## Dependencies

This project uses several Python packages:

- **pandas**: Data processing and CSV handling
- **requests**: HTTP requests for Google Places API
- **python-dotenv**: Environment variable management
- **google-genai**: Google's Gemini AI for business classification
- **playwright**: Modern web scraping for IndiaMart
- **selenium**: Browser automation for ClearTax GST lookup
- **webdriver-manager**: Automatic Chrome driver management

## Project Structure

```
lead-pipeline/
├── lead_pipeline.py          # Main pipeline script
├── requirements.txt          # Dependencies (pip install -r requirements.txt)
├── pyproject.toml           # Modern Python project configuration
├── .env                     # Environment variables (API keys)
├── .env.example            # Template for .env file
├── README.md               # This file
├── session.json            # Login session cache (auto-generated)
└── indiamart_leads/        # Output directory
    ├── step1_scraped.csv   # Raw scraped data
    ├── step2_enriched.csv  # Google Places enriched data
    └── step3_final.csv     # Final classified data
```

## GitHub Setup

Before uploading to GitHub:

1. **Create .env.example:**

   ```bash
   cp .env .env.example
   # Edit .env.example to remove actual API keys, keep placeholders
   ```

2. **Add to .gitignore:**

   ```
   .env
   .venv/
   __pycache__/
   *.pyc
   session.json
   indiamart_leads/*.csv
   ```

3. **Commit and push:**
   ```bash
   git add .
   git commit -m "Initial commit: Lead generation pipeline"
   git push origin main
   ```
