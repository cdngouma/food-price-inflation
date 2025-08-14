from statcan_wds import previewDimensions
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
POSTGRES_USER = os.getenv("POSTGRES_USER", "root")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "root")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "education_roi")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

### === Tables specifications === #

# Foreign exchange codes
FX_CODES = {
    "legacy": {"IEXM0102_AVG": "USD/CAD", "EUROCAM01": "EUR/CAD"},
    "current": {"FXMUSDCAD": "USD/CAD", "FXMEURCAD": "EUR/CAD"}
}

# Labour Force Status
LFS_SPECS = [
    {"Geography": ["Canada"]},
    {"Labour force characteristics": ["Employment rate", "Unemployment rate"]},
    {"Data type": ["Seasonally adjusted"]},
    {"Statistics": ["Estimate"]},
    {"Gender": ["Total - Gender"]},
    {"Age group": "15 years and over"}
]

# Fuel price
FUEL_PRICE_SPECS = [
    {"Geography": [g for g in previewDimensions(pid=18100001, target="values", dimName="Geography") if g != "Canada"]},
    {"Type of fuel": ["Regular unleaded gasoline at self service filling stations", "Diesel fuel at self service filling stations"]}
]

# Trade index
TRADE_SPECS = [
    {"Geography": ["Canada"]},
    {"Trade": ["Import", "Export"]},
    {"Basis": ["Customs"]},
    {"Seasonal adjustment": ["Seasonally adjusted"]},
    {"Index": ["Price index"]},
    {"Weighting": ["Laspeyres fixed weighted"]},
    {"North American Product Classification System (NAPCS)": ["Farm, fishing and intermediate food products"]}
]

# Food CPI
CPI_SPECS = [
    {"Geography": ["Canada"]},
    {"Products and product groups": ["Food"]}
]

