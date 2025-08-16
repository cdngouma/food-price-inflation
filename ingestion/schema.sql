-- foreign_exchange
DROP TABLE IF EXISTS public.foreign_exchange CASCADE;
CREATE TABLE public.foreign_exchange (
  id        SERIAL PRIMARY KEY,
  "date"    DATE  NOT NULL UNIQUE,
  usd_rate  REAL  NOT NULL,
  eur_rate  REAL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- labour_force_status
DROP TABLE IF EXISTS public.labour_force_status CASCADE;
CREATE TABLE public.labour_force_status (
  id                  SERIAL PRIMARY KEY,
  "geography"         VARCHAR(255) NOT NULL,
  "date"              DATE  NOT NULL,
  employment_rate     REAL  NOT NULL,
  unemployment_rate   REAL  NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE ("geography", "date")
);

-- fuel_price
DROP TABLE IF EXISTS public.fuel_price CASCADE;
CREATE TABLE public.fuel_price (
  id             SERIAL PRIMARY KEY,
  "geography"    VARCHAR(255) NOT NULL,
  "date"         DATE  NOT NULL,
  gasoline_price REAL  NOT NULL,
  diesel_price   REAL  NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE ("geography", "date")
);

-- trade_index
DROP TABLE IF EXISTS public.trade_index CASCADE;
CREATE TABLE public.trade_index (
  id                   SERIAL PRIMARY KEY,
  "geography"          VARCHAR(255) NOT NULL,
  "date"               DATE  NOT NULL,
  export_price_index   REAL  NOT NULL,
  import_price_index   REAL  NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE ("geography", "date")
);

-- food_cpi
DROP TABLE IF EXISTS public.food_cpi CASCADE;
CREATE TABLE public.food_cpi (
  id           SERIAL PRIMARY KEY,
  "geography"  VARCHAR(255) NOT NULL,
  "date"       DATE  NOT NULL,
  food_cpi     REAL  NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE ("geography", "date")
);
