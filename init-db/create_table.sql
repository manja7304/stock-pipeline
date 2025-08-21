CREATE TABLE IF NOT EXISTS public.stocks (
  id SERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume BIGINT,
  raw JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
