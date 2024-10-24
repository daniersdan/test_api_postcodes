CREATE TABLE IF NOT EXISTS public.coordinates (
    id SERIAL PRIMARY KEY,
    latitude VARCHAR NOT NULL,
    longitude VARCHAR NOT NULL,
    postal_code TEXT,
    error TEXT,
    created_at timestamp DEFAULT now() NOT NULL
);