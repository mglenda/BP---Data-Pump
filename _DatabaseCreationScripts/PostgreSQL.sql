CREATE TABLE IF NOT EXISTS public.country_region
(
    region_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
    country_iso character(3) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT uq_country_region UNIQUE (region_name, country_iso)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.country_region
    OWNER to postgres;

CREATE TABLE IF NOT EXISTS public.fact_country_year_indicators
(
    country_iso character(3) COLLATE pg_catalog."default" NOT NULL,
    year integer NOT NULL,
    gdp_per_country numeric,
    gdp_growth numeric,
    population integer,
    urbanisation numeric,
    co_per_capita numeric,
    healthcare_spendings_gdp_ratio numeric,
    unemployment numeric,
    employment_industry numeric,
    energy_consumption_per_capita numeric,
    inflation numeric,
    gdp_per_capita numeric,
    life_expectancy numeric,
    fdi_net_inflows_gdp_ratio numeric,
    renewable_electricity_output_ratio numeric,
    manufacturing_value_added_gdp_ratio numeric,
    labor_force_participation numeric,
    CONSTRAINT fact_country_year_indicators_pkey PRIMARY KEY (country_iso, year)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fact_country_year_indicators
    OWNER to postgres;

CREATE TABLE IF NOT EXISTS public.fact_indicator_correlations
(
    scope_type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    scope_value character varying(50) COLLATE pg_catalog."default" NOT NULL,
    method character varying(20) COLLATE pg_catalog."default" NOT NULL,
    variable_x character varying(100) COLLATE pg_catalog."default" NOT NULL,
    variable_y character varying(100) COLLATE pg_catalog."default" NOT NULL,
    correlation_value numeric NOT NULL,
    abs_correlation_value numeric,
    strength_label character varying(20) COLLATE pg_catalog."default",
    direction character varying(10) COLLATE pg_catalog."default",
    p_value numeric,
    observation_count integer NOT NULL,
    calculated_at timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT fact_indicator_correlations_pkey PRIMARY KEY (scope_type, scope_value, method, variable_x, variable_y),
    CONSTRAINT chk_corr_abs_value_range CHECK (abs_correlation_value >= 0::numeric AND abs_correlation_value <= 1::numeric),
    CONSTRAINT chk_corr_direction CHECK (direction::text = ANY (ARRAY['positive'::character varying::text, 'negative'::character varying::text, 'neutral'::character varying::text])),
    CONSTRAINT chk_corr_method CHECK (method::text = ANY (ARRAY['pearson'::character varying::text, 'spearman'::character varying::text])),
    CONSTRAINT chk_corr_observation_count CHECK (observation_count >= 0),
    CONSTRAINT chk_corr_strength_label CHECK (strength_label::text = ANY (ARRAY['very_weak'::character varying::text, 'weak'::character varying::text, 'moderate'::character varying::text, 'strong'::character varying::text, 'very_strong'::character varying::text])),
    CONSTRAINT chk_corr_value_range CHECK (correlation_value >= '-1'::integer::numeric AND correlation_value <= 1::numeric)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fact_indicator_correlations
    OWNER to postgres;

CREATE OR REPLACE FUNCTION public.trg_set_indicator_correlation_derived_fields()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- absolútna hodnota korelácie
    NEW.abs_correlation_value := abs(NEW.correlation_value);
	
    -- smer vzťahu
    IF NEW.correlation_value > 0 THEN
        NEW.direction := 'positive';
    ELSIF NEW.correlation_value < 0 THEN
        NEW.direction := 'negative';
    ELSE
        NEW.direction := 'neutral';
    END IF;

    -- sila vzťahu podľa absolútnej hodnoty
    IF NEW.abs_correlation_value < 0.20 THEN
        NEW.strength_label := 'very_weak';
    ELSIF NEW.abs_correlation_value < 0.40 THEN
        NEW.strength_label := 'weak';
    ELSIF NEW.abs_correlation_value < 0.60 THEN
        NEW.strength_label := 'moderate';
    ELSIF NEW.abs_correlation_value < 0.80 THEN
        NEW.strength_label := 'strong';
    ELSE
        NEW.strength_label := 'very_strong';
    END IF;

    -- timestamp pri update
    NEW.calculated_at := now();

    RETURN NEW;
END;
$BODY$;

ALTER FUNCTION public.trg_set_indicator_correlation_derived_fields()
    OWNER TO postgres;

CREATE OR REPLACE TRIGGER trg_set_indicator_correlation_derived_fields
    BEFORE INSERT OR UPDATE 
    ON public.fact_indicator_correlations
    FOR EACH ROW
    EXECUTE FUNCTION public.trg_set_indicator_correlation_derived_fields();