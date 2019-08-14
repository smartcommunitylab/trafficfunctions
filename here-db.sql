CREATE TABLE public.flowdata_traffic-here_aggregate
(
    "timestamp" timestamp with time zone,
    tmc_id BIGINT,
    speed FLOAT,
    jam_factor FLOAT,
    free_flow_speed FLOAT,    
    "length" FLOAT,
    CID INT,
    LCD BIGINT,
    LON FLOAT,
    LAT FLOAT,
    -- confidence_factor FLOAT
);

CREATE TABLE public.flowdata_traffic-here_traffic
(
    "timestamp" timestamp with time zone,
    tmc_id BIGINT,
    speed FLOAT,
    jam_factor FLOAT,
    free_flow_speed FLOAT,    
    "length" FLOAT,
    CID INT,
    LCD BIGINT,
    LON FLOAT,
    LAT FLOAT,
    -- confidence_factor FLOAT
);
