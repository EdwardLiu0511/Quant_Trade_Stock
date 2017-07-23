Create Table IF NOT EXISTS trade.trade_FX_Rates(
                         date_id  DATE NOT NULL,
                         fx_us_eur     DOUBLE,
                         fx_us_cny     DOUBLE,
                         fx_us_jpy     DOUBLE,
                         fx_us_gbp     DOUBLE,
                         fx_us_hko     DOUBLE,
                         fx_us_cad     DOUBLE,
                         fx_us_aud     DOUBLE,
                         fx_us_inr     DOUBLE,
                        CONSTRAINT pk_FX_Rates PRIMARY KEY (date_id)
);

