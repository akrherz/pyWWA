---
--- Text Products
---
CREATE TABLE text_products (
    reads smallint DEFAULT 0,
    product text,
    product_id character varying(32)
);
select addgeometrycolumn('','text_products','geom',4326,'MULTIPOLYGON',2);

grant select on text_products to apache;

create index text_products_idx 
  on text_products(product_id);

---
--- riverpro
---
CREATE TABLE riverpro (
    nwsli character varying(5),
    stage_text text,
    flood_text text,
    forecast_text text,
    severity character(1)
);

grant select on riverpro to apache;

CREATE UNIQUE INDEX riverpro_nwsli_idx ON riverpro USING btree (nwsli);

CREATE RULE replace_riverpro AS ON INSERT TO riverpro WHERE (EXISTS (SELECT 1 FROM riverpro WHERE ((riverpro.nwsli)::text = (new.nwsli)::text))) DO INSTEAD UPDATE riverpro SET stage_text = new.stage_text, flood_text = new.flood_text, forecast_text = new.forecast_text, severity = new.severity WHERE ((riverpro.nwsli)::text = (new.nwsli)::text);



---
--- VTEC Table
---
CREATE TABLE warnings_2008 (
    id serial,
    issue timestamp with time zone,
    expire timestamp with time zone,
    updated timestamp with time zone,
    type character(3),
    gtype character(1),
    wfo character(3),
    eventid smallint,
    status character(3),
    fips integer,
    fcster character varying(24),
    report text,
    svs text,
    ugc character varying(6),
    phenomena character(2),
    significance character(1),
    hvtec_nwsli character(5)
) WITH OIDS;
select addgeometrycolumn('','warnings_2008','geom',4326,'MULTIPOLYGON',2);

grant select on warnings_2008 to apache;

create index warnings_2008_idx 
   on warnings_2008(wfo,eventid,significance,phenomena);




---
--- Storm Based Warnings Geo Tables
---
create table sbw_2008(
  wfo char(3),
  eventid smallint,
  significance char(1),
  phenomena char(2),
  status char(3),
  issue timestamp with time zone,
  init_expire timestamp with time zone,
  expire timestamp with time zone,
  polygon_begin timestamp with time zone,
  polygon_end timestamp with time zone,
  report text
) WITH OIDS;
select addgeometrycolumn('','sbw_2008','geom',4326,'MULTIPOLYGON',2);

grant select on sbw_2008 to apache;

create index sbw_2008_idx on sbw_2008(wfo,eventid,significance,phenomena);


---
--- LSRs!
--- 
CREATE TABLE lsrs (
    valid timestamp with time zone,
    type character(1),
    magnitude real,
    city character varying(32),
    county character varying(32),
    state character(2),
    source character varying(32),
    remark text,
    wfo character(3),
    typetext character varying(40)
) WITH OIDS;
select addgeometrycolumn('','lsrs','geom',4326,'POINT',2);

grant select on lsrs to apache;

CREATE INDEX lsrs_bogus_idx ON lsrs USING btree (oid);
CREATE INDEX lsrs_valid_idx ON lsrs USING btree (valid);
CREATE INDEX lsrs_wfo_idx ON lsrs USING btree (wfo);

---
--- watches
---
CREATE TABLE watches (
    sel character(5),
    issued timestamp with time zone,
    expired timestamp with time zone,
    type character(3),
    report text,
    num smallint
);
select addgeometrycolumn('','watches','geom',4326,'MULTIPOLYGON',2);

grant select on watches to apache;

CREATE UNIQUE INDEX watches_idx ON watches USING btree (issued, num);

