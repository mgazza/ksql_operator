package ksqlparser

import (
	"fmt"
	"strings"
	"testing"
)

const ksql = `
CREATE OR REPLACE STREAM PAGE_EVENT_2_ST (
  ns string,
  type string,
  code_version string,
  journey_code string,
  session_id string,
  timestamp bigint,
  page string,
  url string,
  action string,
  scenario_id string,
  display string,
  promo_code string,
  form_mappings string,
  cookie_id string,
  basket ARRAY<STRUCT<ve_id string, client_code string, currency string, price double>>,
  order STRUCT<total_basket double, currency string>,
  header STRUCT<user_agent string, referrer string, ip string>,
  location string,
  device string)
WITH (kafka_topic='PageEvent2', value_format='JSON', PARTITIONS=1, REPLICAS=1);


CREATE OR REPLACE TABLE SESSION_ACTIONS_V2_TB
    AS SELECT
      AS_VALUE(session_id) as session_id,
      AS_VALUE(journey_code) as journey_code,
      journey_code as journey_code_key,
      session_id as session_id_key,
      collect_list(action + '|' + CAST(timestamp AS STRING)) actions,
      collect_list(form_mappings) form_mappings_list,
      latest_by_offset(cookie_id) cookie_id,
      AS_MAP(collect_list(CAST(timestamp AS STRING)), collect_list(scenario_id)) scenarios,
      AS_MAP(collect_list(CAST(timestamp AS STRING)), collect_list(promo_code)) promo_codes,
      AS_MAP(collect_list(CAST(timestamp AS STRING)), collect_list(CASE WHEN order IS NOT null THEN order->total_basket ELSE null END)) orders_value,
      AS_MAP(collect_list(CAST(timestamp AS STRING)), collect_list(CASE WHEN order IS NOT null THEN order->currency ELSE null END)) orders_currency,
      AS_MAP(collect_list(action), collect_list(timestamp)) attribution_actions,
      location as location_key,
      device as device_key,
      AS_VALUE(location) as location,
      AS_VALUE(device) as device,
      EARLIEST_BY_OFFSET(CASE WHEN action LIKE 'engaged_digital_assistant' THEN timestamp END) as earliest_da
    FROM page_event_2_st WINDOW SESSION (20 MINUTES, RETENTION 30 MINUTES, GRACE PERIOD 0 SECONDS)
    WHERE action != 'null'
      AND journey_code != 'null'
      AND session_id != 'null'
    GROUP BY session_id, journey_code, location, device EMIT CHANGES;


CREATE OR REPLACE STREAM SESSION_ACTIONS_ST (
    journey_code string,
    session_id string,
    actions ARRAY<STRING>,
    scenarios MAP<STRING, STRING>,
    promo_codes MAP<STRING, STRING>,
    orders_value MAP<STRING, DOUBLE>,
    orders_currency MAP<STRING, STRING>,
    attribution_actions MAP<STRING, STRING>,
    form_mappings string,
    cookie_id string,
    location string,
    device string,
    earliest_da string
    )
WITH (KAFKA_TOPIC='SESSION_ACTIONS_V2_TB', VALUE_FORMAT='JSON', PARTITIONS=1, REPLICAS=1);


CREATE OR REPLACE STREAM SESSION_ACTIONS_EXPLODED_ST
  AS SELECT
      journey_code,
      session_id,
      actions,
      EXPLODE(actions) action,
      as_map(actions, slice(actions, 2, array_length(actions))) AS actions_map,
      attribution_actions
  FROM SESSION_ACTIONS_ST
  EMIT CHANGES;


CREATE OR REPLACE TABLE HC_1MINUTE_TB
  AS SELECT journey_code, COUNT(session_id) actions_count,
    /*COLLECT_LIST(SPLIT(action, '|')[1]) actions,
    COUNT(CASE WHEN SPLIT(action, '|')[1] = 'add_to_basket' THEN 1 ELSE null END) add_to_basket_count,
    */
    ROUND(CAST( COUNT(CASE WHEN SPLIT(action, '|')[1] = 'add_to_basket' THEN 1 ELSE null END) as DOUBLE) / CAST(COUNT(session_id) AS DOUBLE), 2) as add_to_basket_ratio,
    ROUND(CAST( COUNT(CASE WHEN SPLIT(action, '|')[1] = 'view_category_page' THEN 1 ELSE null END) as DOUBLE) / CAST(COUNT(session_id) AS DOUBLE), 2) as category_page_ratio,
    ROUND(CAST( COUNT(CASE WHEN SPLIT(action, '|')[1] = 'view_product_page' THEN 1 ELSE null END) as DOUBLE) / CAST(COUNT(session_id) AS DOUBLE), 2) as product_page_ratio,
    ROUND(CAST( COUNT(CASE WHEN SPLIT(action, '|')[1] = 'view_home_page' THEN 1 ELSE null END) as DOUBLE) / CAST(COUNT(session_id) AS DOUBLE), 2) as home_page_ratio,
    ROUND(CAST( COUNT(CASE WHEN SPLIT(action, '|')[1] = 'checkout' THEN 1 ELSE null END) as DOUBLE) / CAST(COUNT(session_id) AS DOUBLE), 2) as checkout_ratio
  FROM SESSION_ACTIONS_EXPLODED_ST
    WINDOW HOPPING (SIZE 1 minute, ADVANCE BY 30 SECONDS, RETENTION 2 MINUTES, GRACE PERIOD 0 SECONDS)
  GROUP BY journey_code;

 CREATE OR REPLACE STREAM ATTRIBUTIONS_ST (
  journey_code string,
  session_id string,
  order_value double,
  order_currency string,
  scenario_id string,
    location string,
    device string
  )
 WITH (KAFKA_TOPIC='DA_ATTRIBUTIONS_TB', VALUE_FORMAT='JSON', PARTITIONS=1, REPLICAS=1);

CREATE OR REPLACE STREAM PROMOCODE_ATTRIBUTIONS_ST (
  journey_code string,
  session_id string,
  order_value double,
  order_currency string,
  scenario_id string,
    location string,
    device string
  )
WITH (KAFKA_TOPIC='PROMOCODE_ATTRIBUTIONS_TB', VALUE_FORMAT='JSON', PARTITIONS=1, REPLICAS=1);

CREATE OR REPLACE STREAM EMAIL_ATTRIBUTIONS_ST (
  journey_code string,
  session_id string,
  order_value double,
  order_currency string,
  scenario_id string,
    location string,
    device string
  )
WITH (KAFKA_TOPIC='EMAIL_ATTRIBUTIONS_TB', VALUE_FORMAT='JSON', PARTITIONS=1, REPLICAS=1);

CREATE OR REPLACE TABLE DA_ATTRIBUTIONS_TB AS
    SELECT
        session_id as session_id_key,
        journey_code as journey_code_key,
        attribution_actions['engaged_digital_assistant'] as da_engaged_ts_key,
        attribution_actions['purchase'] as checkout_ts_key,
        orders_currency[attribution_actions['purchase']] as currency_key,
        orders_value[attribution_actions['purchase']] as value_key,
        scenarios[attribution_actions['engaged_digital_assistant']] as scenario_key,
        AS_VALUE(orders_currency[attribution_actions['purchase']]) as order_currency,
        AS_VALUE(orders_value[attribution_actions['purchase']]) as order_value,
        AS_VALUE(scenarios[attribution_actions['engaged_digital_assistant']]) as scenario_id,
        AS_VALUE(session_id) as session_id,
        AS_VALUE(journey_code) as journey_code,
        AS_VALUE(location) as location,
        AS_VALUE(device) as device,
        location as location_key,
        device as device_key
    FROM SESSION_ACTIONS_ST WINDOW SESSION (20 MINUTES, RETENTION 30 MINUTES, GRACE PERIOD 0 SECONDS)
    WHERE
        earliest_da < attribution_actions['purchase']
    GROUP BY
        journey_code,
        session_id,
        attribution_actions['engaged_digital_assistant'],
        attribution_actions['purchase'],
        orders_currency[attribution_actions['purchase']],
        orders_value[attribution_actions['purchase']],
        scenarios[attribution_actions['engaged_digital_assistant']],
        location,
        device
    HAVING COUNT(session_id + attribution_actions['engaged_digital_assistant'] + attribution_actions['purchase']) = 1;

CREATE OR REPLACE TABLE PROMOCODE_ATTRIBUTIONS_TB
  AS SELECT
        session_id as session_id_key,
        journey_code as journey_code_key,
        attribution_actions['purchase'] as checkout_ts_key,
        AS_VALUE(session_id) as session_id,
        AS_VALUE(journey_code) as journey_code,
        location,
        device
    FROM SESSION_ACTIONS_ST WINDOW SESSION (20 MINUTES, RETENTION 30 MINUTES, GRACE PERIOD 0 SECONDS)
    WHERE
        promo_codes[attribution_actions['purchase']] IS NOT NULL
    GROUP BY journey_code, session_id, attribution_actions['purchase'], location, device
    HAVING COUNT(session_id + attribution_actions['purchase']) = 1;


INSERT INTO ATTRIBUTIONS_ST SELECT * FROM PROMOCODE_ATTRIBUTIONS_ST;

CREATE OR REPLACE TABLE ATTRIBUTIONS_CONFIG_TB (
    journey_code string PRIMARY KEY,
    percentage double)
    WITH (
    KAFKA_TOPIC = 'attribution_config_tb',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
  );

CREATE OR REPLACE STREAM INVOICES_ST
    AS
    SELECT
        ATTRIBUTIONS_ST.journey_code as journey_code,
        ATTRIBUTIONS_ST.session_id,
        ATTRIBUTIONS_CONFIG_TB.percentage,
        ATTRIBUTIONS_ST.order_value,
        ATTRIBUTIONS_ST.order_currency,
        ATTRIBUTIONS_ST.scenario_id,
        AS_VALUE(ATTRIBUTIONS_ST.order_value * ATTRIBUTIONS_CONFIG_TB.percentage) as invoice_total,
        ATTRIBUTIONS_ST.location,
        ATTRIBUTIONS_ST.device
    FROM ATTRIBUTIONS_ST
        LEFT JOIN ATTRIBUTIONS_CONFIG_TB ON ATTRIBUTIONS_ST.journey_code = ATTRIBUTIONS_CONFIG_TB.journey_code
    EMIT CHANGES;


CREATE OR REPLACE TABLE REPORTING_TB AS
SELECT
    journey_code,
    SUM(invoice_total) as invoice_total,
    SUM(order_value) as sales_generated,
    -- latest_by_offset(order_currency),
    COUNT(*) as conversions,
    location as location_key,
    device as device_key,
    AS_VALUE(location) as location,
    AS_VALUE(device) as device,
    AS_VALUE(SUM(order_value)/COUNT(*)) as average_order_value
FROM INVOICES_ST
WINDOW HOPPING (SIZE 20 MINUTES, ADVANCE BY 10 MINUTES, RETENTION 40 MINUTES, GRACE PERIOD 0 SECONDS)
GROUP BY journey_code, location, device;


CREATE OR REPLACE TABLE sessions_engagements_displays_count AS
SELECT
    journey_code as journey_code_key,
    scenario_id as scenario_id_key,
    location as location_key,
    device as device_key,
    AS_VALUE(journey_code) as journey_code,
    AS_VALUE(scenario_id) as scenario_id,
    AS_VALUE(location) as location,
    AS_VALUE(device) as device,
    COUNT_DISTINCT(session_id) as sessions_count,
    COUNT(CASE WHEN action = 'displayed_digital_assistant' OR action = 'displayed_email' THEN 1 ELSE null END) displays_count,
    COUNT(CASE WHEN action = 'engaged_digital_assistant' OR action = 'engaged_email' then 1 ELSE null END) engagements_count,
    COUNT(CASE WHEN action = 'displayed_digital_assistant' OR action = 'displayed_email' THEN 1 ELSE null END)/COUNT(CASE WHEN action = 'engaged_digital_assistant' OR action = 'engaged_email' then 1 ELSE null END) as engagements_through_rate
FROM page_event_2_st
WINDOW HOPPING (SIZE 20 MINUTES, ADVANCE BY 10 MINUTES, RETENTION 40 MINUTES, GRACE PERIOD 0 SECONDS)
GROUP BY journey_code, scenario_id, location, device;

`

func TestParseWithSamples(t *testing.T) {
	samples := strings.Split(strings.TrimSpace(ksql), ";")
	for i, sql := range samples {
		if sql == "" {
			continue
		}
		t.Run(fmt.Sprintf("sample %d %s...", i, strings.TrimSpace(sql[:30])), func(tt *testing.T) {
			q, err := Parse(sql + ";")
			if err != nil {
				tt.Error(err)
			}
			tt.Log(q.String())
		})
	}
}
