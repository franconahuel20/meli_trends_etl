
  
    

  create  table "snowflake_dev"."mdm_revenue_research_meli"."trends__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
    SELECT *
    FROM stg_revenue_research_meli.meli_trends_l3
),

final AS (
    SELECT
        CAST(keyword_str AS VARCHAR(1000)) AS keyword_str,
        CAST(url_str AS VARCHAR(3000)) AS url_str,
        CAST(date_dte AS DATE) AS date_dte,
        CAST(country_str AS VARCHAR(200)) AS country_str,
        CAST(category_str AS VARCHAR(200)) AS category_str,
        CAST(subcategory_str AS VARCHAR(200)) AS subcategory_str,
        CURRENT_TIMESTAMP AS loading_dtm,
        ROW_NUMBER() OVER () AS file_row_id_int,
        CAST(NULL AS VARCHAR(200)) AS brand_str,
        CAST(NULL AS VARCHAR(200)) AS character_str,
        CAST(NULL AS VARCHAR(200)) AS second_brand_str,
        CAST(NULL AS VARCHAR(500)) AS group_company_owner_rights_str,
        CAST(NULL AS INTEGER) AS twdc_int,
        CAST(NULL AS VARCHAR(200)) AS franchise_n1_str,
        CAST(NULL AS VARCHAR(200)) AS franchise_n2_str,
        CAST(NULL AS VARCHAR(200)) AS franchise_n3_str,
        CAST(NULL AS VARCHAR(300)) AS content_str,
        CAST(NULL AS VARCHAR(1000)) AS keyword_in_spanish_str,
        CAST(NULL AS DATE) AS week_dte,
        CAST(NULL AS VARCHAR(1000)) AS normalized_keyword_es_str,
        CAST(NULL AS VARCHAR(1000)) AS keyword_es_without_stopwords_str,
        CAST(NULL AS VARCHAR(200)) AS character_without_stopwords_str,
        CAST(NULL AS VARCHAR(1000)) AS lemmatized_keyword_str,
        CAST(NULL AS VARCHAR(1000)) AS nouns_keyword_str,
        CAST(NULL AS VARCHAR(5)) AS has_franchise_n2_str,
        CAST(NULL AS VARCHAR(5)) AS has_franchise_n3_str,
        CAST(NULL AS VARCHAR(5)) AS has_character_str,
        CAST(NULL AS VARCHAR(5)) AS has_franchise_n1_str,
        CAST(NULL AS VARCHAR(5)) AS has_brand_str,
        CAST(NULL AS VARCHAR(20)) AS branded_str,

       EXTRACT(YEAR FROM CAST(date_dte AS DATE))::INTEGER AS year_calendar_int,
       EXTRACT(MONTH FROM CAST(date_dte AS DATE))::INTEGER AS month_calendar_int,
       EXTRACT(DAY FROM CAST(date_dte AS DATE))::INTEGER AS day_calendar_int

    FROM base
)

SELECT * FROM final
  );
  