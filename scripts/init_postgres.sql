CREATE SCHEMA IF NOT EXISTS standardization_revenue_research_meli;
CREATE SCHEMA IF NOT EXISTS stg_revenue_research_meli;
CREATE SCHEMA IF NOT EXISTS mdm_revenue_research_meli;

CREATE TABLE IF NOT EXISTS standardization_revenue_research_meli.brands(
  brand_str VARCHAR(200) NULL,
  branded_int INTEGER NULL
);

CREATE TABLE IF NOT EXISTS standardization_revenue_research_meli.characters(
  number_id_int INTEGER NULL,
  group_company_owner_rights_str VARCHAR(500) NULL,
  twdc_int INTEGER NULL,
  franchise_n1_str VARCHAR(200) NULL,
  franchise_n2_str VARCHAR(200) NULL,
  franchise_n3_str VARCHAR(200) NULL,
  content_str VARCHAR(300) NULL,
  character_str VARCHAR(200) NULL,
  character_standardization_str VARCHAR(200) NULL
);

CREATE TABLE IF NOT EXISTS standardization_revenue_research_meli.mercado_libre_subcat(
  cat_code_str VARCHAR(50) NULL,
  category_api_str VARCHAR(50) NULL,
  cod_subcat_str VARCHAR(50) NULL,
  subcategory_str VARCHAR(200) NULL,
  items_int INTEGER NULL,
  country_str VARCHAR(100) NULL,
  homologated_category_str VARCHAR(200) NULL,
  id_selected_int INTEGER NULL,
  homologated_subcategory_str VARCHAR(200) NULL
);

CREATE INDEX IF NOT EXISTS idx_subcat_selected
ON standardization_revenue_research_meli.mercado_libre_subcat (id_selected_int, cod_subcat_str);

CREATE INDEX IF NOT EXISTS idx_brand_lower
ON standardization_revenue_research_meli.brands (LOWER(brand_str));

CREATE INDEX IF NOT EXISTS idx_character_lower
ON standardization_revenue_research_meli.characters (LOWER(character_str));
