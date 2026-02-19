-- IMPROVADO SENIOR MARKETING ANALYST — TECHNICAL ASSESSMENT
-- SQL Transformation Script for BigQuery

-- This script:
--   1. Builds a unified_ads table
--   2. Creates summary views for dashboard consumption

-- 1. Create the dataset and raw tables in BigQuery

-- CREATE SCHEMA IF NOT EXISTS `project-id.marketing_data`;

-- Upload the 3 CSV files via BigQuery Console:
--   Table names: facebook_ads, google_ads, tiktok_ads
--   Verify column types once tables are created


-- 2. DATA QUALITY CHECKS

-- Check for duplicates in each source
SELECT 'facebook' AS source, date, campaign_id, ad_set_id, COUNT(*) AS cnt
FROM `marketing_data.facebook_ads`
GROUP BY 1, 2, 3, 4
HAVING cnt > 1

UNION ALL

SELECT 'google', date, campaign_id, ad_group_id, COUNT(*)
FROM `marketing_data.google_ads`
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1

UNION ALL

SELECT 'tiktok', date, campaign_id, adgroup_id, COUNT(*)
FROM `marketing_data.tiktok_ads`
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;


-- Check date ranges across platforms
SELECT
  'facebook' AS source, MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS row_count
FROM `marketing_data.facebook_ads`
UNION ALL
SELECT 'google', MIN(date), MAX(date), COUNT(*)
FROM `marketing_data.google_ads`
UNION ALL
SELECT 'tiktok', MIN(date), MAX(date), COUNT(*)
FROM `marketing_data.tiktok_ads`;


-- 3. CREATE UNIFIED ADS TABLE

CREATE OR REPLACE TABLE `marketing_data.unified_ads` AS

WITH facebook AS (
  SELECT
    date,
    'Facebook'                            AS platform,
    campaign_id,
    campaign_name,
    ad_set_id                             AS ad_group_id,
    ad_set_name                           AS ad_group_name,

    -- Core metrics 
    impressions,
    clicks,
    spend,
    conversions,

    -- Derived core metrics
    SAFE_DIVIDE(clicks, impressions)      AS ctr,
    SAFE_DIVIDE(spend, clicks)            AS cpc,
    SAFE_DIVIDE(spend, conversions)       AS cpa,

    -- Platform-specific metrics
    video_views,
    reach,
    frequency,
    engagement_rate,

    -- Google-specific (NULL for Facebook)
    CAST(NULL AS FLOAT64)                 AS conversion_value,
    CAST(NULL AS INT64)                   AS quality_score,
    CAST(NULL AS FLOAT64)                 AS search_impression_share,

    -- TikTok-specific (NULL for Facebook)
    CAST(NULL AS INT64)                   AS likes,
    CAST(NULL AS INT64)                   AS shares,
    CAST(NULL AS INT64)                   AS comments

  FROM `marketing_data.facebook_ads`
),

google AS (
  SELECT
    date,
    'Google'                              AS platform,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,

    impressions,
    clicks,
    cost                                  AS spend,
    conversions,

    SAFE_DIVIDE(clicks, impressions)      AS ctr,
    SAFE_DIVIDE(cost, clicks)             AS cpc,
    SAFE_DIVIDE(cost, conversions)        AS cpa,

    -- Platform-specific
    CAST(NULL AS INT64)                   AS video_views,
    CAST(NULL AS INT64)                   AS reach,
    CAST(NULL AS FLOAT64)                 AS frequency,
    CAST(NULL AS FLOAT64)                 AS engagement_rate,

    conversion_value,
    quality_score,
    search_impression_share,

    CAST(NULL AS INT64)                   AS likes,
    CAST(NULL AS INT64)                   AS shares,
    CAST(NULL AS INT64)                   AS comments

  FROM `marketing_data.google_ads`
),

tiktok AS (
  SELECT
    date,
    'TikTok'                              AS platform,
    campaign_id,
    campaign_name,
    adgroup_id                            AS ad_group_id,
    adgroup_name                          AS ad_group_name,

    impressions,
    clicks,
    cost                                  AS spend,
    conversions,

    SAFE_DIVIDE(clicks, impressions)      AS ctr,
    SAFE_DIVIDE(cost, clicks)             AS cpc,
    SAFE_DIVIDE(cost, conversions)        AS cpa,

    video_views,
    CAST(NULL AS INT64)                   AS reach,
    CAST(NULL AS FLOAT64)                 AS frequency,
    CAST(NULL AS FLOAT64)                 AS engagement_rate,

    CAST(NULL AS FLOAT64)                 AS conversion_value,
    CAST(NULL AS INT64)                   AS quality_score,
    CAST(NULL AS FLOAT64)                 AS search_impression_share,

    likes,
    shares,
    comments

  FROM `marketing_data.tiktok_ads`
),

unioned AS (
  SELECT * FROM facebook
  UNION ALL
  SELECT * FROM google
  UNION ALL
  SELECT * FROM tiktok
)

SELECT
  *,

  CASE
    WHEN LOWER(campaign_name) LIKE '%brand%'
      OR LOWER(campaign_name) LIKE '%awareness%'
      OR LOWER(campaign_name) LIKE '%video_view%'
      OR LOWER(campaign_name) LIKE '%influencer%'
      OR LOWER(campaign_name) LIKE '%display%'
      THEN 'Awareness'
    WHEN LOWER(campaign_name) LIKE '%traffic%'
      OR LOWER(campaign_name) LIKE '%generic%'
      OR LOWER(campaign_name) LIKE '%shopping%'
      OR LOWER(campaign_name) LIKE '%trending%'
      THEN 'Consideration'
    WHEN LOWER(campaign_name) LIKE '%conversion%'
      OR LOWER(campaign_name) LIKE '%retarget%'
      THEN 'Conversion'
    ELSE 'Other'
  END AS funnel_stage

FROM unioned;