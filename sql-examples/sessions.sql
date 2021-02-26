/*
Prepare SQL which converts raw hits (user events) into sessions.
Count the number of sessions in the result table.

In:
  Session definition: https://support.google.com/analytics/answer/2731565?hl=en
  Public table with samle hits data: analytics-230012.Assignments.hits_for_sessions
*/

WITH 
hits AS (
  SELECT 
    anonymousId, 
    hitTime,
    EXTRACT(HOUR FROM hitTime) AS hitHour,
    ARRAY_TO_STRING([utm_campaign, utm_source, utm_medium], '', '?') AS uniqueSource
  FROM `analytics-230012.Assignments.hits_for_sessions`),
previous AS (
  SELECT 
    *, 
    COALESCE(LAG(hitTime, 1) OVER (PARTITION BY anonymousId ORDER BY hitTime), hitTime) AS previousHitTime,
    LAG(hitHour, 1) OVER (PARTITION BY anonymousId ORDER BY hitTime) AS previousHitHour,
    COALESCE(LAG(uniqueSource, 1) OVER (PARTITION BY anonymousId ORDER BY hitTime), uniqueSource) AS previousUniqueSource
  FROM hits),
sessions AS (
  SELECT 
    *,
    CASE 
      WHEN (TIMESTAMP_DIFF(hitTime, previousHitTime, MINUTE) > 30) 
        OR (previousHitHour = 23 AND hitHour = 0) 
        OR (uniqueSource <> previousUniqueSource)
        OR (previousHitHour IS NULL) -- first hit
      THEN 1 ELSE 0 END AS newSession
  FROM previous)
SELECT SUM(newSession) AS sessionsTotal FROM sessions
