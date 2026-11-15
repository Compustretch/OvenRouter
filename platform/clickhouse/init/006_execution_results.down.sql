-- 006_execution_results.down.sql
--
-- Remove the canonical execution projection introduced by 006. Raw route
-- records and all projections from migrations 001-005 remain untouched.

DROP TABLE IF EXISTS llm_observability.route_execution_results_mv;
DROP TABLE IF EXISTS llm_observability.route_execution_results;
