-- 仅调整ID=1的诊断端口，不修改业务监听、数据库地址及denylist。
-- 如果实际进程ID不同，请先修改对应WHERE条件；多实例不能共用相同监听端口。
BEGIN IMMEDIATE;
UPDATE gatewaySerConfig SET deBugModePort = '6060' WHERE gatewaySerId = 1;
UPDATE actorSerConfig SET deBugModePort = '6061' WHERE actorSerId = 1;
UPDATE modularizedSerConfig SET deBugModePort = '6062' WHERE modularizedSerId = 1;
UPDATE interactiveSerConfig SET deBugModePort = '6063' WHERE interactiveSerId = 1;
UPDATE webSerConfig SET deBugModePort = '6064' WHERE webSerId = 1;
COMMIT;

SELECT 'gateway_server' AS process, gatewaySerId AS id, deBugModePort AS debug_port FROM gatewaySerConfig WHERE gatewaySerId = 1
UNION ALL SELECT 'actor_server', actorSerId, deBugModePort FROM actorSerConfig WHERE actorSerId = 1
UNION ALL SELECT 'modularized_server', modularizedSerId, deBugModePort FROM modularizedSerConfig WHERE modularizedSerId = 1
UNION ALL SELECT 'interactive_server', interactiveSerId, deBugModePort FROM interactiveSerConfig WHERE interactiveSerId = 1
UNION ALL SELECT 'web_server', webSerId, deBugModePort FROM webSerConfig WHERE webSerId = 1;
