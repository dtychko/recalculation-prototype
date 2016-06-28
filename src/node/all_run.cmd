@echo off
call cd  ./logging
call npm install
call cd  ../orchestrator
call npm install
call cd  ../balancer
call npm install
call cd  ../computation_log
call npm install
call cd  ../scheduled_producer
call npm install
call cd  ../
start "orchestrator" node orchestrator_run.js
start "balancer" node balancer_run.js
start "computation_log" node computation_log_run.js
start "scheduled_producer" node scheduled_producer_run.js ./data/default.json