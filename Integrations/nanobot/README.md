# NanoBot Integration Boundary

This directory is an external-runtime integration for NanoBot. It is intentionally outside the CommonGround Kernel contract surface.

NanoBot may parse payloads such as `common_ground.work_order.v1`, `execution_plan`, `child_tasks`, or integration-specific bootstrap fields because those are useful runtime conventions for this adapter. Those schemas are not Kernel law.

CommonGround Kernel remains payload-agnostic except for explicitly modeled CommonGround lifecycle, actor, claim, topology, ledger, and reference facts. Other runtimes can ignore the NanoBot work-order schema entirely and define their own payload contracts.
