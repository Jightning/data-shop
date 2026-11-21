Read the attached rules.md word-for-word. Build a standalone Rust project (edition 2024) + multi-stage linux/amd64 Dockerfile that implements the entire specification with 100% accuracy. Prioritize clarity and exact compliance with every edge case in rules.md (especially non-RFC \r handling, strict F1/F2/F3 timestamps, dedup-before-validation, 400-hop Union-Find naming, and exact boundary inequalities). Print a breakdown of milliseconds per pipeline stage to stderr and print nothing to stdout. Respect DATA_DIR, OUTPUT_DIR, SCRATCH_DIR, NCPU, and MEM_GB.



All 36 fixtures and the 1M dev set pass. Here are my real timings on 20M events (8 vCPUs) and peak RAM on 60M events:
[paste stderr stage timings + peak RSS here]
Look at the single slowest stage above. Propose ONE concrete architectural optimization to cut that stage by at least 2× without exceeding 18 GB RAM on 60M events and without violating any edge case in rules.md.



Audit our codebase and Dockerfile against all container limits and rules.md edge cases: check for any writes to / or /tmp, any stdout output, any u32 overflow risks on 60M events or timestamps, any hardcoded CPU flags (target-cpu=native), and any assumptions that could fail on a brand-new random seed (including Feb 29, 2028, empty/header-only CSVs, or shared-device whale clusters).   