# InvestAI - Gemini CLI Instructions

This file contains project-specific instructions, context, and standards for Gemini CLI.

## Project Overview
InvestAI is a Python-based project.

## Engineering Standards
- Follow PEP 8 for Python code styling.
- Ensure all new features include unit tests.
- Maintain clear documentation for complex algorithms.
- **Testing Mandate**: ALWAYS verify that the intended LLM provider (Real vs. Fake) is correctly active before finalizing a test. Confirm this via explicit log verification or email subject line inspection.
    - **Results Table**: ALWAYS present a detailed "Ground Truth vs AI" verification table at the conclusion of every test to confirm system accuracy.
- **Environment Management**: NEVER set or rely on OS-level environment variables for project configuration. ALL environment variables must be managed exclusively within the local `.env` file unless explicitly requested otherwise by the user.
    - **Overlap Check**: Before running the stack, always verify if any variables defined in `.env` (e.g., `LLM_PROVIDER`) also exist in the local OS. 
    - **Remediation**: If an overlap is found, use `unset VARIABLE_NAME` in the current terminal session to ensure the `.env` value is respected. Do NOT delete variables from the permanent shell profile (e.g., `.zshrc` or `.bash_profile`).
- **Model Persistence Mandate**: NEVER update, change, or "upgrade" the LLM model identifier (e.g., `gemini-flash-latest`) unless explicitly instructed by the user.
- **Framework Mandate**: LangChain is the ONLY permitted framework for LLM interactions. Direct SDK calls are prohibited.

## Git Workflow & Promotion Standards
- **Branching Strategy**: 
    - `master`: Stable, production-ready code.
    - `testing`: Development and integration testing.
- **Strict Promotion Policy**:
    - **NEVER** force push to the `master` branch.
    - All changes must first be committed and verified on the `testing` branch.
    - Promotion to `master` occurs ONLY via a formal Pull Request (PR) from `testing`.
    - **MANUAL MERGE ONLY**: I will only push to the `testing` branch. You (the user) will manually review and merge into `master`.

## Operational Context
- Virtual environment is located in `.venv/`.
- Entry point is `main.py`.
