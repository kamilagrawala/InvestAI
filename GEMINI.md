# InvestAI - Gemini CLI Instructions

This file contains project-specific instructions, context, and standards for Gemini CLI.

## Project Overview
InvestAI is a Python-based project.

## Engineering Standards
- Follow PEP 8 for Python code styling.
- Ensure all new features include unit tests.
- Maintain clear documentation for complex algorithms.

## Git Workflow & Promotion Standards
- **Branching Strategy**: 
    - `master`: Stable, production-ready code.
    - `testing`: Development and integration testing.
- **Strict Promotion Policy**:
    - **NEVER** force push to the `master` branch.
    - All changes must first be committed and verified on the `testing` branch.
    - Promotion to `master` occurs ONLY via a formal Pull Request (PR) from `testing`.
    - This PR **must be reviewed and approved** before merging to ensure code quality and system stability.

## Operational Context
- Virtual environment is located in `.venv/`.
- Entry point is `main.py`.
