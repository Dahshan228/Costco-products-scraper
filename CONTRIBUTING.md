# Contributing

## Development setup
1. Create and activate a virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```
3. Run checks:
   ```bash
   ruff check .
   pytest
   ```

## Pull requests
- Keep changes focused and small.
- Include test updates for behavior changes.
- Ensure CI passes on all supported platforms.

## Release flow
- Update `VERSION` and `CHANGELOG.md`.
- Create a tag like `v1.0.1`.
- Push the tag to trigger the release workflow.
