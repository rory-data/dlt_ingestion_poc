# Development Guidelines for AI Agents

> **Note**: This file provides quick-reference development guidelines. For comprehensive AI instructions, see `~/.copilot/instructions/copilot-instructions.md`.
> **Important**: This development environment is macOS (Silicon) using zsh. Commands need to cater for that.

## Core Engineering Approach

Follow **Core Engineering Principles** from `~/.copilot/instructions/core/engineering-principles.instructions.md`:

- Apply SOLID principles, DRY, YAGNI, KISS pragmatically
- Write clean, readable code that tells a story and minimises cognitive load
- Follow test pyramid: 70% unit, 20% integration, 10% end-to-end
- Use AAA pattern (Arrange, Act, Assert) and Given/When/Then for tests

## Workflow Standards

Follow the **6-Phase Loop** from `~/.copilot/instructions/core/workflow-standards.instructions.md`:

1. **Analyse**: Understand requirements, document in EARS notation
2. **Design**: Create technical design, error handling, test strategy
3. **Implement**: Code in small testable increments following language conventions
4. **Validate**: Execute tests, verify edge cases and error handling
5. **Reflect**: Refactor for maintainability, update documentation
6. **Handoff**: Generate summary, prepare PR with changelog

## Language-Specific Skills

For language-specific guidance, refer to the relevant skills:

- **Python**: See `~/.copilot/skills/python-conventions/SKILL.md` and `~/.copilot/skills/python-testing-patterns/SKILL.md`
- **Go**: See `~/.copilot/skills/golang-conventions/SKILL.md`
- **Docker**: See `~/.copilot/skills/docker-best-practices/SKILL.md`
- **Markdown**: See `~/.copilot/skills/markdown-conventions/SKILL.md`

## Project-Specific Guidance

### dlt Development

- dlt means "data load tool". It is an open source Python library installable via `uv add dlt`.
- To create a new pipeline, use `dlt init <source> <destination>`.
- The dlt library comes with the `dlt` CLI. Add the `--help` flag to any command to verify its specs.
- The preferred way to configure dlt (sources, resources, destinations, etc.) is to use `.dlt/config.toml` and `.dlt/secrets.toml`. Make sure to fill required fields when adding a source or resource.
- During development, always set `dev_mode=True` when creating a dlt Pipeline. `pipeline = dlt.pipeline(..., dev_mode=True)`. This allows to reset the pipeline's schema and state between iterations.
- Use type annotations only if you're certain you're properly importing the types.
- Use dlt's REST API source if loading data from the web.
- Use dlt's SQL source when loading data from an SQL database or backend.
- Use dlt's filesystem source if loading data from files (CSV, PDF, Parquet, JSON, and more). This works for local filesystems and cloud buckets (AWS, Azure, GCP, Minio, etc.).

## Code Quality Standards

- **Never compromise fundamentals**: Maintain code quality and architectural integrity
- **Balance craft with delivery**: Good over perfect, but never skip core principles
- **Document decisions**: Use decision records for significant architectural choices
- **Manage technical debt**: Document debt, assess impact, plan remediation
- **Review regularly**: Apply SOLID principles, identify improvements

## Common Commands and Patterns

### Project Setup

- Check README.md and CONTRIBUTING.md for project-specific setup instructions
- Use `uv`-managed virtual environments for Python projects
- Use Astral's `ruff` for linting and formatting
- Use Astral's `ty` for typing
- Install dependencies before making changes

### Terminal Interactions

- `timeout` is not available in macOS zsh

### Testing

- Python: `uv run pytest` or `uv run pytest -v` for verbose output
- Run specific tests: `uv run pytest -k "test_name_pattern"`
- Check coverage: `uv run pytest --cov=module_name`

### Code Quality

- Format code according to language conventions
- Run linters before committing
- Fix all errors and warnings before finalising changes

## AI Interaction Guidelines

- **Be explicit**: Ask clarifying questions when requirements are ambiguous
- **Use references**: Point to relevant instruction files for detailed guidance
- **Document thoroughly**: Create requirements.md, design.md, tasks.md for complex work
- **Validate continuously**: Run tests and checks throughout implementation
- **Think forward**: Anticipate future needs and technical evolution

## Quick Reference

> **For comprehensive quick reference tables**, see `~/.copilot/instructions/quick-reference/instruction-index.md`

| Task                | Primary Instructions                    | Action                      |
| ------------------- | --------------------------------------- | --------------------------- |
| Feature development | engineering-principles + language skill | Follow 6-phase loop         |
| Code review         | engineering-principles                  | Apply SOLID, clean code     |
| Documentation       | markdown-conventions skill              | Use templates, NZ English   |
| Testing             | python-testing-patterns skill           | Test pyramid, AAA pattern   |
| Containerisation    | docker-best-practices skill             | Multi-stage, security focus |

## Emergency Quick Checks

Before finalising any work:

- [ ] All tests pass
- [ ] Code follows language conventions
- [ ] Documentation updated
- [ ] No errors or warnings
- [ ] SOLID principles applied
- [ ] Edge cases and errors handled
- [ ] Technical debt documented if incurred
