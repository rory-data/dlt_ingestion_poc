# Development Guidelines for AI Agents

> **Note**: This file provides quick-reference development guidelines. For comprehensive AI instructions, see `instructions/copilot-instructions.md`.
> **Important**: This development environment is macOS (Silicon) using zsh. Commands need to cater for that.

## Core Engineering Approach

Follow **Core Engineering Principles** from `instructions/core/engineering-principles.instructions.md`:

- Apply SOLID principles, DRY, YAGNI, KISS pragmatically
- Write clean, readable code that tells a story and minimises cognitive load
- Follow test pyramid: 70% unit, 20% integration, 10% end-to-end
- Use AAA pattern (Arrange, Act, Assert) and Given/When/Then for tests

## Workflow Standards

Follow the **6-Phase Loop** from `instructions/core/workflow-standards.instructions.md`:

1. **Analyse**: Understand requirements, document in EARS notation
2. **Design**: Create technical design, error handling, test strategy
3. **Implement**: Code in small testable increments following language conventions
4. **Validate**: Execute tests, verify edge cases and error handling
5. **Reflect**: Refactor for maintainability, update documentation
6. **Handoff**: Generate summary, prepare PR with changelog

## Language-Specific Guidelines

### Python Development

- Use modern Python syntax with type hints
- Ensure code is compatible with the Python versions specified in the project configuration
- Follow PEP 8 conventions
- Use pytest for testing with descriptive test names: `test_[function]_[scenario]_[expected_outcome]`
- Organise tests to mirror module structure
- Target >80% test coverage with meaningful tests
- Use `memray` for memory profiling in complex scenarios. It is installed as a `uv tool`
- `loguru` is the preferred logging library, preferably using structured logging. `loguru` prefers brace-style formatting.
- See `instructions/language/python.instructions.md` for details

### Go Development

- Use Go 1.21+ with modern features (generics, slices package)
- Write idiomatic Go: simple, explicit, composition over inheritance
- Follow table-driven test patterns with subtests
- Use `gofmt`, `goimports`, and `golangci-lint` for code quality
- Accept interfaces, return structs
- See `instructions/language/golang.instructions.md` for details

### Docker Development

- Use multi-stage builds for optimal image size
- Run containers as non-root users
- Pin base image versions for reproducibility
- Minimise layers and use .dockerignore
- See `instructions/language/docker.instructions.md` for details

### Documentation

- Use NZ English spelling and grammar
- Follow Markdown standards from `instructions/language/markdown.instructions.md`
- Use sentence case for headings (except main title)
- Keep documentation concise and actionable
- Do not create a summarry document of any agent changes unless specifcally instructed

## Testing Instructions

- Run all tests before committing changes
- Add or update tests for any code changes
- Use pytest with proper fixture management and mocking
- Mock external dependencies at appropriate levels
- Ensure mocks include specs where appropriate
- Test happy paths, edge cases, and error conditions
- Validate test coverage and ensure meaningful assertions

## Code Quality Standards

- **Never compromise fundamentals**: Maintain code quality and architectural integrity
- **Balance craft with delivery**: Good over perfect, but never skip core principles
- **Document decisions**: Use decision records for significant architectural choices
- **Manage technical debt**: Document debt, assess impact, plan remediation
- **Review regularly**: Apply SOLID principles, identify improvements

## Common Commands and Patterns

### Project Setup

- Check README.md and CONTRIBUTING.md for project-specific setup instructions
- Use uv-managed virtual environments for Python projects
- Install dependencies before making changes

### Terminal Interactions

- `timeout` is not available in macOS zsh

### Testing

- Python: `pytest` or `pytest -v` for verbose output
- Run specific tests: `pytest -k "test_name_pattern"`
- Check coverage: `pytest --cov=module_name`

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

> **For comprehensive quick reference tables**, see `instructions/quick-reference/instruction-index.md`

| Task                | Primary Instructions                          | Action                      |
| ------------------- | --------------------------------------------- | --------------------------- |
| Feature development | engineering-principles.md + language-specific | Follow 6-phase loop         |
| Code review         | engineering-principles.md                     | Apply SOLID, clean code     |
| Documentation       | markdown.instructions.md                      | Use templates, NZ English   |
| Testing             | engineering-principles.md + language-specific | Test pyramid, AAA pattern   |
| Containerisation    | docker.instructions.md                        | Multi-stage, security focus |

## Emergency Quick Checks

Before finalising any work:

- [ ] All tests pass
- [ ] Code follows language conventions
- [ ] Documentation updated
- [ ] No errors or warnings
- [ ] SOLID principles applied
- [ ] Edge cases and errors handled
- [ ] Technical debt documented if incurred
