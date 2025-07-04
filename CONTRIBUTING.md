# Contributing to ETL Lineage Analyzer

Thank you for your interest in contributing to ETL Lineage Analyzer! This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Reporting Bugs](#reporting-bugs)
- [Feature Requests](#feature-requests)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

- Use the [GitHub issue tracker](https://github.com/your-username/lineage-analyzer/issues)
- Check existing issues to avoid duplicates
- Include detailed information:
  - Python version
  - Operating system
  - Steps to reproduce
  - Expected vs actual behavior
  - Sample files (if applicable)

### Suggesting Enhancements

- Use the [GitHub issue tracker](https://github.com/your-username/lineage-analyzer/issues)
- Clearly describe the enhancement
- Explain why this enhancement would be useful
- Include use cases and examples

### Pull Requests

- Fork the repository
- Create a feature branch (`git checkout -b feature/amazing-feature`)
- Make your changes
- Add tests if applicable
- Ensure all tests pass
- Update documentation
- Commit your changes (`git commit -m 'Add amazing feature'`)
- Push to the branch (`git push origin feature/amazing-feature`)
- Open a Pull Request

## Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-username/lineage-analyzer.git
   cd lineage-analyzer
   ```

2. **Set up a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt  # For development dependencies
   ```

4. **Install in development mode**
   ```bash
   pip install -e .
   ```

## Coding Standards

### Python Style Guide

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guidelines
- Use type hints for function parameters and return values
- Keep functions focused and single-purpose
- Add docstrings for all public functions and classes

### Code Formatting

We use `black` for code formatting and `flake8` for linting:

```bash
# Format code
black src/ tests/

# Check linting
flake8 src/ tests/
```

### Type Checking

We use `mypy` for static type checking:

```bash
mypy src/
```

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test file
pytest tests/test_lineage.py

# Run with verbose output
pytest -v
```

### Writing Tests

- Place tests in the `tests/` directory
- Use descriptive test names
- Test both success and failure cases
- Include edge cases and error conditions
- Use fixtures for common test data

### Test Structure

```python
def test_function_name():
    """Test description."""
    # Arrange
    input_data = "..."
    
    # Act
    result = function_to_test(input_data)
    
    # Assert
    assert result == expected_output
```

## Pull Request Process

1. **Update the README.md** with details of changes if applicable
2. **Update the CHANGELOG.md** with a note describing your changes
3. **Ensure the test suite passes**
4. **Update or add tests as appropriate**
5. **Check that your code follows the style guidelines**
6. **Ensure your commit messages are clear and descriptive**

### Commit Message Format

Use conventional commit format:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:
```
feat(parser): add support for PostgreSQL syntax
fix(analyzer): handle empty SQL blocks gracefully
docs(readme): update installation instructions
```

## Areas for Contribution

### High Priority

- **SQL Dialect Support**: Add support for more SQL dialects (PostgreSQL, MySQL, etc.)
- **Performance Improvements**: Optimize parsing for large files
- **Better Error Handling**: Improve error messages and recovery
- **Testing**: Add more comprehensive test coverage

### Medium Priority

- **CLI Improvements**: Add more command-line options
- **Output Formats**: Support for additional output formats (XML, YAML, etc.)
- **Visualization**: Enhanced data flow diagrams
- **Documentation**: More examples and tutorials

### Low Priority

- **IDE Integration**: Plugins for popular IDEs
- **Web Interface**: Simple web-based interface
- **API**: REST API for programmatic access

## Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Documentation**: Check the README.md and inline code comments

## Recognition

Contributors will be recognized in:
- The [CHANGELOG.md](CHANGELOG.md) file
- The project's README.md contributors section
- GitHub's contributors graph

Thank you for contributing to ETL Lineage Analyzer! 🚀 